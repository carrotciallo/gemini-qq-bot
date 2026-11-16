//! Outbound OneBot actions.
//!
//! Every action is sent as an `{action, params, echo}` frame and matched by echo.
//! `retcode` must be zero for success; anything else is a hard failure. A send
//! whose response never arrives is *ambiguous* and reported as
//! [`SendOutcome::Unknown`] so the caller can move the outbox row to `unknown`
//! instead of retrying and double-sending.

use serde::Serialize;
use serde_json::{Value, json};

use crate::media::collage::STICKER_MAX_BASE64_BYTES;

/// Parameters for `send_group_msg`.
#[derive(Debug, Clone, Serialize)]
pub struct SendGroupMsg {
    pub group_id: String,
    pub message: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_escape: Option<bool>,
}

impl SendGroupMsg {
    /// Build a plain-text group message. The array message format is used so that
    /// no CQ code parsing happens downstream.
    pub fn text(group_id: &str, text: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            message: json!([{ "type": "text", "data": { "text": text } }]),
            auto_escape: Some(true),
        }
    }

    /// Build a plain-text group message that quotes an earlier message.
    ///
    /// OneBot v11 carries the quote as its own segment, placed first:
    /// `{type: "reply", data: {id: <message_id>}}`. The id is the quoted
    /// message's OneBot message id — never a batch-local ref; the turn
    /// resolves refs to ids before the row is queued.
    pub fn text_quoted(group_id: &str, text: &str, quote_message_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            message: json!([
                { "type": "reply", "data": { "id": quote_message_id } },
                { "type": "text", "data": { "text": text } }
            ]),
            auto_escape: Some(true),
        }
    }

    /// Build a group message that is an animated sticker.
    ///
    /// QQ treats a sticker as an `image` segment with `sub_type: 1`; `0` would make
    /// it an ordinary picture attachment. Verified against the live client: a frame
    /// sent with `sub_type: 1` reads back as `sub_type: 1`. The bytes ride inline as
    /// a base64 data URL so nothing has to be uploaded first and no temporary file
    /// has to survive between the outbox write and the send.
    /// A sticker is an animated-image segment. `sub_type: 1` is what makes QQ
    /// render it as a sticker instead of a plain picture.
    ///
    /// Animated stickers go out byte for byte at any size; stills are
    /// normalized to a QQ-ready JPEG by `fit_for_sticker`. Sends carry no
    /// size cap (user ruling, 2026-11-11).
    pub fn sticker(group_id: &str, bytes: &[u8]) -> Option<Self> {
        Self::sticker_inner(group_id, bytes, None).map(|(msg, _)| msg)
    }

    /// A sticker that quotes an earlier message: the same `reply` segment
    /// convention as [`Self::text_quoted`], ahead of the image segment.
    pub fn sticker_quoted(group_id: &str, bytes: &[u8], quote_message_id: &str) -> Option<Self> {
        Self::sticker_inner(group_id, bytes, Some(quote_message_id)).map(|(msg, _)| msg)
    }

    /// Build a sticker frame and report which payload went on the wire:
    /// the animated original, or a still normalized from the source bytes.
    pub fn sticker_tracked(
        group_id: &str,
        bytes: &[u8],
        quote_message_id: Option<&str>,
    ) -> Option<(Self, StickerPayload)> {
        Self::sticker_inner(group_id, bytes, quote_message_id)
    }

    fn sticker_inner(
        group_id: &str,
        bytes: &[u8],
        quote_message_id: Option<&str>,
    ) -> Option<(Self, StickerPayload)> {
        // An animated sticker must be forwarded verbatim, at any size.
        // `fit_for_sticker` decodes through `load_from_memory().into_rgb8()`
        // and re-encodes JPEG, which keeps exactly one frame — every GIF
        // that went through it arrived on QQ as a still (2026-10-28,
        // user-directed). An earlier cut also downgraded oversized
        // animations to a still instead of dropping them; sends carry no
        // size cap at all now (user ruling, 2026-11-11), so the original
        // bytes always go out.
        let (payload, kind) = if is_animated_image(bytes) {
            (
                crate::media::collage::base64_encode(bytes),
                StickerPayload::Animated,
            )
        } else {
            let jpeg = crate::media::collage::fit_for_sticker(bytes)?;
            (
                crate::media::collage::base64_encode(&jpeg),
                StickerPayload::Static,
            )
        };
        let url = format!("base64://{payload}");
        let mut segments = Vec::with_capacity(2);
        if let Some(id) = quote_message_id {
            segments.push(json!({ "type": "reply", "data": { "id": id } }));
        }
        segments.push(json!({
            "type": "image",
            "data": { "file": url, "sub_type": 1 },
        }));
        Some((
            Self {
                group_id: group_id.to_string(),
                message: Value::Array(segments),
                auto_escape: Some(false),
            },
            kind,
        ))
    }

    /// Which bytes of a sticker payload are actually going on the wire, and
    /// whether they are the animated original. Lets the tests assert "verbatim"
    /// and lets the send path tell the two paths apart.
    #[cfg(test)]
    fn sent_image_bytes(&self) -> Option<String> {
        let segments = self.message.as_array()?;
        for segment in segments {
            if segment.get("type").and_then(Value::as_str) == Some("image") {
                let file = segment
                    .get("data")
                    .and_then(|d| d.get("file"))
                    .and_then(Value::as_str)?;
                return Some(file.trim_start_matches("base64://").to_string());
            }
        }
        None
    }

    /// A plain picture (QQ `sub_type` 0), as opposed to a sticker (`sub_type` 1).
    ///
    /// The bytes go out verbatim, at any size: there is no payload cap on
    /// sends (user ruling, 2026-11-11). This constructor briefly borrowed
    /// the sticker budget, and every generated picture (900 KB–1 MB, so
    /// 1.2 MB+ in base64) silently failed that check — the frame was never
    /// even attempted and the send landed `unknown`. A raw-client control
    /// send of the same file went through in under two seconds, proving
    /// the cap was the whole fault. The only refusal left is empty bytes.
    pub fn image(group_id: &str, bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        let payload = crate::media::collage::base64_encode(bytes);
        Some(Self {
            group_id: group_id.to_string(),
            message: json!([{
                "type": "image",
                "data": { "file": format!("base64://{payload}"), "sub_type": 0 },
            }]),
            auto_escape: Some(false),
        })
    }

    pub fn to_frame(&self, echo: &str) -> Value {
        json!({ "action": "send_group_msg", "params": self, "echo": echo })
    }

    /// The `params` object for a manual `ConnectionHandle::call`.
    pub fn params(&self) -> Value {
        json!(self)
    }
}

/// Whether these bytes are an animation that must be sent without re-encoding.
///
/// Two formats can reach the sticker library as animations: GIF with more than
/// one frame, and animated WebP. Both are detected from the bytes themselves
/// (not from the file extension, which is a fixed `.jpg` whatever the content
/// is), because the send path only has the bytes.
fn is_animated_image(bytes: &[u8]) -> bool {
    use image::AnimationDecoder;

    if bytes.starts_with(b"GIF8") {
        let Ok(decoder) = image::codecs::gif::GifDecoder::new(std::io::Cursor::new(bytes)) else {
            return false;
        };
        // Two frames are enough to know it moves; counting the whole stream on a
        // 20-frame sticker would cost far more than the answer is worth.
        return decoder.into_frames().take(2).count() > 1;
    }
    if bytes.len() > 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return image::codecs::webp::WebPDecoder::new(std::io::Cursor::new(bytes))
            .map(|decoder| decoder.has_animation())
            .unwrap_or(false);
    }
    false
}

/// Which bytes a sticker frame actually carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StickerPayload {
    /// A still picture, normalized to a QQ-ready JPEG.
    Static,
    /// A multi-frame GIF or animated WebP, forwarded byte for byte.
    Animated,
}

impl StickerPayload {
    pub fn as_str(self) -> &'static str {
        match self {
            StickerPayload::Static => "static",
            StickerPayload::Animated => "animated",
        }
    }
}

/// The result of one send attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    /// The action was accepted; contains the new message id when reported.
    Sent { message_id: Option<String> },
    /// The action was rejected with a retcode. Not ambiguous, safe to retry later
    /// only by an operator decision.
    Rejected { retcode: i64, message: String },
    /// The response was missing, timed out, or the socket dropped: delivery is
    /// unknown and the message must never be sent again automatically.
    Unknown { reason: String },
}

/// Classify a raw response frame for `send_group_msg`.
pub fn classify_send_response(raw: &crate::onebot::event::RawActionResponse) -> SendOutcome {
    let retcode = raw.retcode.unwrap_or(-1);
    if retcode != 0 {
        let message = raw
            .message
            .clone()
            .or_else(|| raw.wording.clone())
            .unwrap_or_default();
        return SendOutcome::Rejected { retcode, message };
    }
    let message_id = raw
        .data
        .as_ref()
        .and_then(|d| d.get("message_id"))
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
    match message_id {
        Some(id) => SendOutcome::Sent {
            message_id: Some(id),
        },
        None => SendOutcome::Sent { message_id: None },
    }
}

/// Check that a response frame belongs to `echo`.
pub fn echo_matches(raw: &crate::onebot::event::RawActionResponse, echo: &str) -> bool {
    raw.echo.as_deref() == Some(echo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onebot::event::RawActionResponse;

    fn resp(json: &str) -> RawActionResponse {
        serde_json::from_str(json).expect("解析响应")
    }

    /// A solid-colour PNG of the given size, built inline so this test does not
    /// depend on private helpers in another module.
    fn png(w: u32, h: u32) -> Vec<u8> {
        let img = image::RgbaImage::from_fn(w, h, |x, y| {
            // Noise-free nonsense, but patterned enough that JPEG cannot trivially
            // crush it to nothing at high quality.
            image::Rgba([(x % 251) as u8, (y % 241) as u8, ((x * y) % 239) as u8, 255])
        });
        let mut bytes = Vec::new();
        image::DynamicImage::ImageRgba8(img)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .expect("编码 PNG");
        bytes
    }

    /// A plain picture must go out as `sub_type: 0` with its bytes verbatim:
    /// QQ renders `sub_type: 1` as a sticker, and a generated picture is a
    /// photo the group should be able to save and forward as one.
    #[test]
    fn plain_image_frame_is_sub_type_zero_and_verbatim() {
        let bytes = png(48, 48);
        let msg = SendGroupMsg::image("100", &bytes).expect("小图应当能发");
        let frame = msg.to_frame("e1");
        assert_eq!(frame["params"]["message"][0]["type"], "image");
        assert_eq!(frame["params"]["message"][0]["data"]["sub_type"], 0);
        let sent = msg.sent_image_bytes().expect("有图片段");
        assert_eq!(
            sent,
            crate::media::collage::base64_encode(&bytes),
            "原样转发，不重编码"
        );
    }

    #[test]
    fn plain_image_refuses_empty_but_not_size() {
        assert!(SendGroupMsg::image("100", &[]).is_none());
        // Incident nail (2026-11-11): a generated picture of ~1 MB was
        // refused by the borrowed sticker cap and never even attempted.
        // Sends have no size cap; this must build.
        let huge = vec![0xFFu8; STICKER_MAX_BASE64_BYTES];
        assert!(SendGroupMsg::image("100", &huge).is_some());
    }

    /// A sticker must go out as `sub_type: 1`, otherwise QQ renders a plain picture.
    #[test]
    fn sticker_frame_marks_the_animated_sub_type() {
        let msg = SendGroupMsg::sticker("100", &png(64, 64)).expect("小图应当能发");
        let frame = msg.to_frame("e1");
        assert_eq!(frame["params"]["message"][0]["type"], "image");
        assert_eq!(frame["params"]["message"][0]["data"]["sub_type"], 1);
        let file = frame["params"]["message"][0]["data"]["file"]
            .as_str()
            .expect("file 是字符串");
        assert!(file.starts_with("base64://"), "实际: {file}");
        assert_eq!(frame["params"]["auto_escape"], false);
    }

    /// The library may hold a picture close to a mebibyte, and base64 adds a third.
    /// Sending it verbatim would overflow the frame, so the payload is shrunk first.
    #[test]
    fn sticker_payload_is_shrunk_until_it_fits_the_frame() {
        let big = png(1400, 1400);
        let msg = SendGroupMsg::sticker("100", &big).expect("应当压到能发");
        let file = msg.message[0]["data"]["file"].as_str().expect("file");
        let payload = file.trim_start_matches("base64://");
        assert!(
            payload.len() <= STICKER_MAX_BASE64_BYTES,
            "base64 长度 {} 超过上限 {STICKER_MAX_BASE64_BYTES}",
            payload.len()
        );
    }

    /// Garbage is not an image, so there is nothing honest to send.
    #[test]
    fn non_image_bytes_are_refused() {
        assert!(SendGroupMsg::sticker("100", b"not an image at all").is_none());
    }

    /// A GIF with `frames` frames, each a whole-frame noise pattern so the encoder
    /// cannot collapse them into one another.
    fn gif(frames: usize, w: u32, h: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder = image::codecs::gif::GifEncoder::new(&mut bytes);
            encoder
                .set_repeat(image::codecs::gif::Repeat::Infinite)
                .expect("循环设置");
            for f in 0..frames {
                let img = image::RgbaImage::from_fn(w, h, |x, y| {
                    let seed = f as u32;
                    image::Rgba([
                        ((x + seed * 7) % 251) as u8,
                        ((y + seed * 13) % 241) as u8,
                        ((x * y + seed * 29) % 239) as u8,
                        255,
                    ])
                });
                encoder.encode_frame(image::Frame::new(img)).expect("写帧");
            }
        }
        bytes
    }

    fn decode_base64(payload: &str) -> Vec<u8> {
        fn val(c: u8) -> Option<u32> {
            Some(match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => return None,
            } as u32)
        }
        let data: Vec<u32> = payload.bytes().filter_map(val).collect();
        let mut out = Vec::with_capacity(data.len() * 3 / 4);
        for chunk in data.chunks(4) {
            let mut triple = 0u32;
            for (i, v) in chunk.iter().enumerate() {
                triple |= v << (18 - 6 * i);
            }
            out.push((triple >> 16) as u8);
            if chunk.len() > 2 {
                out.push((triple >> 8) as u8);
            }
            if chunk.len() > 3 {
                out.push(triple as u8);
            }
        }
        out
    }

    /// The whole point of the fix: a multi-frame GIF leaves byte-for-byte intact,
    /// so QQ receives the animation instead of a single re-encoded frame.
    #[test]
    fn a_two_frame_gif_is_sent_byte_for_byte() {
        let original = gif(2, 48, 48);
        assert!(original.starts_with(b"GIF8"), "测试素材应当是 GIF");
        let (msg, kind) = SendGroupMsg::sticker_tracked("100", &original, None).expect("应当能发");
        assert_eq!(kind, StickerPayload::Animated);
        let payload = msg.sent_image_bytes().expect("图片段存在");
        assert_eq!(decode_base64(&payload), original, "动图字节必须原样发出");
        // Still a sticker, not a plain picture.
        assert_eq!(msg.message[0]["data"]["sub_type"], 1);
    }

    /// One frame is not an animation: it takes the ordinary shrinker, which is
    /// what keeps a big still under the frame budget.
    #[test]
    fn a_single_frame_gif_is_treated_as_static() {
        let one = gif(1, 32, 32);
        let (_msg, kind) = SendGroupMsg::sticker_tracked("100", &one, None).expect("应当能发");
        assert_eq!(kind, StickerPayload::Static);
    }

    #[test]
    fn a_static_png_still_goes_through_the_shrinker() {
        let (_msg, kind) =
            SendGroupMsg::sticker_tracked("100", &png(32, 32), None).expect("应当能发");
        assert_eq!(kind, StickerPayload::Static);
    }

    /// Inflate a GIF past the frame budget by padding its comment extension: the
    /// animation itself stays two small frames, so the test stays fast while the
    /// base64 payload really does exceed the cap.
    fn padded_gif(target_raw: usize) -> Vec<u8> {
        let base = gif(2, 48, 48);
        let mut comment = vec![0x21, 0xFE];
        let pad = target_raw.saturating_sub(base.len() + 2 + 2);
        let mut left = pad;
        while left > 0 {
            let take = left.min(255);
            comment.push(take as u8);
            comment.extend(std::iter::repeat_n(b'#', take));
            left -= take;
        }
        comment.push(0x00);
        // A comment extension is only legal between blocks, i.e. after the header
        // (6 bytes), the logical screen descriptor (7) and the global colour table.
        let packed = base[10];
        let gct = if packed & 0x80 != 0 {
            3usize << ((packed & 0x07) + 1)
        } else {
            0
        };
        let split = 13 + gct;
        let mut out = Vec::with_capacity(base.len() + comment.len());
        out.extend_from_slice(&base[..split]);
        out.extend_from_slice(&comment);
        out.extend_from_slice(&base[split..]);
        out
    }

    /// An animation goes out byte for byte at any size: sends have no
    /// size cap (2026-11-11), so the old still-frame downgrade is gone.
    #[test]
    fn an_oversized_animation_goes_out_verbatim() {
        // Big enough that its base64 form passes the old sticker cap.
        let huge = padded_gif(STICKER_MAX_BASE64_BYTES / 4 * 3 + 4096);
        let encoded_len = crate::media::collage::base64_encode(&huge).len();
        assert!(
            encoded_len > STICKER_MAX_BASE64_BYTES,
            "素材不够大：base64 {encoded_len} <= {STICKER_MAX_BASE64_BYTES}"
        );
        let (msg, kind) =
            SendGroupMsg::sticker_tracked("100", &huge, None).expect("应当原样发而不是失败");
        assert_eq!(kind, StickerPayload::Animated);
        let payload = msg.sent_image_bytes().expect("图片段存在");
        assert_eq!(payload, crate::media::collage::base64_encode(&huge));
        let bytes = decode_base64(&payload);
        assert_eq!(&bytes[..4], b"GIF8", "原样转发应当还是 GIF");
    }

    #[test]
    fn text_frame_uses_array_format() {
        let msg = SendGroupMsg::text("100", "hello");
        let frame = msg.to_frame("e1");
        assert_eq!(frame["action"], "send_group_msg");
        assert_eq!(frame["echo"], "e1");
        assert_eq!(frame["params"]["group_id"], "100");
        assert_eq!(frame["params"]["message"][0]["type"], "text");
        assert_eq!(frame["params"]["message"][0]["data"]["text"], "hello");
        assert_eq!(frame["params"]["auto_escape"], true);
    }

    #[test]
    fn quoted_text_puts_the_reply_segment_first() {
        let msg = SendGroupMsg::text_quoted("100", "说的就是你", "4242");
        let frame = msg.to_frame("e1");
        assert_eq!(frame["params"]["message"][0]["type"], "reply");
        assert_eq!(frame["params"]["message"][0]["data"]["id"], "4242");
        assert_eq!(frame["params"]["message"][1]["type"], "text");
        assert_eq!(frame["params"]["message"][1]["data"]["text"], "说的就是你");
        assert_eq!(frame["params"]["auto_escape"], true);
    }

    #[test]
    fn quoted_sticker_keeps_reply_first_and_sub_type() {
        let msg = SendGroupMsg::sticker_quoted("100", &png(64, 64), "777").expect("小图应当能发");
        let frame = msg.to_frame("e1");
        assert_eq!(frame["params"]["message"][0]["type"], "reply");
        assert_eq!(frame["params"]["message"][0]["data"]["id"], "777");
        assert_eq!(frame["params"]["message"][1]["type"], "image");
        assert_eq!(frame["params"]["message"][1]["data"]["sub_type"], 1);
    }

    #[test]
    fn classify_send_response_covers_success_and_rejects() {
        assert_eq!(
            classify_send_response(&resp(
                r#"{"status":"ok","retcode":0,"data":{"message_id":12345,"group_id":100},"echo":"e"}"#,
            )),
            SendOutcome::Sent {
                message_id: Some("12345".into())
            }
        );

        match classify_send_response(&resp(
            r#"{"status":"failed","retcode":100,"message":"forbidden","echo":"e"}"#,
        )) {
            SendOutcome::Rejected { retcode, message } => {
                assert_eq!(retcode, 100);
                assert_eq!(message, "forbidden");
            }
            other => panic!("期待拒绝: {other:?}"),
        }

        // A frame without retcode must not be mistaken for a successful send.
        assert!(matches!(
            classify_send_response(&resp(r#"{"echo":"e"}"#)),
            SendOutcome::Rejected { .. }
        ));
    }

    #[test]
    fn echo_matching_is_exact() {
        let r = resp(r#"{"status":"ok","retcode":0,"echo":"abc"}"#);
        assert!(echo_matches(&r, "abc"));
        assert!(!echo_matches(&r, "ab"));
        assert!(!echo_matches(&r, ""));
    }
}
