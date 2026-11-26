//! Relevance judgement for ambient chatter: is this burst about her?
//!
//! Deliberately a separate judge from the burst gate in
//! [`crate::app::preflight`]. The burst gate scores how *unfinished* a
//! burst looks and, by design, has no authority over whether she
//! replies. This judge answers exactly that other question — and only
//! for windows nothing in which is addressed to her, only as an intake
//! claim gate at the group's [`AmbientPace`] tier. Once a batch is
//! claimed she still decides `should_reply` herself: a Relevant
//! verdict never forces a reply, and an Unrelated one only holds the
//! batch at the gate (it stays pending and can still be claimed
//! later, e.g. when an addressed message lands in the same window).

use std::time::Duration;

use serde_json::Value;

use crate::onebot::normalize::IncomingMessage;
use crate::store::AmbientPace;

/// Whether an ambient burst is about her, as judged by the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relevance {
    Relevant,
    Unrelated,
    /// The judge could not answer. Intake treats it as Relevant
    /// (fail open): a broken judge must never silence her.
    Unavailable,
}

impl Relevance {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Relevant => "relevant",
            Self::Unrelated => "unrelated",
            Self::Unavailable => "unavailable",
        }
    }
}

/// The claim decision for one window, as a pure function so the whole
/// truth table is unit-testable. `addressed` windows always claim;
/// `cooldown_ready` is only consulted by the quiet tier.
pub fn gate_decision(
    pace: AmbientPace,
    addressed: bool,
    relevance: Relevance,
    cooldown_ready: bool,
) -> bool {
    if addressed {
        return true;
    }
    match pace {
        AmbientPace::Lively => true,
        AmbientPace::AddressedOnly => false,
        AmbientPace::Normal => relevance != Relevance::Unrelated,
        AmbientPace::Quiet => relevance != Relevance::Unrelated && cooldown_ready,
    }
}

/// The stable judging rules, passed as the call's system side. Byte
/// identical on every call, like the burst gate's rules.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_decision_truth_table() {
        use AmbientPace::*;
        // Addressed always claims, at every tier.
        for pace in [AddressedOnly, Quiet, Normal, Lively] {
            assert!(gate_decision(pace, true, Relevance::Unrelated, false));
        }
        assert!(!gate_decision(
            AddressedOnly,
            false,
            Relevance::Relevant,
            true
        ));
        assert!(gate_decision(Lively, false, Relevance::Unrelated, false));
        assert!(gate_decision(Normal, false, Relevance::Relevant, false));
        assert!(gate_decision(Normal, false, Relevance::Unavailable, false));
        assert!(!gate_decision(Normal, false, Relevance::Unrelated, true));
        assert!(gate_decision(Quiet, false, Relevance::Relevant, true));
        assert!(!gate_decision(Quiet, false, Relevance::Relevant, false));
        assert!(!gate_decision(Quiet, false, Relevance::Unrelated, true));
    }
}
