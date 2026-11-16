//! Group memory: retrieval for each turn, and asynchronous compaction of reviewed
//! memories by the agent's own memory session.

pub mod chunker;
pub mod compaction;
pub mod consolidate;
pub mod digest;
pub mod extract;
pub mod handover;
pub mod migrate_v1;
pub mod retrieval;
pub mod schema;
pub mod sealed;
pub mod stickers;

pub use chunker::{Chunker, FoldOutcome, FoldSkip};
pub use compaction::{CompactionRequest, CompactionScheduler, CompactionThresholds};
pub use consolidate::{ConsolidationReport, Consolidator};
pub use digest::{Digest, DigestEngine, DigestRequest};
pub use retrieval::{MemoryContext, MemoryContextBuilder, RetrievalInput};
pub use stickers::StickerDraft;
