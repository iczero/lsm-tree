//! Compaction filters

/// Trait for compaction filter objects.
pub trait CompactionFilter {
    /// Returns whether a k/v should be kept during compaction.
    fn should_keep(&mut self) -> bool;
}
