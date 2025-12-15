//! Compaction filters

use crate::coding::Decode;
use crate::compaction::worker::Options;
use crate::version::Version;
use crate::{BlobIndirection, InternalValue, Slice};

/// Trait for compaction filter objects.
pub trait CompactionFilter {
    #[allow(clippy::doc_markdown, reason = "thinks RocksDB is a Rust type")]
    /// Returns whether an item should be kept during compaction.
    /* TODO: perhaps prevented by super versions? check this
    ///
    /// ## Warning!
    ///
    /// Compaction filters ignore transactions. Any item filtered out (deleted)
    /// by a compaction filter will immediately stop existing for all readers,
    /// even those in a snapshot which would otherwise expect the item to still
    /// exist. This mirrors the behavior of RocksDB since 6.0.
    // note: for rocksdb behavior, see
    // <https://github.com/facebook/rocksdb/wiki/Compaction-Filter>
     */
    fn should_keep(&mut self, item: ItemAccessor<'_>) -> bool;
}

/// Accessor for the key/value from a compaction filter.
pub struct ItemAccessor<'a> {
    pub(crate) item: &'a InternalValue,
    pub(crate) opts: &'a Options,
    pub(crate) version: &'a Version,
}
impl<'a> ItemAccessor<'a> {
    /// Get the key of this item
    #[must_use]
    pub fn key(&self) -> &'a Slice {
        &self.item.key.user_key
    }

    /// Returns whether this item's value is stored separately.
    #[must_use]
    pub fn is_indirection(&self) -> bool {
        self.item.key.value_type.is_indirection()
    }

    /// Get the value of this item
    ///
    /// # Errors
    ///
    /// This method will return an error if blob retrieval fails.
    pub fn value(&self) -> crate::Result<Slice> {
        match self.item.key.value_type {
            crate::ValueType::Value => Ok(self.item.value.clone()),
            crate::ValueType::Tombstone => {
                let mut reader = &self.item.value[..];
                let indirection = BlobIndirection::decode_from(&mut reader)?;
                let vhandle = indirection.vhandle;

                if let Some(value) = self
                    .opts
                    .config
                    .cache
                    .get_blob(vhandle.blob_file_id, &vhandle)
                {
                    return Ok(value);
                }

                todo!();
            }
            crate::ValueType::WeakTombstone | crate::ValueType::Indirection => {
                unreachable!("tombstones are filtered out before calling filter")
            }
        }
    }
}
