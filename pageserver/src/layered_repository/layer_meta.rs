use bytes::Bytes;
use zenith_utils::lsn::Lsn;
use zenith_utils::multi_bitmap::LayeredBitmap;

const BLOCKS_PER_SEGMENT: u32 = 1 << 20; // 8GiB per segment

/// Layer metadata for in-memory layers and incremental (on-disk) layers.
/// Efficiency heavily relies on block-based relations.
pub struct NonSnapshotLayerMetadata {
    /// Bitmap defining which pages have been modified in this layer, and can
    /// thus be restored using this layer only.
    /// Because we only store booleans (yes/no), this maps to unit type.
    pages_contained: LayeredBitmap<()>,
    /// This contain an LSN for each page that was modified since the last
    /// snapshot, but have not been modified in this Layer. As such, this Lsn
    /// should be enough to find the correct Layer for page reconstruction;
    ///
    /// Note: As a consequence of this, LSNs stored in this map are of the
    /// range '(last snapshot's LSN, start of this layer)', that is, both
    /// exclusive.
    pages_modified_since_snapshot: LayeredBitmap<Lsn>,
}

enum PageFindResult {
    InThisLayer,
    InOtherLayer { last_modified: Lsn },
    UsePreviousSnapshot,
}

trait LayerMetadata {
    fn find_page(&self, block_num: u32) -> PageFindResult;
    fn blocknum_to_local(block_num: u32) -> usize {
        return (block_num % BLOCKS_PER_SEGMENT) as usize;
    }
}

trait MutLayerMetadata: LayerMetadata {
    fn add_mutated_page(&mut self, block_num: u32);
}

impl LayerMetadata for NonSnapshotLayerMetadata {
    fn find_page(&self, block_num: u32) -> PageFindResult {
        let offset = LayerMetadata::blocknum_to_local(block_num);

        return if self.pages_contained.get(offset) {
            PageFindResult::InThisLayer
        } else if Some(lsn) = self.pages_modified_since_snapshot.get(offset) {
            PageFindResult::InOtherLayer { last_modified: lsn }
        } else {
            PageFindResult::UsePreviousSnapshot
        };
    }
}

impl MutLayerMetadata for NonSnapshotLayerMetadata {
    fn add_mutated_page(&mut self, block_num: u32) {
        let offset = LayerMetadata::blocknum_to_local(block_num);
        if self.pages_contained.get_bool(offset) {
            return;
        }
        self.pages_modified_since_snapshot.reset(offset);
        self.pages_contained.set_bool(offset, true);
    }
}

trait LayerShortcuts {
    fn get_page_at_latest(&self, blcknum: u32) -> Option<Bytes>;
}
