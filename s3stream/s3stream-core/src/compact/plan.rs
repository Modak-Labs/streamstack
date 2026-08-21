//! Compaction planning: which blocks of which objects become which new objects.
//!
//! CompactionStats,CompactOperations}`, `compact.objects.{CompactedObject,
//! CompactedObjectBuilder,CompactionType}`, `compact.utils.CompactionUtils`, and
//! `s3.StreamDataBlock`.
//!
//! - Input: this node's stream set objects' index entries, exploded into
//!   `StreamDataBlock`s (one per data block, tagged with source object).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use s3stream_object::DataBlockIndex;

/// One data block tagged with its source object: the unit compaction moves around.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDataBlock {
    pub object_id: u64,
    pub index: DataBlockIndex,
}

impl StreamDataBlock {
    pub fn new(
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        object_id: u64,
        block_position: u64,
        block_size: u32,
        record_count: u32,
    ) -> Self {
        Self {
            object_id,
            index: DataBlockIndex {
                block_id: -1,
                stream_id,
                start_offset,
                end_offset_delta: (end_offset - start_offset) as u32,
                record_count,
                start_position: block_position,
                size: block_size,
            },
        }
    }

    pub fn stream_id(&self) -> u64 {
        self.index.stream_id
    }

    pub fn start_offset(&self) -> u64 {
        self.index.start_offset
    }

    pub fn end_offset(&self) -> u64 {
        self.index.end_offset()
    }

    pub fn block_start_position(&self) -> u64 {
        self.index.start_position
    }

    pub fn block_size(&self) -> u32 {
        self.index.size
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionType {
    /// Blocks of one stream split out into a standalone stream object.
    Split,
    /// Blocks of many streams merged into a new stream set object.
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompactOperations {
    /// Normal object: delete. Composite object: delete the composite only.
    Delete = 0,
    /// Only delete the metadata in the control plane.
    KeepData = 1,
    /// Composite object: delete it and all linked objects.
    DeepDelete = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactResult {
    Success,
    Skipped,
    Failed,
}

/// One planned output object.
#[derive(Debug, Clone)]
pub struct CompactedObject {
    pub compaction_type: CompactionType,
    /// Blocks to copy, stream-major, offset-ordered (the ObjectWriter ordering rule).
    pub blocks: Vec<StreamDataBlock>,
}

impl CompactedObject {
    pub fn size(&self) -> u64 {
        self.blocks.iter().map(|b| b.block_size() as u64).sum()
    }
}

/// One memory-bounded stage of a compaction round.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub order: u32,
    pub compacted_objects: Vec<CompactedObject>,
    /// Blocks this stage must read, grouped by source object, position-ordered.
    pub stream_data_blocks_map: BTreeMap<u64, Vec<StreamDataBlock>>,
}

/// Mutable accumulator for one output object, including the current-stream
/// window that `split_current_stream` cuts on.
#[derive(Debug)]
struct CompactedObjectBuilder {
    compaction_type: CompactionType,
    blocks: Vec<StreamDataBlock>,
    curr_stream_index_head: isize,
    curr_stream_index_tail: isize,
}

impl CompactedObjectBuilder {
    fn new() -> Self {
        Self {
            compaction_type: CompactionType::Compact,
            blocks: Vec::new(),
            curr_stream_index_head: -1,
            curr_stream_index_tail: -1,
        }
    }

    fn split_current_stream(&mut self) -> CompactedObjectBuilder {
        self.split(self.curr_stream_index_head, self.curr_stream_index_tail)
    }

    fn split(&mut self, start: isize, end: isize) -> CompactedObjectBuilder {
        if start < 0 || end > self.curr_stream_index_tail {
            return CompactedObjectBuilder::new();
        }
        let mut builder = CompactedObjectBuilder::new();
        for block in self.blocks.drain(start as usize..end as usize) {
            builder.add_block(block);
        }
        builder.compaction_type = self.compaction_type;
        self.reset_curr_stream_position();
        builder
    }

    fn reset_curr_stream_position(&mut self) {
        self.curr_stream_index_head = -1;
        self.curr_stream_index_tail = -1;
        let mut curr_stream_id = None;
        for (i, block) in self.blocks.iter().enumerate() {
            if curr_stream_id != Some(block.stream_id()) {
                curr_stream_id = Some(block.stream_id());
                self.curr_stream_index_head = i as isize;
            }
            self.curr_stream_index_tail = i as isize + 1;
        }
    }

    fn last_stream_id(&self) -> Option<u64> {
        self.blocks.last().map(|b| b.stream_id())
    }

    fn last_offset(&self) -> Option<u64> {
        self.blocks.last().map(|b| b.end_offset())
    }

    fn add_block(&mut self, block: StreamDataBlock) {
        if Some(block.stream_id()) != self.last_stream_id() {
            self.curr_stream_index_head = self.blocks.len() as isize;
        }
        self.blocks.push(block);
        self.curr_stream_index_tail = self.blocks.len() as isize;
    }

    fn total_stream_num(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| b.stream_id())
            .collect::<HashSet<_>>()
            .len()
    }

    fn curr_stream_block_size(&self) -> u64 {
        if self.curr_stream_index_head < 0 || self.curr_stream_index_tail < 0 {
            return 0;
        }
        self.blocks[self.curr_stream_index_head as usize..self.curr_stream_index_tail as usize]
            .iter()
            .map(|b| b.block_size() as u64)
            .sum()
    }

    fn unique_object_ids(&self) -> BTreeSet<u64> {
        self.blocks.iter().map(|b| b.object_id).collect()
    }

    fn total_block_size(&self) -> u64 {
        self.blocks.iter().map(|b| b.block_size() as u64).sum()
    }

    fn merge(&mut self, other: CompactedObjectBuilder) {
        if other.compaction_type == CompactionType::Split {
            return;
        }
        self.blocks.extend(other.blocks);
        self.reset_curr_stream_position();
    }

    fn build(self) -> CompactedObject {
        CompactedObject {
            compaction_type: self.compaction_type,
            blocks: self.blocks,
        }
    }
}

struct CompactionStats {
    stream_num_in_stream_set: usize,
    stream_object_num: usize,
    object_to_compacted_object_num: BTreeMap<u64, usize>,
}

impl CompactionStats {
    fn of(builders: &[CompactedObjectBuilder]) -> Self {
        let mut stream_num_in_stream_set = 0;
        let mut stream_object_num = 0;
        let mut object_map: BTreeMap<u64, usize> = BTreeMap::new();
        for builder in builders {
            for object_id in builder.unique_object_ids() {
                *object_map.entry(object_id).or_insert(0) += 1;
            }
            if builder.compaction_type == CompactionType::Split && !builder.blocks.is_empty() {
                stream_object_num += 1;
            } else if builder.compaction_type == CompactionType::Compact {
                stream_num_in_stream_set += builder.total_stream_num();
            }
        }
        Self {
            stream_num_in_stream_set,
            stream_object_num,
            object_to_compacted_object_num: object_map,
        }
    }
}

fn total_object_stats(builder: &CompactedObjectBuilder, stats: &BTreeMap<u64, usize>) -> usize {
    builder
        .unique_object_ids()
        .iter()
        .map(|id| stats.get(id).copied().unwrap_or(0))
        .sum()
}

/// Sort stream data blocks by stream id, then start offset.
///
/// (TreeMap by stream id + stable
/// per-stream sort by start offset).
pub fn sort_stream_range_positions(
    stream_data_block_map: &HashMap<u64, Vec<StreamDataBlock>>,
) -> Vec<StreamDataBlock> {
    let mut by_stream: BTreeMap<u64, Vec<StreamDataBlock>> = BTreeMap::new();
    for blocks in stream_data_block_map.values() {
        for block in blocks {
            by_stream.entry(block.stream_id()).or_default().push(*block);
        }
    }
    let mut sorted = Vec::new();
    for (_, mut blocks) in by_stream {
        blocks.sort_by_key(StreamDataBlock::start_offset);
        sorted.extend(blocks);
    }
    sorted
}

/// Keep only objects that share at least one stream with another object.
pub fn filter_blocks_to_compact(
    stream_data_block_map: HashMap<u64, Vec<StreamDataBlock>>,
) -> HashMap<u64, Vec<StreamDataBlock>> {
    let mut stream_to_object_ids: HashMap<u64, HashSet<u64>> = HashMap::new();
    for blocks in stream_data_block_map.values() {
        for block in blocks {
            stream_to_object_ids
                .entry(block.stream_id())
                .or_default()
                .insert(block.object_id);
        }
    }
    let objects_to_compact: HashSet<u64> = stream_to_object_ids
        .values()
        .filter(|ids| ids.len() > 1)
        .flat_map(|ids| ids.iter().copied())
        .collect();
    stream_data_block_map
        .into_iter()
        .filter(|(object_id, _)| objects_to_compact.contains(object_id))
        .collect()
}

/// Group consecutive blocks while `predicate` holds (predicate is stateful).
pub fn group_stream_data_blocks<P: FnMut(&StreamDataBlock) -> bool>(
    blocks: &[StreamDataBlock],
    mut predicate: P,
) -> Vec<Vec<StreamDataBlock>> {
    let mut groups = Vec::new();
    let mut curr = Vec::new();
    for block in blocks {
        if predicate(block) {
            curr.push(*block);
        } else if !curr.is_empty() {
            groups.push(std::mem::take(&mut curr));
            curr.push(*block);
        }
    }
    if !curr.is_empty() {
        groups.push(curr);
    }
    groups
}

/// Stateful grouping predicate: cut groups at stream change, offset discontinuity, or
/// size/count/delta overflow.
pub struct GroupByLimitPredicate {
    block_size_threshold: u64,
    stream_id: i64,
    start_offset: u64,
    next_start_offset: u64,
    block_size: u64,
    record_cnt: u64,
}

impl GroupByLimitPredicate {
    pub fn new(block_size_threshold: u64) -> Self {
        Self {
            block_size_threshold,
            stream_id: -1,
            start_offset: 0,
            next_start_offset: 0,
            block_size: 0,
            record_cnt: 0,
        }
    }

    pub fn test(&mut self, block: &StreamDataBlock) -> bool {
        let mut flag = true;
        if self.stream_id == -1
            || block.stream_id() as i64 != self.stream_id
            || block.start_offset() != self.next_start_offset
            || self.block_size + block.block_size() as u64 >= self.block_size_threshold
            || self.record_cnt + block.index.record_count as u64 > i32::MAX as u64
            || block.end_offset() - self.start_offset > i32::MAX as u64
        {
            if self.stream_id != -1 {
                flag = false;
            }
            self.stream_id = block.stream_id() as i64;
            self.start_offset = block.start_offset();
            self.block_size = 0;
            self.record_cnt = 0;
        }
        self.next_start_offset = block.end_offset();
        self.block_size += block.block_size() as u64;
        self.record_cnt += block.index.record_count as u64;
        flag
    }
}

/// Stateful grouping predicate: cut groups at stream change or offset discontinuity.
pub struct GroupByOffsetPredicate {
    curr_stream_id: i64,
    next_start_offset: u64,
}

impl GroupByOffsetPredicate {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            curr_stream_id: -1,
            next_start_offset: 0,
        }
    }

    pub fn test(&mut self, block: &StreamDataBlock) -> bool {
        if self.curr_stream_id == -1 {
            self.curr_stream_id = block.stream_id() as i64;
            self.next_start_offset = block.end_offset();
            true
        } else if self.curr_stream_id == block.stream_id() as i64
            && self.next_start_offset == block.start_offset()
        {
            self.next_start_offset = block.end_offset();
            true
        } else {
            self.curr_stream_id = block.stream_id() as i64;
            self.next_start_offset = block.end_offset();
            false
        }
    }
}

/// Plan builder for stream set compaction.
pub struct CompactionAnalyzer {
    compaction_cache_size: u64,
    stream_split_size: u64,
    max_stream_num_in_stream_set: usize,
    max_stream_object_num: usize,
}

impl CompactionAnalyzer {
    pub fn new(
        compaction_cache_size: u64,
        stream_split_size: u64,
        max_stream_num_in_stream_set: usize,
        max_stream_object_num: usize,
    ) -> Self {
        Self {
            compaction_cache_size,
            stream_split_size,
            max_stream_num_in_stream_set,
            max_stream_object_num,
        }
    }

    /// `excluded_object_ids` collects objects
    pub fn analyze(
        &self,
        stream_data_block_map: HashMap<u64, Vec<StreamDataBlock>>,
        excluded_object_ids: &mut HashSet<u64>,
    ) -> Vec<CompactionPlan> {
        if stream_data_block_map.is_empty() {
            return Vec::new();
        }
        let stream_data_block_map = filter_blocks_to_compact(stream_data_block_map);
        if stream_data_block_map.is_empty() {
            return Vec::new();
        }
        match self.group_object_with_limits(stream_data_block_map, excluded_object_ids) {
            Some(builders) => self.generate_plan_with_cache_limit(builders),
            None => Vec::new(),
        }
    }

    fn group_object_with_limits(
        &self,
        mut stream_data_block_map: HashMap<u64, Vec<StreamDataBlock>>,
        excluded_object_ids: &mut HashSet<u64>,
    ) -> Option<Vec<CompactedObjectBuilder>> {
        let mut sorted_blocks = sort_stream_range_positions(&stream_data_block_map);
        let mut builders: Vec<CompactedObjectBuilder> = Vec::new();
        let mut stats: Option<CompactionStats> = None;
        loop {
            let mut objects_to_remove: HashSet<u64> = HashSet::new();
            if let Some(stats) = &stats {
                if stats.stream_object_num > self.max_stream_object_num {
                    tracing::warn!(
                        stream_object_num = stats.stream_object_num,
                        max = self.max_stream_object_num,
                        "stream object num exceeds limit, reducing objects to compact"
                    );
                    Self::add_objects_to_remove(
                        CompactionType::Split,
                        &builders,
                        stats,
                        &mut objects_to_remove,
                    );
                } else {
                    tracing::warn!(
                        stream_num = stats.stream_num_in_stream_set,
                        max = self.max_stream_num_in_stream_set,
                        "stream num in stream set exceeds limit, reducing objects to compact"
                    );
                    Self::add_objects_to_remove(
                        CompactionType::Compact,
                        &builders,
                        stats,
                        &mut objects_to_remove,
                    );
                }
                if objects_to_remove.is_empty() {
                    tracing::error!("unable to derive objects to exclude, compaction failed");
                    return None;
                }
            }
            if !objects_to_remove.is_empty() {
                excluded_object_ids.extend(objects_to_remove.iter().copied());
            }
            sorted_blocks.retain(|b| !objects_to_remove.contains(&b.object_id));
            for object_id in &objects_to_remove {
                stream_data_block_map.remove(object_id);
            }
            stream_data_block_map = filter_blocks_to_compact(stream_data_block_map);
            if stream_data_block_map.is_empty() {
                tracing::warn!("no viable objects to compact after exclusion");
                return None;
            }
            builders = self.compact_objects(&sorted_blocks)?;
            let new_stats = CompactionStats::of(&builders);
            let over_limit = new_stats.stream_num_in_stream_set > self.max_stream_num_in_stream_set
                || new_stats.stream_object_num > self.max_stream_object_num;
            stats = Some(new_stats);
            if !over_limit {
                return Some(builders);
            }
        }
    }

    fn add_objects_to_remove(
        compaction_type: CompactionType,
        builders: &[CompactedObjectBuilder],
        stats: &CompactionStats,
        objects_to_remove: &mut HashSet<u64>,
    ) {
        let candidates: Vec<&CompactedObjectBuilder> = builders
            .iter()
            .filter(|b| b.compaction_type == compaction_type)
            .collect();
        if candidates.is_empty() {
            return;
        }
        if compaction_type == CompactionType::Split {
            // Remove the stream object with fewest blocks (ties: fewest total object
            let mut sorted = candidates;
            sorted.sort_by_key(|b| {
                (
                    b.blocks.len(),
                    total_object_stats(b, &stats.object_to_compacted_object_num),
                )
            });
            objects_to_remove.extend(sorted[0].blocks.iter().map(|b| b.object_id));
        } else {
            // Remove the stream whose objects have minimum stream dispersion.
            let mut stream_object_ids_map: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
            let mut object_stream_ids_map: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
            for builder in &candidates {
                for block in &builder.blocks {
                    stream_object_ids_map
                        .entry(block.stream_id())
                        .or_default()
                        .insert(block.object_id);
                    object_stream_ids_map
                        .entry(block.object_id)
                        .or_default()
                        .insert(block.stream_id());
                }
            }
            let min_stream = stream_object_ids_map
                .iter()
                .map(|(stream_id, object_ids)| {
                    let dispersion: usize = object_ids
                        .iter()
                        .map(|id| object_stream_ids_map[id].len())
                        .sum();
                    (dispersion, *stream_id)
                })
                .min();
            if let Some((_, stream_id)) = min_stream {
                objects_to_remove.extend(stream_object_ids_map[&stream_id].iter().copied());
            }
        }
    }

    fn compact_objects(
        &self,
        sorted_blocks: &[StreamDataBlock],
    ) -> Option<Vec<CompactedObjectBuilder>> {
        let mut builders: Vec<CompactedObjectBuilder> = Vec::new();
        let mut builder = CompactedObjectBuilder::new();
        for block in sorted_blocks {
            match builder.last_stream_id() {
                None => builder.add_block(*block),
                Some(last_stream_id) if last_stream_id == block.stream_id() => {
                    let last_offset = builder.last_offset().unwrap();
                    if block.start_offset() > last_offset {
                        // Not continuous: split current run out as a stream object.
                        builder = Self::split_object(builder, &mut builders);
                        builder.add_block(*block);
                    } else if block.start_offset() == last_offset {
                        builder.add_block(*block);
                    } else {
                        tracing::error!(
                            last_offset,
                            block = ?block,
                            "FATAL: illegal stream range position"
                        );
                        return None;
                    }
                }
                Some(_) => {
                    builder = self.split_and_add_block(builder, *block, &mut builders);
                }
            }
        }
        if builder.curr_stream_block_size() > self.stream_split_size {
            Self::split_object(builder, &mut builders);
        } else {
            builders.push(builder);
        }
        Some(builders)
    }

    fn split_and_add_block(
        &self,
        mut builder: CompactedObjectBuilder,
        block: StreamDataBlock,
        builders: &mut Vec<CompactedObjectBuilder>,
    ) -> CompactedObjectBuilder {
        if builder.curr_stream_block_size() > self.stream_split_size {
            builder = Self::split_object(builder, builders);
        }
        builder.add_block(block);
        builder
    }

    fn split_object(
        mut builder: CompactedObjectBuilder,
        builders: &mut Vec<CompactedObjectBuilder>,
    ) -> CompactedObjectBuilder {
        let mut split_builder = builder.split_current_stream();
        split_builder.compaction_type = CompactionType::Split;
        if builder.total_block_size() != 0 {
            builders.push(builder);
        }
        builders.push(split_builder);
        CompactedObjectBuilder::new()
    }

    fn generate_plan_with_cache_limit(
        &self,
        builders: Vec<CompactedObjectBuilder>,
    ) -> Vec<CompactionPlan> {
        let mut plans = Vec::new();
        let mut compacted_objects: Vec<CompactedObject> = Vec::new();
        let mut stream_set_builder: Option<CompactedObjectBuilder> = None;
        let mut total_size = 0u64;
        let mut order = 0u32;
        let mut builders: Vec<Option<CompactedObjectBuilder>> =
            builders.into_iter().map(Some).collect();
        let mut i = 0;
        while i < builders.len() {
            let builder = builders[i].as_mut().unwrap();
            if total_size + builder.total_block_size() > self.compaction_cache_size {
                let mut end_offset = 0usize;
                let mut tmp_size = total_size;
                for (j, block) in builder.blocks.iter().enumerate() {
                    tmp_size += block.block_size() as u64;
                    if tmp_size > self.compaction_cache_size {
                        end_offset = j;
                        break;
                    }
                }
                if end_offset != 0 {
                    let prefix = builder.split(0, end_offset as isize);
                    stream_set_builder =
                        Self::add_or_merge(prefix, &mut compacted_objects, stream_set_builder);
                }
                plans.push(Self::generate_plan(
                    order,
                    std::mem::take(&mut compacted_objects),
                    stream_set_builder.take(),
                ));
                order += 1;
                total_size = 0;
            } else {
                let builder = builders[i].take().unwrap();
                total_size += builder.total_block_size();
                stream_set_builder =
                    Self::add_or_merge(builder, &mut compacted_objects, stream_set_builder);
                i += 1;
            }
        }
        if !compacted_objects.is_empty() || stream_set_builder.is_some() {
            plans.push(Self::generate_plan(
                order,
                compacted_objects,
                stream_set_builder,
            ));
        }
        plans
    }

    fn add_or_merge(
        builder: CompactedObjectBuilder,
        compacted_objects: &mut Vec<CompactedObject>,
        stream_set_builder: Option<CompactedObjectBuilder>,
    ) -> Option<CompactedObjectBuilder> {
        if builder.compaction_type == CompactionType::Split {
            compacted_objects.push(builder.build());
            stream_set_builder
        } else {
            let mut target = stream_set_builder.unwrap_or_else(CompactedObjectBuilder::new);
            target.merge(builder);
            Some(target)
        }
    }

    fn generate_plan(
        order: u32,
        mut compacted_objects: Vec<CompactedObject>,
        stream_set_builder: Option<CompactedObjectBuilder>,
    ) -> CompactionPlan {
        if let Some(builder) = stream_set_builder {
            compacted_objects.push(builder.build());
        }
        let mut stream_data_blocks_map: BTreeMap<u64, Vec<StreamDataBlock>> = BTreeMap::new();
        for compacted in &compacted_objects {
            for block in &compacted.blocks {
                stream_data_blocks_map
                    .entry(block.object_id)
                    .or_default()
                    .push(*block);
            }
        }
        for blocks in stream_data_blocks_map.values_mut() {
            blocks.sort_by_key(StreamDataBlock::block_start_position);
        }
        CompactionPlan {
            order,
            compacted_objects,
            stream_data_blocks_map,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(
        stream_id: u64,
        start: u64,
        end: u64,
        object_id: u64,
        pos: u64,
        size: u32,
    ) -> StreamDataBlock {
        StreamDataBlock::new(stream_id, start, end, object_id, pos, size, 1)
    }

    /// Base fixture layout:
    /// object 0: s0[0,20) s1[30,60) s2[30,60). Object 1: s0[20,25) s1[60,120).
    /// Object 2: s1[400,500) s2[230,270).
    fn base_map() -> HashMap<u64, Vec<StreamDataBlock>> {
        let mut map = HashMap::new();
        map.insert(
            0,
            vec![
                block(0, 0, 20, 0, 0, 20),
                block(1, 30, 60, 0, 20, 30),
                block(2, 30, 60, 0, 50, 30),
            ],
        );
        map.insert(
            1,
            vec![block(0, 20, 25, 1, 0, 5), block(1, 60, 120, 1, 5, 60)],
        );
        map.insert(
            2,
            vec![
                block(1, 400, 500, 2, 0, 100),
                block(2, 230, 270, 2, 100, 40),
            ],
        );
        map.insert(3, vec![block(3, 0, 50, 3, 0, 50)]);
        map
    }

    #[test]
    fn filter_drops_objects_without_shared_streams() {
        let filtered = filter_blocks_to_compact(base_map());
        // Object 3's stream 3 appears nowhere else.
        assert_eq!(filtered.len(), 3);
        assert!(!filtered.contains_key(&3));
    }

    #[test]
    fn sort_is_stream_major_offset_ordered() {
        let sorted = sort_stream_range_positions(&filter_blocks_to_compact(base_map()));
        let keys: Vec<(u64, u64)> = sorted
            .iter()
            .map(|b| (b.stream_id(), b.start_offset()))
            .collect();
        assert_eq!(
            keys,
            vec![
                (0, 0),
                (0, 20),
                (1, 30),
                (1, 60),
                (1, 400),
                (2, 30),
                (2, 230)
            ]
        );
    }

    #[test]
    fn analyze_splits_large_streams_and_compacts_rest() {
        let analyzer = CompactionAnalyzer::new(1 << 30, 100, 100, 100);
        let mut excluded = HashSet::new();
        let plans = analyzer.analyze(base_map(), &mut excluded);
        assert!(excluded.is_empty());
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        // Every input block from viable objects lands in exactly one output object.
        let total_blocks: usize = plan.compacted_objects.iter().map(|c| c.blocks.len()).sum();
        assert_eq!(total_blocks, 7);
        let splits: Vec<_> = plan
            .compacted_objects
            .iter()
            .filter(|c| c.compaction_type == CompactionType::Split)
            .collect();
        let compacts: Vec<_> = plan
            .compacted_objects
            .iter()
            .filter(|c| c.compaction_type == CompactionType::Compact)
            .collect();
        // Discontinuity at stream1 [120 -> 400) forces one split. Stream2's run
        // [30,60)+[230,270) is discontinuous too.
        assert!(!splits.is_empty());
        assert_eq!(compacts.len(), 1);
        // COMPACT blocks are stream-major and offset-ordered.
        let compact = compacts[0];
        let mut prev: Option<(u64, u64)> = None;
        for b in &compact.blocks {
            if let Some((ps, po)) = prev {
                assert!((b.stream_id(), b.start_offset()) >= (ps, po));
            }
            prev = Some((b.stream_id(), b.start_offset()));
        }
    }

    #[test]
    fn cache_limit_cuts_plans_into_stages() {
        // Budget 100 bytes. Total block size is 285 → multiple stages.
        let analyzer = CompactionAnalyzer::new(100, 1 << 30, 100, 100);
        let mut excluded = HashSet::new();
        let plans = analyzer.analyze(base_map(), &mut excluded);
        assert!(plans.len() > 1);
        for (i, plan) in plans.iter().enumerate() {
            assert_eq!(plan.order, i as u32);
            let stage_bytes: u64 = plan
                .stream_data_blocks_map
                .values()
                .flatten()
                .map(|b| b.block_size() as u64)
                .sum();
            // Each stage's read set stays within the memory budget (the closing
            // stage may include the split prefix that exactly fills it).
            assert!(
                stage_bytes <= 100 + 100,
                "stage {i} reads {stage_bytes} bytes"
            );
        }
        // No block lost across stages.
        let total: usize = plans
            .iter()
            .map(|p| p.stream_data_blocks_map.values().flatten().count())
            .sum();
        assert_eq!(total, 7);
    }

    #[test]
    fn stream_object_limit_excludes_objects() {
        let analyzer = CompactionAnalyzer::new(1 << 30, 30, 100, 1);
        let mut excluded = HashSet::new();
        let plans = analyzer.analyze(base_map(), &mut excluded);
        // Analyzer either produced a viable reduced plan or nothing. Excluded is
        // non-empty in both cases.
        assert!(!excluded.is_empty());
        for plan in &plans {
            let split_count = plan
                .compacted_objects
                .iter()
                .filter(|c| c.compaction_type == CompactionType::Split)
                .count();
            assert!(split_count <= 1);
        }
    }

    /// Golden conformance: run the analyzer on the exact inputs the Java
    /// `CompactionAnalyzer` was run on (`conformance/fixtures/compaction/plans.json`)
    /// and require identical plans: same stages, same object types, same blocks in
    /// the same order, same exclusions.
    #[test]
    fn plans_match_java_golden_fixtures() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/compaction/plans.json");
        let cases: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(path).expect("run conformance/generator first"),
        )
        .unwrap();
        for case in cases.as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let analyzer = CompactionAnalyzer::new(
                case["cache_size"].as_u64().unwrap(),
                case["stream_split_size"].as_u64().unwrap(),
                case["max_stream_num"].as_u64().unwrap() as usize,
                case["max_stream_object_num"].as_u64().unwrap() as usize,
            );
            let mut excluded = HashSet::new();
            let plans = analyzer.analyze(base_map(), &mut excluded);

            let mut expected_excluded: Vec<u64> = case["excluded"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .collect();
            expected_excluded.sort_unstable();
            let mut actual_excluded: Vec<u64> = excluded.into_iter().collect();
            actual_excluded.sort_unstable();
            assert_eq!(actual_excluded, expected_excluded, "case {name}: excluded");

            let expected_plans = case["plans"].as_array().unwrap();
            assert_eq!(plans.len(), expected_plans.len(), "case {name}: plan count");
            for (plan, expected) in plans.iter().zip(expected_plans) {
                assert_eq!(plan.order as u64, expected["order"].as_u64().unwrap());
                let expected_objects = expected["objects"].as_array().unwrap();
                assert_eq!(
                    plan.compacted_objects.len(),
                    expected_objects.len(),
                    "case {name} plan {}: object count",
                    plan.order
                );
                for (obj, expected_obj) in plan.compacted_objects.iter().zip(expected_objects) {
                    let expected_type = match expected_obj["type"].as_str().unwrap() {
                        "SPLIT" => CompactionType::Split,
                        "COMPACT" => CompactionType::Compact,
                        other => panic!("unknown type {other}"),
                    };
                    assert_eq!(obj.compaction_type, expected_type, "case {name}");
                    let actual_blocks: Vec<[u64; 6]> = obj
                        .blocks
                        .iter()
                        .map(|b| {
                            [
                                b.object_id,
                                b.stream_id(),
                                b.start_offset(),
                                b.end_offset(),
                                b.block_start_position(),
                                b.block_size() as u64,
                            ]
                        })
                        .collect();
                    let expected_blocks: Vec<[u64; 6]> = expected_obj["blocks"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|b| {
                            let v: Vec<u64> = b
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|x| x.as_u64().unwrap())
                                .collect();
                            v.try_into().unwrap()
                        })
                        .collect();
                    assert_eq!(actual_blocks, expected_blocks, "case {name}: blocks");
                }
            }
        }
    }

    #[test]
    fn group_by_offset_cuts_on_discontinuity() {
        let blocks = vec![
            block(1, 0, 10, 1, 0, 10),
            block(1, 10, 20, 1, 10, 10),
            block(1, 30, 40, 1, 20, 10),
            block(2, 0, 5, 1, 30, 5),
        ];
        let mut pred = GroupByOffsetPredicate::new();
        let groups = group_stream_data_blocks(&blocks, |b| pred.test(b));
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[2].len(), 1);
    }

    #[test]
    fn group_by_limit_cuts_on_size_threshold() {
        let blocks = vec![
            block(1, 0, 10, 1, 0, 10),
            block(1, 10, 20, 1, 10, 10),
            block(1, 20, 30, 1, 20, 10),
        ];
        let mut pred = GroupByLimitPredicate::new(25);
        let groups = group_stream_data_blocks(&blocks, |b| pred.test(b));
        // 10+10 < 25 keeps blocks 1-2 together. 20+10 >= 25 cuts before block 3.
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
    }
}
