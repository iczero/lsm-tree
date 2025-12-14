use lsm_tree::compaction::CompactionOptions;
use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_builds_filters() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .expect_point_read_hits(false)
    .open()?;

    tree.insert("a", "a", 0);

    tree.flush_active_memtable(0)?;
    assert!(tree.filter_size() > 0);

    tree.major_compact(
        u64::MAX,
        CompactionOptions {
            seqno_threshold: 0,
            ..Default::default()
        },
    )?;
    assert!(tree.filter_size() > 0);

    Ok(())
}

#[test]
fn tree_expect_point_read_hits() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .expect_point_read_hits(true)
    .open()?;

    tree.insert("a", "a", 0);

    tree.flush_active_memtable(0)?;
    assert!(tree.filter_size() > 0);

    tree.major_compact(
        u64::MAX,
        CompactionOptions {
            seqno_threshold: 0,
            ..Default::default()
        },
    )?;
    assert!(tree.filter_size() == 0);

    Ok(())
}
