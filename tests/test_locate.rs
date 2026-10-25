use filecheck::{GenerateOptions, HashType, LocateStatus, Locator};
use std::collections::HashMap;

fn generate(dir: &std::path::Path) -> filecheck::Dump{
    filecheck::generate(dir, &GenerateOptions{ jobs: 2, hashtype: HashType::SHA256, ..GenerateOptions::default() }).unwrap().dump
}

fn statuses(entries: &[filecheck::LocateEntry]) -> HashMap<String, LocateStatus>{
    entries.iter().map(|e| (e.path.clone(), e.status)).collect()
}

#[test]
fn test_locate_any_path_and_exclude() {
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.txt"), "aaa").unwrap();
    std::fs::write(src.path().join("b.txt"), "bbb").unwrap();
    std::fs::write(src.path().join("c.txt"), "ccc").unwrap();
    std::fs::write(src.path().join("d.txt"), "ddd").unwrap();

    // a: renamed in dest1; b: only under the excluded dir; c: in dest2 under excluded and normal path; d: nowhere
    let dest1 = tempfile::tempdir().unwrap();
    std::fs::create_dir(dest1.path().join("_trash")).unwrap();
    std::fs::write(dest1.path().join("renamed.txt"), "aaa").unwrap();
    std::fs::write(dest1.path().join("_trash").join("b.txt"), "bbb").unwrap();
    let dest2 = tempfile::tempdir().unwrap();
    std::fs::create_dir(dest2.path().join("_trash")).unwrap();
    std::fs::write(dest2.path().join("_trash").join("c.txt"), "ccc").unwrap();
    std::fs::write(dest2.path().join("keep.txt"), "ccc").unwrap();

    let src_dump = generate(src.path());
    let exclude = vec!["_trash".to_string()];
    let mut locator = Locator::new(&src_dump);
    locator.add_dest("d1", &generate(dest1.path()), &exclude).unwrap();
    locator.add_dest("d2", &generate(dest2.path()), &exclude).unwrap();
    let entries = locator.finish(None).unwrap();

    let s = statuses(&entries);
    assert_eq!(s["a.txt"], LocateStatus::Found);
    assert_eq!(s["b.txt"], LocateStatus::ExcludedOnly);
    assert_eq!(s["c.txt"], LocateStatus::Found);
    assert_eq!(s["d.txt"], LocateStatus::Missing);
    let a = entries.iter().find(|e| e.path == "a.txt").unwrap();
    assert_eq!(a.location.as_deref(), Some("d1:renamed.txt"));
    let c = entries.iter().find(|e| e.path == "c.txt").unwrap();
    assert_eq!(c.location.as_deref(), Some("d2:keep.txt"));
    // sorted by path
    assert!(entries.windows(2).all(|w| w[0].path <= w[1].path));
}

#[test]
fn test_locate_expect_with_prefix() {
    // staging root has sub/x.txt and sub/y.txt; x is unchanged, y changed afterwards, z is new
    let staging = tempfile::tempdir().unwrap();
    std::fs::create_dir(staging.path().join("sub")).unwrap();
    std::fs::write(staging.path().join("sub").join("x.txt"), "xxx").unwrap();
    std::fs::write(staging.path().join("sub").join("y.txt"), "old").unwrap();
    let now = tempfile::tempdir().unwrap();
    std::fs::write(now.path().join("x.txt"), "xxx").unwrap();
    std::fs::write(now.path().join("y.txt"), "new").unwrap();
    std::fs::write(now.path().join("z.txt"), "zzz").unwrap();
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("all"), "xxx").unwrap();
    std::fs::write(dest.path().join("y"), "new").unwrap();
    std::fs::write(dest.path().join("z"), "zzz").unwrap();

    let src_dump = generate(now.path());
    let staging_dump = generate(staging.path());
    let mut locator = Locator::new(&src_dump);
    locator.add_dest("nas", &generate(dest.path()), &[]).unwrap();
    let s = statuses(&locator.finish(Some((&staging_dump, "sub/"))).unwrap());
    assert_eq!(s["x.txt"], LocateStatus::Found);
    assert_eq!(s["y.txt"], LocateStatus::Changed);
    assert_eq!(s["z.txt"], LocateStatus::NotExpected);
}
