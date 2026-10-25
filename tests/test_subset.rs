use filecheck::{GenerateOptions, HashType};

fn opts(hashtype: HashType) -> GenerateOptions{
    GenerateOptions{ jobs: 4, hashtype, ..GenerateOptions::default() }
}

#[test]
fn test_buffer_size_does_not_change_checksum() {
    let dir = tempfile::tempdir().unwrap();
    // larger than several 8K buffers and not a multiple of any buffer size below
    let data: Vec<u8> = (0..100_003_u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.path().join("big"), &data).unwrap();
    std::fs::write(dir.path().join("empty"), b"").unwrap();

    let mut results = Vec::new();
    for buffer_size in [1, 7, 8 * 1024, 1024 * 1024]{
        let generated = filecheck::generate(dir.path(), &GenerateOptions{ buffer_size, ..opts(HashType::SHA256) }).unwrap();
        results.push(filecheck::export_lines(&generated.dump));
    }
    assert!(results.windows(2).all(|w| w[0] == w[1]));
    assert!(results[0].contains(&"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  ./empty".to_string()));
}

#[test]
fn test_parse_size() {
    assert_eq!(filecheck::parse_size("8192").unwrap(), 8192);
    assert_eq!(filecheck::parse_size("8K").unwrap(), 8 * 1024);
    assert_eq!(filecheck::parse_size("8k").unwrap(), 8 * 1024);
    assert_eq!(filecheck::parse_size("1M").unwrap(), 1024 * 1024);
    assert!(filecheck::parse_size("0").is_err());
    assert!(filecheck::parse_size("abc").is_err());
    assert!(filecheck::parse_size("").is_err());
}

#[test]
fn test_subset() {
    // Create the first temporary folder
    let temp_dir1 = tempfile::tempdir().expect("Failed to create temporary directory");
    let temp_dir1_path = temp_dir1.path();

    // Create the second temporary folder
    let temp_dir2 = tempfile::tempdir().expect("Failed to create temporary directory");
    let temp_dir2_path = temp_dir2.path();

    // Create the files in the first temporary folder
    std::fs::write(temp_dir1_path.join("a.txt"), "Content of a.txt").expect("Failed to create file");
    std::fs::write(temp_dir1_path.join("b.txt"), "Content of b.txt").expect("Failed to create file");

    // Create the files in the second temporary folder
    std::fs::write(temp_dir2_path.join("a.txt"), "Content of a.txt").expect("Failed to create file");
    std::fs::write(temp_dir2_path.join("b.txt"), "Content of b.txt").expect("Failed to create file");
    std::fs::write(temp_dir2_path.join("c.txt"), "Content of c.txt").expect("Failed to create file");

    let filecheck_output1 = filecheck::generate(temp_dir1_path, &opts(HashType::SHA256)).expect("Failed to generate filecheck output");
    let filecheck_output2 = filecheck::generate(temp_dir2_path, &opts(HashType::SHA256)).expect("Failed to generate filecheck output");

    let report = filecheck::subset(&filecheck_output1.dump, &filecheck_output2.dump).unwrap();
    assert!(report.is_subset());

    let report = filecheck::subset(&filecheck_output2.dump, &filecheck_output1.dump).unwrap();
    assert_eq!(report.missing_checksum, vec!["c.txt".to_string()]);
    assert!(report.missing_duplicate.is_empty());
}

#[test]
fn test_known_checksums() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("abc"), "abc").unwrap();

    let sha = filecheck::generate(dir.path(), &opts(HashType::SHA256)).unwrap();
    assert_eq!(filecheck::export_lines(&sha.dump),
        vec!["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad  ./abc".to_string()]);

    let md5 = filecheck::generate(dir.path(), &opts(HashType::MD5)).unwrap();
    assert_eq!(filecheck::export_lines(&md5.dump),
        vec!["900150983cd24fb0d6963f7d28e17f72  ./abc".to_string()]);
}

#[test]
fn test_includes_gitignored_hidden_and_nested() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sub/deeper")).unwrap();
    std::fs::write(root.join(".gitignore"), "*.log\nsub/\n").unwrap();
    std::fs::write(root.join("x.log"), "ignored by git").unwrap();
    std::fs::write(root.join(".hidden"), "hidden").unwrap();
    std::fs::write(root.join("sub/deeper/y.txt"), "nested").unwrap();
    std::fs::write(root.join(".DS_Store"), "mac").unwrap();
    std::fs::write(root.join("._x.log"), "mac").unwrap();

    let generated = filecheck::generate(root, &opts(HashType::SHA256)).unwrap();
    assert!(generated.failures.is_empty());
    let mut paths: Vec<String> = generated.dump.files.values().flatten().cloned().collect();
    paths.sort();
    assert_eq!(paths, vec![".gitignore", ".hidden", "sub/deeper/y.txt", "x.log"]);

    let with_mac = filecheck::generate(root, &GenerateOptions{ include_macos_metadata: true, ..opts(HashType::SHA256) }).unwrap();
    assert_eq!(with_mac.file_count, 6);
}

#[test]
fn test_same_content_different_path_is_missing_duplicate() {
    let src = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.txt"), "same").unwrap();
    std::fs::write(src.path().join("b.txt"), "same").unwrap();
    std::fs::write(dest.path().join("a.txt"), "same").unwrap();

    let s = filecheck::generate(src.path(), &opts(HashType::SHA256)).unwrap();
    let d = filecheck::generate(dest.path(), &opts(HashType::SHA256)).unwrap();
    let report = filecheck::subset(&s.dump, &d.dump).unwrap();
    assert!(report.missing_checksum.is_empty());
    assert_eq!(report.missing_duplicate, vec!["b.txt".to_string()]);
}

#[test]
fn test_dump_roundtrip_and_no_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a"), "a").unwrap();
    let generated = filecheck::generate(dir.path(), &opts(HashType::SHA256)).unwrap();

    let dump_path = out.path().join("x.dump");
    filecheck::write_dump(&generated.dump, &dump_path, false).unwrap();
    assert!(filecheck::write_dump(&generated.dump, &dump_path, false).is_err());
    filecheck::write_dump(&generated.dump, &dump_path, true).unwrap();

    let read = filecheck::read_dump(&dump_path).unwrap();
    assert_eq!(read.files, generated.dump.files);
    assert_eq!(read.hashtype, HashType::SHA256);
}

#[test]
fn test_mismatched_hashtypes_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a"), "a").unwrap();
    let sha = filecheck::generate(dir.path(), &opts(HashType::SHA256)).unwrap();
    let md5 = filecheck::generate(dir.path(), &opts(HashType::MD5)).unwrap();
    assert!(filecheck::subset(&sha.dump, &md5.dump).is_err());
}

#[cfg(unix)]
#[test]
fn test_non_utf8_and_backslash_names_are_encoded() {
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    // "OST " followed by GBK bytes that are not valid UTF-8
    let gbk = std::ffi::OsStr::from_bytes(b"OST \xC9\xCF\xBE\xED");
    std::fs::create_dir(dir.path().join(gbk)).unwrap();
    std::fs::write(dir.path().join(gbk).join("a.flac"), "x").unwrap();
    std::fs::write(dir.path().join("back\\slash"), "y").unwrap();

    let generated = filecheck::generate(dir.path(), &opts(HashType::SHA256)).unwrap();
    assert!(generated.failures.is_empty());
    let mut paths: Vec<String> = generated.dump.files.values().flatten().cloned().collect();
    paths.sort();
    assert_eq!(paths, vec!["OST \\xC9\\xCF\\xBE\\xED/a.flac".to_string(), "back\\\\slash".to_string()]);

    let lines = filecheck::export_lines(&generated.dump);
    assert!(lines.iter().all(|l| l.starts_with('\\')));
}

#[cfg(unix)]
#[test]
fn test_unreadable_directory_is_reported_and_walk_continues() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::write(locked.join("hidden.txt"), "x").unwrap();
    std::fs::write(dir.path().join("ok.txt"), "ok").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    // root can read anything, so only check when the directory is really unreadable
    if std::fs::read_dir(&locked).is_ok() {
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let generated = filecheck::generate(dir.path(), &opts(HashType::SHA256));
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    let generated = generated.unwrap();
    assert_eq!(generated.failures.len(), 1);
    assert!(generated.failures[0].0.ends_with("locked"));
    let paths: Vec<String> = generated.dump.files.values().flatten().cloned().collect();
    assert_eq!(paths, vec!["ok.txt".to_string()]);
}

#[cfg(unix)]
#[test]
fn test_unreadable_file_is_reported() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("secret");
    std::fs::write(&file, "x").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
    // root can read anything, so only check when the file is really unreadable
    if std::fs::File::open(&file).is_ok() {
        return;
    }
    let generated = filecheck::generate(dir.path(), &opts(HashType::SHA256)).unwrap();
    assert_eq!(generated.failures.len(), 1);
    assert!(generated.dump.files.is_empty());
}
