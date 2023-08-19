use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
};

pub fn read_file_content_at_path(file_path: &PathBuf) -> Result<Vec<u8>, String> {
    use std::fs::File;
    use std::io::BufReader;

    let file = File::open(file_path.clone())
        .map_err(|e| format!("unable to read file {}\n{:?}", file_path.display(), e))?;
    let mut file_reader = BufReader::new(file);
    let mut file_buffer = vec![];
    file_reader
        .read_to_end(&mut file_buffer)
        .map_err(|e| format!("unable to read file {}\n{:?}", file_path.display(), e))?;
    Ok(file_buffer)
}

pub fn write_file_content_at_path(file_path: &PathBuf, content: &[u8]) -> Result<(), String> {
    use std::fs::File;
    let mut parent_directory = file_path.clone();
    parent_directory.pop();
    fs::create_dir_all(&parent_directory).map_err(|e| {
        format!(
            "unable to create parent directory {}\n{}",
            parent_directory.display(),
            e
        )
    })?;
    let mut file = File::create(&file_path)
        .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
    file.write_all(content)
        .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
    Ok(())
}
