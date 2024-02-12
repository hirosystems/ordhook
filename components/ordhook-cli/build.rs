use std::process::Command;

fn current_git_hash() -> Option<String> {
    if option_env!("GIT_COMMIT") == None {
        let commit = Command::new("git")
            .arg("log")
            .arg("-1")
            .arg("--pretty=format:%h") // Abbreviated commit hash
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output();

        if let Ok(commit) = commit {
            if let Ok(commit) = String::from_utf8(commit.stdout) {
                return Some(commit);
            }
        }
    } else {
        return option_env!("GIT_COMMIT").map(String::from);
    }

    None
}

fn main() {
    // note: add error checking yourself.
    if let Some(git) = current_git_hash() {
        println!("cargo:rustc-env=GIT_COMMIT={}", git);
    }
}
