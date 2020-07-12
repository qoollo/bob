use std::error::Error;
use std::fs::{create_dir, metadata, read, write, Permissions};
use std::path::Path;
use std::process::Command;

pub fn populate_ssh_directory(
    directory: String,
    host_pub_key: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let pub_key_filename = format!("{}/id_rsa.pub", directory);
    if !Path::new(&pub_key_filename).exists() {
        create_dir(&directory);
        let _ = Command::new("ssh-keygen")
            .arg("-b")
            .arg("2048")
            .arg("-t")
            .arg("rsa")
            .arg("-f")
            .arg(&format!("{}/id_rsa", directory))
            .arg("-q")
            .arg("-N")
            .arg("")
            .output()?;
    }
    let mut pub_key = read(pub_key_filename)?;
    let mut authorized_keys_content = vec![];
    if let Some(host_pub_key) = host_pub_key {
        authorized_keys_content.append(&mut host_pub_key.into_bytes());
    }
    authorized_keys_content.extend("\n".to_string().into_bytes());
    authorized_keys_content.append(&mut pub_key);
    authorized_keys_content.extend("\n".to_string().into_bytes());
    let authorized_keys_filename = format!("{}/authorized_keys", directory);
    write(&authorized_keys_filename, authorized_keys_content)?;
    Ok(())
}
