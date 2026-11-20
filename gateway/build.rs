use std::{collections::BTreeMap, env, error::Error, fmt::Write as _, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    const CATALOG: &str = "../src/messenger-command-catalog.json";
    println!("cargo:rerun-if-changed={CATALOG}");

    let catalog: Vec<BTreeMap<String, String>> = serde_json::from_slice(&fs::read(CATALOG)?)?;
    let mut generated = String::from("&[\n");
    for command in catalog {
        let name = command
            .get("name")
            .ok_or("Messenger command is missing a name")?;
        writeln!(
            generated,
            "    ({name:?}, {}),",
            command.contains_key("text")
        )?;
    }
    generated.push_str("]\n");
    fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("messenger_commands.rs"),
        generated,
    )?;
    Ok(())
}
