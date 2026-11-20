/// Returns the optional-text flag, or `None` for an unsupported name.
pub(crate) fn accepts_text(name: &str) -> Option<bool> {
    const COMMANDS: &[(&str, bool)] = include!(concat!(env!("OUT_DIR"), "/messenger_commands.rs"));
    COMMANDS
        .iter()
        .find_map(|&(command, accepts_text)| (command == name).then_some(accepts_text))
}
