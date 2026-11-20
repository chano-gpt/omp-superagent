use super::{control_text, slash_text};

#[test]
fn normalizes_only_exact_owner_command_spellings() {
    for (input, expected) in [
        ("!resolve held-input", "/resolve held-input"),
        ("!effort\u{feff}high", "/effort\u{feff}high"),
        ("!resolved", "!resolved"),
        ("quoted !resolve", "quoted !resolve"),
        (" !new request", " !new request"),
        ("!model\u{85}model", "!model\u{85}model"),
    ] {
        assert_eq!(control_text(input.into()), expected);
    }
    assert_eq!(
        slash_text("/superagent-model", " exact\nargument \u{feff} ").as_deref(),
        Some("/model exact\nargument")
    );
    assert_eq!(slash_text("/superagent-status", "not accepted"), None);
    assert_eq!(slash_text("/status", ""), None);
}
