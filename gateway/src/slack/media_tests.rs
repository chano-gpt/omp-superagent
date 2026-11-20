use super::{authorized_file_url, message_payload, sections};

#[test]
fn authenticated_download_has_an_exact_slack_workspace_file_path() {
    let expected = "/files-pri/TLOCAL-FORIGINAL/";
    let approved =
        "https://files.slack.com/files-pri/TLOCAL-FORIGINAL/download/report.txt?origin_team=TLOCAL";
    assert!(authorized_file_url(approved, expected, true).is_some());
    for invalid in [
        "http://files.slack.com/files-pri/TLOCAL-FORIGINAL/report.txt",
        "https://user:pass@files.slack.com/files-pri/TLOCAL-FORIGINAL/report.txt",
        "https://files.slack.com:444/files-pri/TLOCAL-FORIGINAL/report.txt",
        "https://files.slack.com.attacker.invalid/files-pri/TLOCAL-FORIGINAL/report.txt",
        "https://files.slack.com/files-pri/TOTHER-FORIGINAL/report.txt",
        "https://files.slack.com/files-pri/TLOCAL-FOTHER/report.txt",
        "https://files.slack.com/files-pri/TLOCAL-FORIGINAL/%2E%2E/report.txt",
        "https://files.slack.com/files-pri/TLOCAL-FORIGINAL/download%2freport.txt",
        "https://files.slack.com/files-pri/TLOCAL-FORIGINAL/report.txt#unrelated",
    ] {
        assert!(
            authorized_file_url(invalid, expected, true).is_none(),
            "{invalid}"
        );
    }
}

#[test]
fn plain_text_fallback_never_expands_into_split_messages() {
    let text = "<!channel>".repeat(300);
    let value = message_payload(&text, sections(&text)).expect("plain card within the block limit");
    assert!(value.get("text").is_none());
    assert_eq!(value["blocks"][0]["text"]["text"], text);
    assert_eq!(value["blocks"][0]["text"]["type"], "plain_text");

    let text = format!("{}🦀a", "x".repeat(2_999));
    let blocks = sections(&text);
    assert_eq!(blocks[0]["text"]["text"], "x".repeat(2_999));
    assert_eq!(blocks[1]["text"]["text"], "🦀a");
}
