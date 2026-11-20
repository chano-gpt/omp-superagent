use serde_json::{Value, json};

use super::approvals::Pending;
use super::dialogs::{Dialog, MENU_TEXT, PAGE, Select};
use super::model::utf16;

pub(super) fn control_id(token: &str, action: &str) -> String {
    format!("omp:{token}:{action}")
}

pub(super) fn split_control(value: &str) -> Option<(&str, &str)> {
    if value.len() > 100 {
        return None;
    }
    let (token, action) = value.strip_prefix("omp:")?.split_once(':')?;
    let token_valid = token.len() == 24
        && token
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'));
    let (name, suffix) = action
        .split_once('-')
        .map_or((action, None), |(name, suffix)| (name, Some(suffix)));
    let action_valid = !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_lowercase())
        && suffix
            .is_none_or(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()));
    (token_valid && action_valid).then_some((token, action))
}

fn row(components: Vec<Value>) -> Value {
    json!({ "type": 1, "components": components })
}

fn button(pending: &Pending, action: &str, label: &str, style: u8) -> Value {
    json!({ "type": 2, "style": style, "custom_id": control_id(&pending.token, action), "label": label })
}

fn menu(pending: &Pending, action: &str, options: Vec<Value>, min: usize, max: usize) -> Value {
    row(vec![
        json!({ "type": 3, "custom_id": control_id(&pending.token, action),
        "options": options, "min_values": min, "max_values": max }),
    ])
}

fn menu_text(value: &str) -> String {
    if utf16(value) <= MENU_TEXT {
        return value.to_owned();
    }
    let mut result = String::new();
    let mut used = 0;
    for ch in value.chars() {
        used += ch.len_utf16();
        if used >= MENU_TEXT {
            break;
        }
        result.push(ch);
    }
    result.push('…');
    result
}

fn select_view(pending: &Pending, select: &Select) -> (String, Value) {
    let page = pending
        .page
        .unwrap_or(select.initial_index.unwrap_or_default() / PAGE);
    let first = page * PAGE;
    let pages = select.options.len().div_ceil(PAGE);
    let labels: Vec<_> = select
        .options
        .iter()
        .enumerate()
        .skip(first)
        .take(PAGE)
        .collect();
    let numbered = pages > 1
        || labels.iter().any(|(index, label)| {
            utf16(label) > MENU_TEXT
                || select
                    .detail(*index)
                    .is_some_and(|detail| utf16(detail) > MENU_TEXT)
        });
    let mut content = select.title.clone();
    if pages > 1 {
        content.push_str(&format!("\nPage {}/{pages}", page + 1));
    }
    let mut options = Vec::with_capacity(labels.len());
    for (index, label) in labels {
        let name = if numbered {
            format!("#{}: {label}", index + 1)
        } else {
            label.clone()
        };
        let marker = select.marker(index);
        let detail = select.detail(index).filter(|s| !s.is_empty());
        if numbered || select.selection_marker.is_some() {
            content.push('\n');
            if let Some(marker) = marker {
                content.push_str(marker);
                content.push(' ');
            }
            content.push_str(&name);
            if Some(index) == select.initial_index {
                content.push_str(" (initial focus)");
            }
            if numbered && let Some(detail) = detail {
                content.push('\n');
                content.push_str(detail);
            }
        }
        let mut value = json!({ "label": menu_text(&name), "value": index.to_string() });
        if let Some(marker) = marker {
            value["emoji"] = json!({ "name": marker });
        }
        if let Some(detail) = detail {
            value["description"] = json!(menu_text(detail));
        }
        options.push(value);
    }
    if select.selection_marker.is_some() {
        content.push_str(
            "\nChoose one row per response; OMP supplies the checked state on each request.",
        );
    }
    let select_action = if page == 0 {
        "select".to_owned()
    } else {
        format!("select-{page}")
    };
    let mut controls = Vec::with_capacity(3);
    if page > 0 {
        controls.push(button(
            pending,
            &format!("page-{}", page - 1),
            "Previous",
            2,
        ));
    }
    if page + 1 < pages {
        controls.push(button(pending, &format!("page-{}", page + 1), "Next", 2));
    }
    controls.push(button(pending, "cancel", "Cancel", 2));
    (
        content,
        json!([menu(pending, &select_action, options, 1, 1), row(controls)]),
    )
}

pub(super) fn view(pending: &Pending) -> Option<(String, Value)> {
    let cancel = button(pending, "cancel", "Cancel", 2);
    Some(match &pending.dialog {
        Dialog::Confirm { title, message } => (
            format!("{title}\n\n{message}"),
            json!([row(vec![
                button(pending, "yes", "Confirm", 4),
                button(pending, "no", "Decline", 2),
                cancel
            ])]),
        ),
        Dialog::Input(input) | Dialog::Editor(input) => (
            format!(
                "{}\nOpen the editor to respond. Discord inputs are not secret; maximum 4000 characters.",
                input.title
            ),
            json!([row(vec![button(pending, "open", "Respond", 1), cancel])]),
        ),
        Dialog::Select(select) => select_view(pending, select),
        Dialog::Ask { questions } => {
            let index = pending.draft.question;
            let question = questions.get(index)?;
            let final_question = index + 1 == questions.len();
            let multi = question.multi.unwrap_or_default();
            let mut controls = Vec::with_capacity(2);
            if !question.options.is_empty() {
                let options = question
                    .options
                    .iter()
                    .enumerate()
                    .map(|(index, option)| {
                        let mut value = json!({ "label": option.label, "value": index.to_string(),
                        "default": pending.draft.selected.contains(&option.label) });
                        if let Some(text) = option.description.as_ref().filter(|s| !s.is_empty()) {
                            value["description"] = json!(text);
                        }
                        value
                    })
                    .collect();
                let max = if multi { question.options.len() } else { 1 };
                controls.push(menu(pending, &format!("choose-{index}"), options, 0, max));
            }
            controls.push(row(vec![
                button(pending, &format!("other-{index}"), "Other / free text", 2),
                button(
                    pending,
                    &format!("next-{index}"),
                    if final_question {
                        "Submit all answers"
                    } else {
                        "Next question"
                    },
                    1,
                ),
                cancel,
            ]));
            let mut content = format!("Question {}/{}", index + 1, questions.len());
            if let Some(header) = question.header.as_ref().filter(|s| !s.is_empty()) {
                content.push_str(&format!(" — {header}"));
            }
            content.push_str(&format!("\n{}", question.question));
            if let Some(context) = pending
                .approval
                .owner_context
                .as_ref()
                .filter(|s| !s.is_empty())
            {
                content.push_str(&format!("\nWhy your decision is needed: {context}"));
            }
            content.push_str(if multi {
                "\nMultiple answers allowed."
            } else {
                "\nChoose one option OR free text."
            });
            for (index, option) in question.options.iter().enumerate() {
                content.push_str(&format!("\n{}. {}", index + 1, option.label));
                if Some(index) == question.recommended {
                    content.push_str(" (recommended, not preselected)");
                }
                if let Some(preview) = option.preview.as_ref().filter(|s| !s.is_empty()) {
                    content.push('\n');
                    content.push_str(preview);
                }
            }
            if pending.draft.custom_input.is_some() {
                content.push_str("\nFree text entered.");
            }
            (content, Value::Array(controls))
        }
    })
}

pub(super) fn modal(pending: &Pending, action: &str) -> Option<Value> {
    let (title, label, style, placeholder, prefill) = match &pending.dialog {
        Dialog::Ask { .. } => (
            "Other answer",
            "Your answer (not secret)",
            2,
            None,
            pending.draft.custom_input.as_deref(),
        ),
        Dialog::Input(input) => (
            &*input.title,
            "Response (not secret)",
            1,
            input.placeholder.as_deref(),
            None,
        ),
        Dialog::Editor(input) => (
            &*input.title,
            "Response (not secret)",
            2,
            None,
            input.prefill.as_deref(),
        ),
        Dialog::Confirm { .. } | Dialog::Select(_) => return None,
    };
    let mut field = json!({ "type": 4, "custom_id": "value", "label": label, "style": style,
        "required": false, "max_length": 4000 });
    if let Some(value) = placeholder.filter(|s| !s.is_empty()) {
        field["placeholder"] = json!(value);
    }
    if let Some(value) = prefill.filter(|s| !s.is_empty()) {
        field["value"] = json!(value);
    }
    Some(
        json!({ "type": 9, "data": { "title": title, "custom_id": control_id(&pending.token, action),
        "components": [row(vec![field])] } }),
    )
}
