use std::fmt::Write as _;

use serde_json::{Value, json};

use crate::adapter::EffectError;

use super::super::media::{plain, sections};
use super::super::wire::js_len;
use super::dialog::{Dialog, Marker};
use super::{Closure, Pending};

pub(super) struct Card {
    pub(super) text: String,
    pub(super) blocks: Vec<Value>,
}

impl Pending {
    pub(super) fn control(&self, action: &str) -> String {
        format!("omp:{}:{action}", self.token)
    }

    fn button(&self, action: &str, label: &str) -> Value {
        json!({
            "type": "button", "action_id": self.control(action), "text": plain(label), "value": action,
        })
    }

    pub(super) fn view(&self, closure: Option<Closure>) -> Result<Card, EffectError> {
        let mut controls = Vec::new();
        let mut row = 0;
        let mut actions = |elements: Vec<Value>| {
            let control = self.control(&format!("row-{}-{row}", self.question));
            row += 1;
            controls.push(json!({ "type": "actions", "block_id": control, "elements": elements }));
        };
        let cancel = self.button("cancel", "Cancel");
        let mut text = match &self.dialog {
            Dialog::Confirm { title, message } => {
                actions(vec![
                    self.button("yes", "Confirm"),
                    self.button("no", "Decline"),
                    cancel,
                ]);
                format!("{title}\n\n{message}")
            }
            Dialog::Input(editor) | Dialog::Editor(editor) => {
                actions(vec![self.button("open", "Respond"), cancel]);
                format!(
                    "{}\nRespond in the editor (not secret; maximum 3000 characters).",
                    editor.title
                )
            }
            Dialog::Select(select) => {
                let mut text = select.title.clone();
                if let Some(marker) = &select.selection_marker {
                    for (index, label) in select.options.iter().enumerate() {
                        let symbol = if index < select.markable_count() {
                            match (marker, select.checked_indices.contains(&index)) {
                                (Marker::Checkbox, true) => "[x]",
                                (Marker::Checkbox, false) => "[ ]",
                                (Marker::Radio, true) => "(x)",
                                (Marker::Radio, false) => "( )",
                            }
                        } else {
                            "—"
                        };
                        let focus = if select.initial_index == Some(index) {
                            " (initial focus)"
                        } else {
                            ""
                        };
                        write!(text, "\n{symbol} {label}{focus}").map_err(card_limit)?;
                    }
                    text.push_str(
                        "\nChoose one row; OMP supplies the checked state on each request.",
                    );
                }
                let options = select
                    .options
                    .iter()
                    .enumerate()
                    .map(|(index, label)| {
                        option(
                            index,
                            label,
                            select
                                .option_details
                                .get(index)
                                .and_then(|detail| detail.description.as_deref()),
                        )
                    })
                    .collect::<Vec<_>>();
                actions(vec![json!({
                    "type": "static_select", "action_id": self.control("select"),
                    "placeholder": plain("Choose one option"), "options": options,
                })]);
                actions(vec![cancel]);
                text
            }
            Dialog::Ask { questions } => {
                let question = questions
                    .get(self.question)
                    .ok_or_else(|| EffectError::rejected("Slack ask question does not exist"))?;
                let position = self.question + 1;
                let header = question
                    .header
                    .as_ref()
                    .filter(|header| !header.is_empty())
                    .map_or_else(String::new, |header| format!(" — {header}"));
                let mode = if question.multi {
                    "Multiple answers allowed."
                } else {
                    "Choose one option OR free text."
                };
                let mut text = format!(
                    "Question {position}/{}{header}\n{}\n{mode}\n",
                    questions.len(),
                    question.question
                );
                for (index, item) in question.options.iter().enumerate() {
                    if index > 0 {
                        text.push('\n');
                    }
                    let recommended = if question.recommended == Some(index) {
                        " (recommended, not preselected)"
                    } else {
                        ""
                    };
                    write!(text, "{}. {}{recommended}", index + 1, item.label)
                        .map_err(card_limit)?;
                    if let Some(preview) =
                        item.preview.as_ref().filter(|preview| !preview.is_empty())
                    {
                        write!(text, "\n{preview}").map_err(card_limit)?;
                    }
                }
                if !question.options.is_empty() {
                    let options = question
                        .options
                        .iter()
                        .enumerate()
                        .map(|(index, item)| {
                            option(index, &item.label, item.description.as_deref())
                        })
                        .collect::<Vec<_>>();
                    let selected = options
                        .iter()
                        .zip(&question.options)
                        .filter(|(_, item)| self.selected.contains(&item.label))
                        .map(|(value, _)| value.clone())
                        .collect::<Vec<_>>();
                    let mut element = json!({
                        "type": if question.multi { "multi_static_select" } else { "static_select" },
                        "action_id": self.control(&format!("choose-{}", self.question)),
                        "placeholder": plain("Choose options"), "options": options,
                    });
                    if question.multi {
                        element["max_selected_items"] = json!(question.options.len());
                        if !selected.is_empty() {
                            element["initial_options"] = json!(selected);
                        }
                    } else if let Some(selected) = selected.into_iter().next() {
                        element["initial_option"] = selected;
                    }
                    actions(vec![element]);
                }
                actions(vec![
                    self.button(&format!("other-{}", self.question), "Other / free text"),
                    self.button(
                        &format!("next-{}", self.question),
                        if position == questions.len() {
                            "Submit all answers"
                        } else {
                            "Next question"
                        },
                    ),
                    cancel,
                ]);
                text
            }
        };
        const MAX_TEXT: usize = 39_000;
        let room = MAX_TEXT.checked_sub(js_len(&text)).ok_or_else(|| {
            EffectError::rejected(
                "Slack dialog exceeds the message text limit; use the local OMP interface",
            )
        })?;
        if matches!(self.dialog, Dialog::Ask { .. })
            && let Some(context) = self
                .approval
                .owner_context
                .as_ref()
                .filter(|context| !context.is_empty())
        {
            const PREFIX: &str = "Why your decision is needed: ";
            if let Some(available) = room.checked_sub(PREFIX.len() + 2).filter(|&room| room > 0) {
                let reason = if js_len(context) <= available {
                    context.clone()
                } else {
                    let mut reason = String::new();
                    let mut left = available - 1; // One UTF-16 unit for the ellipsis.
                    for point in context.chars() {
                        let Some(room) = left.checked_sub(point.len_utf16()) else {
                            break;
                        };
                        reason.push(point);
                        left = room;
                    }
                    reason.push('…');
                    reason
                };
                text = format!("{PREFIX}{reason}\n\n{text}");
            }
        }
        let mut blocks = sections(&text);
        if let Some(block) = blocks.first_mut() {
            block["block_id"] = json!(self.control("card"));
        }
        if let Some(closure) = closure {
            if matches!(self.dialog, Dialog::Ask { .. }) {
                blocks.push(json!({
                    "type": "section", "block_id": self.control(closure.control()), "text": plain(closure.label()),
                }));
                text.push_str("\n\n");
                text.push_str(closure.label());
            }
        } else {
            blocks.append(&mut controls);
            if matches!(self.dialog, Dialog::Ask { .. }) && self.custom_input.is_some() {
                const DRAFT: &str = "Free text draft saved privately.";
                blocks.push(json!({ "type": "section", "text": plain(DRAFT) }));
                text.push_str("\n\n");
                text.push_str(DRAFT);
            }
        }
        Ok(Card { text, blocks })
    }
}

fn option(index: usize, label: &str, description: Option<&str>) -> Value {
    let mut value = json!({ "text": plain(label), "value": index.to_string() });
    if let Some(description) = description.filter(|text| !text.is_empty()) {
        value["description"] = plain(description);
    }
    value
}

fn card_limit(_: std::fmt::Error) -> EffectError {
    EffectError::rejected("Slack dialog could not be rendered")
}
