use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::adapter::EffectError;

use super::super::wire::js_len;

#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub(super) enum Dialog {
    Confirm { title: String, message: String },
    Select(Select),
    Input(Editor),
    Editor(Editor),
    Ask { questions: Vec<Question> },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Select {
    pub(super) title: String,
    pub(super) options: Vec<String>,
    #[serde(default)]
    pub(super) option_details: Vec<Detail>,
    pub(super) selection_marker: Option<Marker>,
    pub(super) markable_count: Option<usize>,
    #[serde(default)]
    pub(super) checked_indices: Vec<usize>,
    pub(super) initial_index: Option<usize>,
}

impl Select {
    pub(super) fn markable_count(&self) -> usize {
        self.markable_count.unwrap_or_else(|| {
            if self.selection_marker.is_some() {
                self.options.len()
            } else {
                0
            }
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Marker {
    Radio,
    Checkbox,
}

#[derive(Deserialize)]
pub(super) struct Detail {
    pub(super) description: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Editor {
    pub(super) title: String,
    pub(super) placeholder: Option<String>,
    pub(super) prefill: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Question {
    pub(super) id: String,
    pub(super) question: String,
    pub(super) header: Option<String>,
    pub(super) options: Vec<Choice>,
    #[serde(default)]
    pub(super) multi: bool,
    pub(super) recommended: Option<usize>,
}

#[derive(Deserialize)]
pub(super) struct Choice {
    pub(super) label: String,
    pub(super) description: Option<String>,
    pub(super) preview: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Response {
    Confirmed { confirmed: bool },
    Cancelled { cancelled: bool },
    Value { value: String },
    Answers { answers: Vec<Answer> },
}

impl Response {
    pub(super) const CANCELLED: Self = Self::Cancelled { cancelled: true };
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Answer {
    pub(super) id: String,
    pub(super) selected_options: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) custom_input: Option<String>,
}

impl Dialog {
    pub(super) fn parse(request: &Value) -> Result<Self, EffectError> {
        let unsupported = || {
            EffectError::rejected(
                "Slack cannot represent this native dialog; use the local OMP interface",
            )
        };
        if request["secret"] == true || request.to_string().len() > 100 * 1024 {
            return Err(unsupported());
        }
        let dialog = Self::deserialize(request).map_err(|_| unsupported())?;
        let valid = match &dialog {
            Self::Confirm { .. } => true,
            Self::Select(select) => {
                let count = select.options.len();
                let markable = select.markable_count();
                (1..=100).contains(&count)
                    && select
                        .options
                        .iter()
                        .all(|label| !label.is_empty() && js_len(label) <= 75)
                    && distinct(select.options.iter())
                    && select
                        .option_details
                        .iter()
                        .take(count)
                        .all(|detail| native_label(detail.description.as_deref()))
                    && markable <= count
                    && select.checked_indices.iter().all(|&index| index < markable)
                    && distinct(select.checked_indices.iter())
                    && select.initial_index.is_none_or(|index| index < count)
            }
            Self::Input(editor) | Self::Editor(editor) => {
                editor
                    .placeholder
                    .as_ref()
                    .is_none_or(|text| js_len(text) <= 150)
                    && editor
                        .prefill
                        .as_ref()
                        .is_none_or(|text| js_len(text) <= 3_000)
            }
            Self::Ask { questions } => {
                !questions.is_empty()
                    && distinct(questions.iter().map(|q| &q.id))
                    && questions.iter().all(|question| {
                        question.options.len() <= 100
                            && question.options.iter().all(|choice| {
                                !choice.label.is_empty()
                                    && js_len(&choice.label) <= 75
                                    && native_label(choice.description.as_deref())
                            })
                            && distinct(question.options.iter().map(|option| &option.label))
                            && question
                                .recommended
                                .is_none_or(|index| index < question.options.len())
                    })
            }
        };
        if valid {
            Ok(dialog)
        } else {
            Err(unsupported())
        }
    }
}

fn distinct<T: Eq + std::hash::Hash>(values: impl Iterator<Item = T>) -> bool {
    let mut seen = HashSet::new();
    values.into_iter().all(|value| seen.insert(value))
}

fn native_label(value: Option<&str>) -> bool {
    value.is_none_or(|label| js_len(label) <= 75)
}

impl Question {
    pub(super) fn validate_answer(&self, answer: &Answer) -> Result<(), EffectError> {
        let text = answer
            .custom_input
            .as_deref()
            .is_some_and(|text| !text.trim().is_empty());
        if answer.id != self.id
            || !distinct(answer.selected_options.iter())
            || answer
                .selected_options
                .iter()
                .any(|label| !self.options.iter().any(|option| &option.label == label))
            || (!self.multi
                && !matches!(
                    (answer.selected_options.len(), text),
                    (0, true) | (1, false)
                ))
        {
            return Err(EffectError::rejected(
                "Decision response does not match its original question and choices",
            ));
        }
        Ok(())
    }
}

pub(super) fn modal_input(view: &Value) -> Result<String, EffectError> {
    let input = &view["state"]["values"]["response"]["value"];
    if input["type"] == "plain_text_input" {
        match input.get("value") {
            Some(Value::Null) => return Ok(String::new()),
            Some(Value::String(value)) if js_len(value) <= 3_000 => return Ok(value.clone()),
            _ => {}
        }
    }
    Err(EffectError::rejected("Malformed Slack modal input"))
}

pub(super) fn selection(
    value: &Value,
    options: Vec<&str>,
    multi: bool,
) -> Result<Vec<String>, EffectError> {
    let invalid = || EffectError::rejected("Invalid Slack selection");
    let selected = if multi {
        value["selected_options"]
            .as_array()
            .ok_or_else(invalid)?
            .as_slice()
    } else {
        match value.get("selected_option") {
            Some(Value::Null) => &[],
            Some(value) => std::slice::from_ref(value),
            None => return Err(invalid()),
        }
    };
    let mut labels = Vec::<String>::new();
    for option in selected {
        let numeric = option["value"].as_str().ok_or_else(invalid)?;
        if (numeric.len() > 1 && numeric.starts_with('0'))
            || !numeric.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(invalid());
        }
        let index = numeric.parse::<usize>().map_err(|_| invalid())?;
        let label = options.get(index).ok_or_else(invalid)?;
        if labels.iter().any(|chosen| chosen.as_str() == *label) {
            return Err(EffectError::rejected("Duplicate Slack selected options"));
        }
        labels.push((*label).to_owned());
    }
    Ok(labels)
}
