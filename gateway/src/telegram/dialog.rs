use std::collections::HashSet;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::store::js_trim;

fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
pub(super) struct Question {
    pub id: String,
    pub question: String,
    #[serde(default, deserialize_with = "present")]
    pub header: Option<String>,
    pub options: Vec<Choice>,
    #[serde(default)]
    pub multi: bool,
    #[serde(default, deserialize_with = "present")]
    pub recommended: Option<usize>,
}

#[derive(Deserialize)]
pub(super) struct Choice {
    pub label: String,
    #[serde(default, deserialize_with = "present")]
    pub description: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub preview: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Answer {
    pub id: String,
    pub selected_options: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_input: Option<String>,
}

impl Answer {
    pub fn matches(&self, question: &Question) -> bool {
        let has_custom = self
            .custom_input
            .as_deref()
            .is_some_and(|value| !js_trim(value).is_empty());
        self.id == question.id
            && distinct(self.selected_options.iter().map(String::as_str))
            && self
                .selected_options
                .iter()
                .all(|label| question.options.iter().any(|choice| &choice.label == label))
            && (question.multi
                || (self.selected_options.len(), has_custom) == (1, false)
                || (self.selected_options.len(), has_custom) == (0, true))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Marker {
    Checkbox,
    Radio,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Selection {
    pub title: String,
    pub options: Vec<String>,
    #[serde(default, deserialize_with = "present")]
    pub selection_marker: Option<Marker>,
    pub markable_count: Option<usize>,
    pub checked_indices: Option<Vec<usize>>,
    #[serde(default, deserialize_with = "present")]
    pub initial_index: Option<usize>,
    #[serde(default)]
    pub option_details: Vec<Detail>,
}

#[derive(Deserialize)]
pub(super) struct Detail {
    #[serde(default, deserialize_with = "present")]
    pub description: Option<String>,
}

impl Selection {
    pub fn markable_count(&self) -> usize {
        self.markable_count.unwrap_or_else(|| {
            if self.selection_marker.is_some() {
                self.options.len()
            } else {
                0
            }
        })
    }

    fn valid(&self) -> bool {
        let count = self.markable_count();
        let checked = self.checked_indices.as_deref().unwrap_or_default();
        !self.options.is_empty()
            && self.options.iter().all(|option| !option.is_empty())
            && distinct(self.options.iter().map(String::as_str))
            && count <= self.options.len()
            && checked.iter().all(|index| *index < count)
            && distinct(checked.iter().copied())
            && self
                .initial_index
                .is_none_or(|index| index < self.options.len())
    }
}

#[derive(Deserialize)]
pub(super) struct TextQuestion {
    pub title: String,
    #[serde(default, deserialize_with = "present")]
    pub placeholder: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub prefill: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "lowercase")]
pub(super) enum Dialog {
    Confirm { title: String, message: String },
    Select(Selection),
    Input(TextQuestion),
    Editor(TextQuestion),
    Ask { questions: Vec<Question> },
}

impl Dialog {
    pub fn parse(request: &Value) -> Option<Self> {
        if request.get("secret").and_then(Value::as_bool) == Some(true) {
            return None;
        }
        let dialog = Self::deserialize(request).ok()?;
        let supported = match &dialog {
            Self::Select(details) => details.valid(),
            Self::Ask { questions } => {
                !questions.is_empty()
                    && distinct(questions.iter().map(|question| question.id.as_str()))
                    && questions.iter().all(|question| {
                        question
                            .options
                            .iter()
                            .all(|choice| !choice.label.is_empty())
                            && distinct(question.options.iter().map(|choice| choice.label.as_str()))
                            && question
                                .recommended
                                .is_none_or(|index| index < question.options.len())
                    })
            }
            Self::Confirm { .. } | Self::Input(_) | Self::Editor(_) => true,
        };
        supported.then_some(dialog)
    }
}

fn distinct<T: Eq + std::hash::Hash>(items: impl Iterator<Item = T>) -> bool {
    let mut seen = HashSet::new();
    items.into_iter().all(|value| seen.insert(value))
}
