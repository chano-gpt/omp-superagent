use std::collections::HashSet;

use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::Value;

use super::model::utf16;
use super::rest::FILE_LIMIT;

pub(super) const PAGE: usize = 25;
pub(super) const MENU_TEXT: usize = 100;

fn present<'de, D: Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Clone, Deserialize)]
#[serde(tag = "method", rename_all = "lowercase")]
pub(super) enum Dialog {
    Confirm { title: String, message: String },
    Input(Input),
    Editor(Input),
    Select(Select),
    Ask { questions: Vec<Question> },
}

#[derive(Clone, Deserialize)]
pub(super) struct Input {
    pub title: String,
    #[serde(default, deserialize_with = "present")]
    pub placeholder: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub prefill: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Marker {
    Radio,
    Checkbox,
}

#[derive(Clone, Default, Deserialize)]
pub(super) struct Detail {
    #[serde(default, deserialize_with = "present")]
    pub description: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Select {
    pub title: String,
    pub options: Vec<String>,
    #[serde(default, deserialize_with = "present")]
    pub option_details: Option<Vec<Detail>>,
    #[serde(default, deserialize_with = "present")]
    pub selection_marker: Option<Marker>,
    pub checked_indices: Option<Vec<usize>>,
    pub markable_count: Option<usize>,
    #[serde(default, deserialize_with = "present")]
    pub initial_index: Option<usize>,
}

impl Select {
    pub fn markable(&self) -> usize {
        self.markable_count.unwrap_or(match &self.selection_marker {
            Some(_) => self.options.len(),
            None => 0,
        })
    }

    pub fn detail(&self, index: usize) -> Option<&str> {
        self.option_details
            .as_ref()?
            .get(index)?
            .description
            .as_deref()
    }

    pub fn marker(&self, index: usize) -> Option<&'static str> {
        match self
            .selection_marker
            .as_ref()
            .filter(|_| index < self.markable())?
        {
            Marker::Radio => Some("⚪"),
            Marker::Checkbox
                if self
                    .checked_indices
                    .as_ref()
                    .is_some_and(|indices| indices.contains(&index)) =>
            {
                Some("☑️")
            }
            Marker::Checkbox => Some("⬜"),
        }
    }
}

#[derive(Clone, Deserialize)]
pub(super) struct Question {
    pub id: String,
    pub question: String,
    #[serde(default, deserialize_with = "present")]
    pub header: Option<String>,
    pub options: Vec<OptionLabel>,
    #[serde(default, deserialize_with = "present")]
    pub multi: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub recommended: Option<usize>,
}

#[derive(Clone, Deserialize)]
pub(super) struct OptionLabel {
    pub label: String,
    #[serde(default, deserialize_with = "present")]
    pub description: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub preview: Option<String>,
}

impl Dialog {
    pub fn parse(request: &Value) -> Result<Self, &'static str> {
        if request["secret"] == true {
            return Err(
                "Secret input cannot safely be collected through a messenger; use the local OMP interface.",
            );
        }
        if request.to_string().len() > FILE_LIMIT - 64 * 1024 {
            return Err(
                "Dialog content exceeds the safe Discord attachment limit; use the local OMP interface.",
            );
        }
        let dialog: Self = serde_json::from_value(request.clone())
            .map_err(|_| "Malformed or unsupported native dialog; use the local OMP interface.")?;
        match &dialog {
            Self::Confirm { .. } => (),
            Self::Input(input) | Self::Editor(input) => {
                if input.title.is_empty()
                    || utf16(&input.title) > 45
                    || input
                        .placeholder
                        .as_ref()
                        .is_some_and(|text| utf16(text) > 100)
                    || input
                        .prefill
                        .as_ref()
                        .is_some_and(|text| utf16(text) > 4_000)
                {
                    return Err(
                        "Input content cannot fit a Discord modal; use the local OMP interface.",
                    );
                }
            }
            Self::Select(select) => {
                if select.options.is_empty()
                    || select.options.iter().any(String::is_empty)
                    || select.options.iter().collect::<HashSet<_>>().len() != select.options.len()
                    || select.markable() > select.options.len()
                    || select.checked_indices.as_ref().is_some_and(|indices| {
                        indices.iter().any(|&index| index >= select.markable())
                            || indices.iter().collect::<HashSet<_>>().len() != indices.len()
                    })
                    || select
                        .initial_index
                        .is_some_and(|index| index >= select.options.len())
                {
                    return Err(
                        "Invalid native select options or marker state; use the local OMP interface.",
                    );
                }
            }
            Self::Ask { questions } => {
                if questions.is_empty()
                    || questions
                        .iter()
                        .map(|q| &q.id)
                        .collect::<HashSet<_>>()
                        .len()
                        != questions.len()
                {
                    return Err(
                        "Invalid native ask question identity; use the local OMP interface.",
                    );
                }
                for question in questions {
                    if question.options.len() > PAGE
                        || question
                            .recommended
                            .is_some_and(|index| index >= question.options.len())
                        || question
                            .options
                            .iter()
                            .map(|option| &option.label)
                            .collect::<HashSet<_>>()
                            .len()
                            != question.options.len()
                        || question.options.iter().any(|option| {
                            option.label.is_empty()
                                || utf16(&option.label) > MENU_TEXT
                                || option
                                    .description
                                    .as_ref()
                                    .is_some_and(|text| utf16(text) > MENU_TEXT)
                        })
                    {
                        return Err(
                            "Ask choices cannot fit a Discord select menu; use the local OMP interface.",
                        );
                    }
                }
            }
        }
        Ok(dialog)
    }
}
