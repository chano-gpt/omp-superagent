use std::borrow::Cow;

use unicode_width::UnicodeWidthStr;

#[derive(Clone, Copy)]
enum Align {
    Left,
    Center,
    Right,
}

fn split_row(line: &str) -> Option<Vec<String>> {
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut input = line.chars().peekable();
    let mut ticks = 0;
    while let Some(ch) = input.next() {
        match ch {
            '\\' if input.next_if_eq(&'|').is_some() => cell.push('|'),
            '`' => {
                let mut run = 1;
                cell.push('`');
                while input.next_if_eq(&'`').is_some() {
                    cell.push('`');
                    run += 1;
                }
                if ticks == run {
                    ticks = 0;
                } else if ticks == 0 {
                    ticks = run;
                }
            }
            '|' if ticks == 0 => cells.push(std::mem::take(&mut cell)),
            _ => cell.push(ch),
        }
    }
    if cells.is_empty() {
        return None;
    }
    if cells.first().is_some_and(|s| s.trim().is_empty()) {
        cells.remove(0);
    }
    if !cell.trim().is_empty() {
        cells.push(cell);
    }
    (!cells.is_empty()).then(|| cells.into_iter().map(|s| s.trim().to_owned()).collect())
}

fn delimiters(line: &str) -> Option<Vec<Align>> {
    split_row(line)?
        .iter()
        .map(|cell| {
            let middle = cell.strip_prefix(':').unwrap_or(cell);
            let middle = middle.strip_suffix(':').unwrap_or(middle);
            if middle.is_empty() || !middle.bytes().all(|b| b == b'-') {
                return None;
            }
            Some(match (cell.starts_with(':'), cell.ends_with(':')) {
                (true, true) => Align::Center,
                (false, true) => Align::Right,
                (_, false) => Align::Left,
            })
        })
        .collect()
}

fn unindent(line: &str) -> Option<&str> {
    let trimmed = line.trim_start_matches(' ');
    (line.len() - trimmed.len() <= 3).then_some(trimmed)
}

fn fence(line: &str) -> Option<(u8, usize, &str)> {
    let line = unindent(line)?;
    let ch = *line.as_bytes().first()?;
    if !matches!(ch, b'`' | b'~') {
        return None;
    }
    let count = line.bytes().take_while(|b| *b == ch).count();
    (count >= 3).then(|| (ch, count, &line[count..]))
}

fn block_start(line: &str) -> bool {
    let Some(line) = unindent(line) else {
        return false;
    };
    let Some(first) = line.as_bytes().first() else {
        return false;
    };
    let marker = match first {
        b'>' => return true,
        b'#' => {
            let length = line.bytes().take_while(|b| *b == b'#').count();
            if length > 6 {
                return false;
            }
            length
        }
        b'-' | b'+' | b'*' => 1,
        b'0'..=b'9' => {
            let length = line.bytes().take_while(u8::is_ascii_digit).count();
            if length > 9 || !matches!(line.as_bytes().get(length), Some(b'.' | b')')) {
                return false;
            }
            length + 1
        }
        _ => return false,
    };
    matches!(line.as_bytes().get(marker), None | Some(b' ' | b'\t'))
}

fn table(rows: &[Vec<String>], aligns: &[Align], budget: usize) -> Option<String> {
    let mut widths = vec![3; aligns.len()];
    let mut extra_bytes = 0;
    for row in rows {
        for (column, cell) in row.iter().enumerate() {
            let width = cell.width();
            if let Some(previous) = widths.get_mut(column) {
                *previous = width.max(*previous);
            } else {
                widths.push(width.max(3));
            }
            extra_bytes += cell.len().saturating_sub(width);
        }
    }
    let row_width = 3 * widths.len() + 1 + widths.iter().sum::<usize>();
    let bytes = 8 + rows.len() + (rows.len() + 1) * row_width + extra_bytes;
    if bytes > budget {
        return None;
    }
    let mut out = String::with_capacity(bytes);
    out.push_str("```\n");
    for (index, row) in rows.iter().enumerate() {
        out.push('|');
        for (column, width) in widths.iter().copied().enumerate() {
            let cell = row.get(column).map_or("", String::as_str);
            let missing = width.saturating_sub(cell.width());
            let left = match aligns.get(column).copied().unwrap_or(Align::Left) {
                Align::Left => 0,
                Align::Center => missing / 2,
                Align::Right => missing,
            };
            out.extend(std::iter::repeat_n(' ', left + 1));
            out.push_str(cell);
            out.extend(std::iter::repeat_n(' ', missing - left + 1));
            out.push('|');
        }
        out.push('\n');
        if index == 0 {
            out.push('|');
            for &width in &widths {
                out.push(' ');
                out.extend(std::iter::repeat_n('-', width));
                out.push_str(" |");
            }
            out.push('\n');
        }
    }
    out.push_str("```");
    Some(out)
}

pub(super) fn render(text: &str, budget: usize) -> Cow<'_, str> {
    if !text.contains('|') || text.len() > budget {
        return Cow::Borrowed(text);
    }
    let mut lines = text.split('\n').peekable();
    let mut pieces = Vec::new();
    let mut bytes = text.len();
    let mut fenced = None;
    while let Some(line) = lines.next() {
        let marker = fence(line);
        if let Some((ch, length)) = fenced {
            if marker.is_some_and(|(kind, count, rest)| {
                kind == ch && count >= length && rest.trim_matches([' ', '\t', '\r']).is_empty()
            }) {
                fenced = None;
            }
        } else if let Some((ch, length, _)) = marker {
            fenced = Some((ch, length));
        } else if !block_start(line)
            && let Some(delimiter) = lines.peek().copied()
            && let Some(aligns) = delimiters(delimiter)
            && let Some(header) = split_row(line)
            && aligns.len() == header.len()
        {
            lines.next();
            let mut rows = vec![header];
            let mut old_bytes = line.len() + 1 + delimiter.len();
            while let Some(next) = lines.peek().copied() {
                if block_start(next) || fence(next).is_some() {
                    break;
                }
                let Some(row) = split_row(next) else {
                    break;
                };
                lines.next();
                rows.push(row);
                old_bytes += 1 + next.len();
            }
            let Some(rendered) = table(&rows, &aligns, budget - bytes + old_bytes) else {
                return Cow::Borrowed(text);
            };
            bytes = bytes - old_bytes + rendered.len();
            pieces.push(Cow::Owned(rendered));
            continue;
        }
        pieces.push(Cow::Borrowed(line));
    }
    if pieces.iter().all(|piece| matches!(piece, Cow::Borrowed(_))) {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(pieces.join("\n"))
    }
}
