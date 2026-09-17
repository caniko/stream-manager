use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Documented template variables (non-executable interpolation only).
pub const ALLOWED_TEMPLATE_VARS: &[&str] = &[
    "title",
    "description",
    "primary_platform",
    "primary_watch_url",
    "destination_platform",
    "start_local",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TemplateError {
    #[error("unknown variable: {0}")]
    UnknownVariable(String),
    #[error("unresolved variable: {0}")]
    UnresolvedVariable(String),
    #[error("unclosed placeholder")]
    UnclosedPlaceholder,
}

/// Render `{{name}}` placeholders. Unknown or unresolved variables are
/// hard errors; no code execution, no silent truncation.
pub fn render_template(
    template: &str,
    vars: &HashMap<String, String>,
) -> Result<String, TemplateError> {
    let allowed: HashSet<&str> = ALLOWED_TEMPLATE_VARS.iter().copied().collect();
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find("}}").ok_or(TemplateError::UnclosedPlaceholder)?;
        let name = after[..end].trim().to_string();
        if !allowed.contains(name.as_str()) {
            return Err(TemplateError::UnknownVariable(name));
        }
        let value = vars
            .get(&name)
            .ok_or_else(|| TemplateError::UnresolvedVariable(name.clone()))?;
        out.push_str(value);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Extract variable names referenced by a template (for preview/review).
pub fn referenced_vars(template: &str) -> Result<Vec<String>, TemplateError> {
    let mut names = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let end = after.find("}}").ok_or(TemplateError::UnclosedPlaceholder)?;
        names.push(after[..end].trim().to_string());
        rest = &after[end + 2..];
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> HashMap<String, String> {
        HashMap::from([
            ("title".into(), "Show".into()),
            ("primary_platform".into(), "YouTube".into()),
            (
                "primary_watch_url".into(),
                "https://youtube.com/watch?v=abc".into(),
            ),
        ])
    }

    #[test]
    fn renders_known_vars() {
        let out = render_template(
            "Watch on {{primary_platform}}: {{primary_watch_url}}",
            &vars(),
        )
        .unwrap();
        assert_eq!(out, "Watch on YouTube: https://youtube.com/watch?v=abc");
    }

    #[test]
    fn rejects_unknown_var() {
        assert_eq!(
            render_template("{{evil}}", &vars()).unwrap_err(),
            TemplateError::UnknownVariable("evil".into())
        );
    }

    #[test]
    fn rejects_unresolved_var() {
        assert_eq!(
            render_template("{{description}}", &vars()).unwrap_err(),
            TemplateError::UnresolvedVariable("description".into())
        );
    }

    #[test]
    fn rejects_unclosed() {
        assert_eq!(
            render_template("{{title", &vars()).unwrap_err(),
            TemplateError::UnclosedPlaceholder
        );
    }
}
