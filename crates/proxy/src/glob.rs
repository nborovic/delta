//! Names and prefix globs: an exact name, `prefix*`, or `*`.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Glob {
    Exact(String),
    Prefix(String),
    Any,
}

impl Glob {
    /// Parses a pattern; `None` when it is not one of the three forms.
    pub fn parse(s: &str) -> Option<Glob> {
        if s == "*" {
            return Some(Glob::Any);
        }
        match s.find('*') {
            None if !s.is_empty() => Some(Glob::Exact(s.to_owned())),
            Some(i) if i == s.len() - 1 && i > 0 => Some(Glob::Prefix(s[..i].to_owned())),
            _ => None,
        }
    }

    pub fn matches(&self, name: &str) -> bool {
        match self {
            Glob::Exact(e) => e == name,
            Glob::Prefix(p) => name.starts_with(p.as_str()),
            Glob::Any => true,
        }
    }

    /// Higher is more specific: an exact name beats any glob, a longer
    /// prefix beats a shorter one, and `*` comes last.
    pub fn specificity(&self) -> usize {
        match self {
            Glob::Exact(_) => usize::MAX,
            Glob::Prefix(p) => p.len() + 1,
            Glob::Any => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_forms() {
        assert_eq!(Glob::parse("*"), Some(Glob::Any));
        assert_eq!(Glob::parse("get*"), Some(Glob::Prefix("get".into())));
        assert_eq!(Glob::parse("getSlot"), Some(Glob::Exact("getSlot".into())));
        assert_eq!(Glob::parse("*Subscribe"), None);
        assert_eq!(Glob::parse("g*t"), None);
        assert_eq!(Glob::parse("get**"), None);
        assert_eq!(Glob::parse(""), None);
    }
}
