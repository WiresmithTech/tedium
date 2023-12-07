//! Module to specify object paths in user friendly manners.

use std::fmt::{Debug, Display};

use crate::error::TdmsError;

/// The internal type of paths into the TDMS file.
pub type ObjectPath<'a> = &'a str;
pub type ObjectPathOwned = String;

/// Names in the path must be escaped.
///
/// Single quotes must be replaced by double quotes
/// as specified in the TDMS Internal Structure document.
fn escape_name(name: &str) -> String {
    name.replace('\'', "\"")
}

/// Parse an object path into its components. Possible returns:
///
/// - Err: The path is invalid.
/// - Ok(None, None): The path is the root of the file.
/// - Ok(Some(group), None): The path is to a group.
/// - Ok(Some(group), Some(channel)): The path is to a channel.
fn parse_path(path: ObjectPath) -> Result<(Option<&str>, Option<&str>), TdmsError> {
    //Simple filter.
    if !path.starts_with('/') {
        return Err(TdmsError::InvalidObjectPath(path.to_string()));
    }

    //Early escape for root.
    if path.len() == 1 {
        return Ok((None, None));
    }

    let mut parts = path
        .split('/')
        .skip(1)
        .map(|p| parse_name(p).ok_or_else(|| TdmsError::InvalidObjectPath(path.to_string())));
    let group = invert(parts.next())?;
    let channel = invert(parts.next())?;

    if parts.next().is_some() {
        // There is more than one group or channel in the path.
        return Err(TdmsError::InvalidObjectPath(path.to_string()));
    }

    Ok((group, channel))
}

//expects name with quotes and removes the quotes.
fn parse_name(name: &str) -> Option<&str> {
    if !name.starts_with('\'') || !name.ends_with('\'') {
        return None;
    }

    Some(&name[1..name.len() - 1])
}

/// Get the group name for the path, if one exists.
pub fn path_group_name(path: ObjectPath) -> Option<&str> {
    parse_path(path).ok()?.0
}

fn invert<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

/// A path to a location in a tdms file.
///
/// These will format the path correctly and turn it into a owned string
/// (i.e. it does allocation)
///
/// ```rust
/// use tedium::PropertyPath;
///
/// let path = PropertyPath::channel("group", "channel");
/// assert_eq!(path.path(), "/'group'/'channel'");
/// ```
///
/// The `[PropertyPath::file]` path is a special case and is used to specify the root of the file.
///
/// ```rust
/// use tedium::PropertyPath;
///
/// let path = PropertyPath::file();
/// assert_eq!(path.path(), "/");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct PropertyPath(String);

impl PropertyPath {
    /// Path to the root of the file.
    pub fn file() -> Self {
        // We did originally look at using Cow so this case doesn't
        // allocate but this is the rare case so wasn't worth optimising.
        Self(String::from("/"))
    }

    /// Generate a path to a group.
    pub fn group(group: &str) -> Self {
        Self(format!("/'{}'", escape_name(group)))
    }

    /// Generate a path to a channel.
    pub fn channel(group: &str, channel: &str) -> Self {
        Self(format!(
            "/'{}'/'{}'",
            escape_name(group),
            escape_name(channel)
        ))
    }

    /// Get the path in the internal format.
    pub fn path(&self) -> ObjectPath {
        self.0.as_ref()
    }

    fn path_depth(&self) -> usize {
        self.0.chars().filter(|c| *c == '/').count()
    }

    fn is_channel(&self) -> bool {
        self.path_depth() == 2
    }
}

impl Debug for PropertyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for PropertyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> TryFrom<ObjectPath<'a>> for PropertyPath {
    type Error = TdmsError;

    fn try_from(value: ObjectPath) -> Result<Self, Self::Error> {
        let parsed = parse_path(value)?;
        match parsed {
            (None, None) => Ok(Self::file()),
            (Some(group), None) => Ok(Self::group(group)),
            (Some(group), Some(channel)) => Ok(Self::channel(group, channel)),
            _ => unreachable!(),
        }
    }
}

/// Path for a channel in the TDMS file.
///
/// This is a wrapper around the [`PropertyPath`] to indicate that it is a channel.
///
/// ```rust
/// use tedium::ChannelPath;
///
/// let path = ChannelPath::new("group", "channel");
/// assert_eq!(path.path(), "/'group'/'channel'");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelPath(PropertyPath);

impl ChannelPath {
    /// Get the path in the internal format.
    pub fn path(&self) -> ObjectPath {
        self.0.path()
    }

    /// Create a new channel path for the specified group and channel.
    ///
    /// NOTE: This allocates internally.
    pub fn new(group: &str, channel: &str) -> Self {
        Self(PropertyPath::channel(group, channel))
    }
}

// Needed to take slice of ChannelPath or &ChannelPath.
impl AsRef<ChannelPath> for ChannelPath {
    fn as_ref(&self) -> &ChannelPath {
        self
    }
}

// Needed to support input into the property path functions.
impl AsRef<PropertyPath> for ChannelPath {
    fn as_ref(&self) -> &PropertyPath {
        &self.0
    }
}

impl std::fmt::Display for ChannelPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", PropertyPath::path(&self.0))
    }
}

impl<'a> TryFrom<ObjectPath<'a>> for ChannelPath {
    type Error = TdmsError;

    fn try_from(value: ObjectPath) -> Result<Self, Self::Error> {
        let path = PropertyPath::try_from(value)?;
        path.try_into()
    }
}

impl TryFrom<PropertyPath> for ChannelPath {
    type Error = TdmsError;
    fn try_from(path: PropertyPath) -> Result<Self, Self::Error> {
        if !path.is_channel() {
            return Err(TdmsError::InvalidChannelPath(path.path().to_string()));
        }
        Ok(Self(path))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_root_path() {
        let path = PropertyPath::file();
        assert_eq!(path.path(), "/");
    }

    #[test]
    fn test_group_path() {
        let path = PropertyPath::group("group");
        assert_eq!(path.path(), "/'group'");
    }

    #[test]
    fn test_channel_path() {
        let path = PropertyPath::channel("group", "channel");
        assert_eq!(path.path(), "/'group'/'channel'");
    }

    #[test]
    fn test_channel_path_type() {
        let path = ChannelPath::new("group", "channel");
        assert_eq!(path.path(), "/'group'/'channel'");
    }

    #[test]
    fn test_group_escapes_chars() {
        let path = PropertyPath::group("group'with'quotes");
        assert_eq!(path.path(), r#"/'group"with"quotes'"#);
    }

    #[test]
    fn test_channel_escapes_chars() {
        let path = PropertyPath::channel("group'with'quotes", "channel'with'quotes");
        assert_eq!(path.path(), r#"/'group"with"quotes'/'channel"with"quotes'"#);
    }

    #[test]
    fn test_escapes_run_on_channel_paths() {
        let path = ChannelPath::new("group'with'quotes", "channel'with'quotes");
        assert_eq!(path.path(), r#"/'group"with"quotes'/'channel"with"quotes'"#);
    }

    #[test]
    fn test_correctly_identifies_group() {
        let path = PropertyPath::group("group");
        assert!(!path.is_channel());
    }

    #[test]
    fn test_correctly_identifies_channel() {
        let path = PropertyPath::channel("group", "channel");
        assert!(path.is_channel());
    }

    #[test]
    fn test_property_path_try_from_object_path_invalid() {
        let path = PropertyPath::try_from("invalid").unwrap_err();
        assert!(matches!(path, TdmsError::InvalidObjectPath(_)));
    }

    #[test]
    fn test_property_path_try_from_object_path_valid() {
        let path = PropertyPath::try_from("/'group'/'channel'").unwrap();
        assert_eq!(path.path(), "/'group'/'channel'");
    }

    #[test]
    fn test_property_path_try_from_object_path_valid_group() {
        let path = PropertyPath::try_from("/'group'").unwrap();
        assert_eq!(path.path(), "/'group'");
    }

    #[test]
    fn test_property_path_try_from_object_path_valid_root() {
        let path = PropertyPath::try_from("/").unwrap();
        assert_eq!(path.path(), "/");
    }

    #[test]
    fn test_channel_path_try_from_object_path_invalid() {
        let path = ChannelPath::try_from("invalid").unwrap_err();
        assert!(matches!(path, TdmsError::InvalidObjectPath(_)));
    }

    #[test]
    fn test_channel_path_try_from_object_path_valid() {
        let path = ChannelPath::try_from("/'group'/'channel'").unwrap();
        assert_eq!(path.path(), "/'group'/'channel'");
    }

    #[test]
    fn test_channel_path_try_from_object_path_invalid_group() {
        let path = ChannelPath::try_from("/'group'").unwrap_err();
        assert!(matches!(path, TdmsError::InvalidChannelPath(_)));
    }

    #[test]
    fn test_channel_path_try_from_object_path_invalid_root() {
        let path = ChannelPath::try_from("/").unwrap_err();
        assert!(matches!(path, TdmsError::InvalidChannelPath(_)));
    }

    #[test]
    fn test_path_group_name() {
        assert_eq!(path_group_name("/'group'/'channel'"), Some("group"));
        assert_eq!(path_group_name("/'group'"), Some("group"));
        assert_eq!(path_group_name("/"), None);
        assert_eq!(path_group_name("invalid"), None);
    }
}
