//! Module to specify object paths in user friendly manners.

use std::fmt::{Debug, Display};

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

/// A path to a location in a tdms file.
///
/// These will format the path correctly and turn it into a owned string
/// (i.e. it does allocation)
///
/// ```rust
/// use tdms_lib::PropertyPath;
///
/// let path = PropertyPath::channel("group", "channel");
/// assert_eq!(path.path(), "/'group'/'channel'");
/// ```
///
/// The `[PropertyPath::file]` path is a special case and is used to specify the root of the file.
///
/// ```rust
/// use tdms_lib::PropertyPath;
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

/// Path for a channel in the TDMS file.
///
/// This is a wrapper around the [`PropertyPath`] to indicate that it is a channel.
///
/// ```rust
/// use tdms_lib::ChannelPath;
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
}
