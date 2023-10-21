//! Module to specify object paths in user friendly manners.

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
};

pub type ObjectPath<'a> = &'a str;
pub type ObjectPathOwned = String;

/// A path to a location in a tdms file.
///
/// It is recommended to use the static methods to create paths.
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
/// If you want to avoid allocation, you can use the `From<&str>` implementation
/// but you must use the format expected internally in the file.
///
///
/// ```rust
/// use tdms_lib::PropertyPath;
///
/// let path_str = "/'group'/'channel'";
/// let path: PropertyPath = path_str.into();
/// assert_eq!(path.path(), path_str);
/// ```
///
/// The FILE path is a special case and is used to specify the root of the file.
///
/// ```rust
/// use tdms_lib::PropertyPath;
///
/// let path = PropertyPath::FILE;
/// assert_eq!(path.path(), "/");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct PropertyPath<'a>(Cow<'a, str>);

impl<'a> PropertyPath<'a> {
    /// Specify the root of the file.
    pub const FILE: Self = Self(Cow::Borrowed("/"));

    /// Used internally to track a location where I can't tie it to a specific path.
    pub(crate) const UNSPECIFIED: Self = Self(Cow::Borrowed("Unspecified"));

    /// Generate a path to a group.
    pub fn group(group: &'a str) -> Self {
        Self(Cow::Owned(format!("/'{}'", group)))
    }

    /// Generate a path to a channel.
    pub fn channel<'b>(group: &'b str, channel: &'b str) -> Self {
        Self(Cow::Owned(format!("/'{}'/'{}'", group, channel)))
    }

    /// Get the path in the internal format.
    pub fn path(&self) -> ObjectPath {
        self.0.as_ref()
    }

    /// Produce an owned, statically allocated version of the path
    /// where ownership is required.
    pub(crate) fn to_static(&self) -> PropertyPath<'static> {
        let path = self.0.to_string();
        let inner: Cow<'static, str> = Cow::Owned(path);
        PropertyPath(inner)
    }
}

impl<'a> From<&'a str> for PropertyPath<'a> {
    fn from(path: &'a str) -> Self {
        Self(Cow::Borrowed(path))
    }
}

impl Debug for PropertyPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_ref())
    }
}

impl Display for PropertyPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_ref())
    }
}

impl<'a> AsRef<PropertyPath<'a>> for PropertyPath<'a> {
    fn as_ref(&self) -> &PropertyPath<'a> {
        self
    }
}

/// Path for channels where only channels are allowed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelPath<'a>(PropertyPath<'a>);

impl<'a> ChannelPath<'a> {
    /// When we can't pin down a single channel.
    pub const UNSPECIFIED: Self = Self(PropertyPath::UNSPECIFIED);

    /// Get the path in the internal format.
    pub fn path(&self) -> ObjectPath {
        self.0.path()
    }

    /// Produce an statically allocated version of the channel path.
    pub fn new<'b>(group: &'b str, channel: &'b str) -> Self {
        Self(PropertyPath::channel(group, channel))
    }

    pub fn to_static(&self) -> ChannelPath<'static> {
        ChannelPath(self.0.to_static())
    }
}

impl<'a> AsRef<ChannelPath<'a>> for ChannelPath<'a> {
    fn as_ref(&self) -> &ChannelPath<'a> {
        self
    }
}

impl<'a> AsRef<PropertyPath<'a>> for ChannelPath<'a> {
    fn as_ref(&self) -> &PropertyPath<'a> {
        &self.0
    }
}

impl std::fmt::Display for ChannelPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", PropertyPath::path(&self.0))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_root_path() {
        let path = PropertyPath::FILE;
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
    fn test_from_full_str() {
        let path_str = "/'group'/'channel'";
        let path: PropertyPath = path_str.into();
        assert_eq!(path.path(), path_str);
    }
}
