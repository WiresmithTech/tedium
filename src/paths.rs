//! Module to specify object paths in user friendly manners.

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
};

/// A path to a location in a tdms file.
///
/// It is recommended to use the static methods to create paths.
/// These will format the path correctly and turn it into a owned string
/// (i.e. it does allocation)
///
/// ```rust
/// use tdms_lib::ObjectPath;
///
/// let path = ObjectPath::channel("group", "channel");
/// assert_eq!(path.path(), "/'group'/'channel'");
/// ```
///
/// If you want to avoid allocation, you can use the `From<&str>` implementation
/// but you must use the format expected internally in the file.
///
///
/// ```rust
/// use tdms_lib::ObjectPath;
///
/// let path_str = "/'group'/'channel'";
/// let path: ObjectPath = path_str.into();
/// assert_eq!(path.path(), path_str);
/// ```
///
/// The FILE path is a special case and is used to specify the root of the file.
///
/// ```rust
/// use tdms_lib::ObjectPath;
///
/// let path = ObjectPath::FILE;
/// assert_eq!(path.path(), "/");
/// ```
pub struct ObjectPath<'a>(Cow<'a, str>);

impl<'a> ObjectPath<'a> {
    /// Specify the root of the file.
    pub const FILE: Self = Self(Cow::Borrowed("/"));

    /// Used internally to track a location where I can't tie it to a specific path.
    pub(crate) const UNSPECIFIED: Self = Self(Cow::Borrowed("Unspecified"));

    /// Generate a path to a group.
    pub fn group(group: &'a str) -> Self {
        Self(Cow::Owned(format!("/'{}'", group)))
    }

    /// Generate a path to a channel.
    pub fn channel(group: &'a str, channel: &'a str) -> Self {
        Self(Cow::Owned(format!("/'{}'/'{}'", group, channel)))
    }

    /// Get the path in the internal format.
    pub fn path(&self) -> &str {
        self.0.as_ref()
    }

    /// Produce an owned, statically allocated version of the path
    /// where ownership is required.
    pub(crate) fn to_static(&self) -> ObjectPath<'static> {
        let path = self.0.to_string();
        let inner: Cow<'static, str> = Cow::Owned(path);
        ObjectPath(inner)
    }
}

impl<'a> From<&'a str> for ObjectPath<'a> {
    fn from(path: &'a str) -> Self {
        Self(Cow::Borrowed(path))
    }
}

impl Debug for ObjectPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_ref())
    }
}

impl Display for ObjectPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_ref())
    }
}

impl<'a> AsRef<ObjectPath<'a>> for ObjectPath<'a> {
    fn as_ref(&self) -> &ObjectPath<'a> {
        self
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_root_path() {
        let path = ObjectPath::FILE;
        assert_eq!(path.path(), "/");
    }

    #[test]
    fn test_group_path() {
        let path = ObjectPath::group("group");
        assert_eq!(path.path(), "/'group'");
    }

    #[test]
    fn test_channel_path() {
        let path = ObjectPath::channel("group", "channel");
        assert_eq!(path.path(), "/'group'/'channel'");
    }

    #[test]
    fn test_from_full_str() {
        let path_str = "/'group'/'channel'";
        let path: ObjectPath = path_str.into();
        assert_eq!(path.path(), path_str);
    }
}
