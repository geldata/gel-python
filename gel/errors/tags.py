__all__ = [
    "Tag",
    "SHOULD_RECONNECT",
    "SHOULD_RETRY",
]


class Tag(object):
    """Error tag

    Tags are used to differentiate certain properties of errors that apply to
    error classes across hierarchy.

    Use ``error.has_tag(tag_name)`` to check for a tag.
    """

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<Tag {self.name}>"


SHOULD_RECONNECT = Tag("SHOULD_RECONNECT")
SHOULD_RETRY = Tag("SHOULD_RETRY")
