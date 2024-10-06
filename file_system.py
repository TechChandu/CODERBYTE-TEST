# Copyright Exafunction, Inc.

"""Interface for a simplified file system."""

import abc
import dataclasses
import enum
from typing import Callable, Iterable


class FileSystemEventType(enum.Enum):
    """Event types for file system events."""

    FILE_OR_SUBDIR_ADDED = 1
    FILE_OR_SUBDIR_REMOVED = 2
    FILE_MODIFIED = 3


@dataclasses.dataclass
class FileSystemEvent:
    """File system events."""

    path: str
    event_type: FileSystemEventType


class FileSystem(abc.ABC):
    """Class representing a simple file system."""

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """Returns whether the path exists."""

    @abc.abstractmethod
    def isfile(self, path: str) -> bool:
        """Returns whether the path is a file."""

    @abc.abstractmethod
    def readfile(self, path: str) -> str:
        """Returns the content of the file at the given path."""

    @abc.abstractmethod
    def writefile(self, path: str, content: str):
        """Writes the given content to the file at the given path.

        This will create the file if it does not exist or overwrite it if it does.
        """

    @abc.abstractmethod
    def removefile(self, path: str):
        """Removes the file at the given path."""

    @abc.abstractmethod
    def isdir(self, path: str) -> bool:
        """Returns whether the path is a directory."""

    @abc.abstractmethod
    def listdir(self, path: str) -> Iterable[str]:
        """Returns the names of children of the directory at the given path.

        Note that the returned names are the base names, not the full paths.
        """

    @abc.abstractmethod
    def makedir(self, path: str):
        """Creates a new directory at the given path.

        Does nothing if the directory already exists.
        """

    @abc.abstractmethod
    def makedirs(self, path: str):
        """Creates a new directory at the given path.

        This will create all necessary parent directories.
        """

    @abc.abstractmethod
    def removedir(self, path: str):
        """Removes the directory at the given path.

        This will also recursively remove all contained files and directories.
        """

    @abc.abstractmethod
    def watchdir(self, path: str, callback: Callable[[FileSystemEvent], None]):
        """Registers a callback for changes to the directory at the given path.

        The callback will be triggered when:
        - An immediate child file or subdirectory is added.
        - An immediate child file or subdirectory is removed.
        - An immmediate child file is modified.
        """

    @abc.abstractmethod
    def unwatchdir(self, path: str):
        """Unregisters the callback for changes to the directory at the given path."""

    @abc.abstractmethod
    def debug_string(self, path: str) -> str:
        """Returns a string representation of the tree rooted at the given path."""