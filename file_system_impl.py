# Copyright Exafunction, Inc.

"""In-memory mock file system library."""

import collections
import dataclasses
import functools
import posixpath
from typing import Callable, Dict, Iterable, List, Set, Union

from file_system import FileSystem
from file_system import FileSystemEvent


class _NotFoundException(Exception):
    def __init__(self, path: str):
        super().__init__(f"File or directory not found: {path}")


class _IsDirectoryException(Exception):
    def __init__(self, path: str):
        super().__init__(f"{path} is a directory")


class _IsFileException(Exception):
    def __init__(self, path: str):
        super().__init__(f"{path} is a file")


@dataclasses.dataclass
class _Directory:
    children: Set[str] = dataclasses.field(default_factory=set)


@dataclasses.dataclass
class _File:
    content: str


_FileSystemObj = Union[_Directory, _File]


# pylint: disable=protected-access
def _count_operation(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        self._operation_counts[f.__name__] += 1
        return f(self, *args, **kwargs)

    return wrapper


def _normpath(f):
    @functools.wraps(f)
    def wrapper(self, path, *args, **kwargs):
        return f(self, posixpath.normpath(path), *args, **kwargs)

    return wrapper


class FileSystemImpl(FileSystem):
    """Class representing a simple file system."""

    def __init__(self):
        self._objs: Dict[str, _FileSystemObj] = {"/": _Directory()}
        self._watch_map: Dict[str, Callable[[FileSystemEvent], None]] = {}
        self._operation_counts: Dict[str, int] = collections.defaultdict(int)

    @_normpath
    @_count_operation
    def exists(self, path: str) -> bool:
        """Returns whether the path exists."""
        return path in self._objs

    @_normpath
    @_count_operation
    def isfile(self, path: str) -> bool:
        """Returns whether the path is a file."""
        if path not in self._objs:
            raise _NotFoundException(path)
        return isinstance(self._objs[path], _File)

    @_normpath
    @_count_operation
    def readfile(self, path: str) -> str:
        """Returns the content of the file at the given path."""
        if path not in self._objs:
            raise _NotFoundException(path)
        obj = self._objs.get(path)
        if isinstance(obj, _Directory):
            raise _IsDirectoryException(path)
        return obj.content

    @_normpath
    @_count_operation
    def writefile(self, path: str, content: str):
        """Writes the given content to the file at the given path.

        This will create the file if it does not exist or overwrite it if it does.
        """
        parent_dir = posixpath.dirname(path)
        if parent_dir not in self._objs:
            raise _NotFoundException(parent_dir)
        if isinstance(self._objs[parent_dir], _File):
            raise _IsFileException(parent_dir)
        if path in self._objs and isinstance(self._objs[path], _Directory):
            raise _IsDirectoryException(path)
        filename = posixpath.basename(path)
        self._objs[parent_dir].children.add(filename)
        self._objs[path] = _File(content)

    @_normpath
    @_count_operation
    def removefile(self, path: str):
        """Removes the file at the given path."""
        if path not in self._objs:
            raise _NotFoundException(path)
        if isinstance(self._objs[path], _Directory):
            raise _IsDirectoryException(path)
        parent_dir = posixpath.dirname(path)
        filename = posixpath.basename(path)
        self._objs[parent_dir].children.remove(filename)
        del self._objs[path]

    @_normpath
    @_count_operation
    def isdir(self, path: str) -> bool:
        """Returns whether the path is a directory."""
        if path not in self._objs:
            raise _NotFoundException(path)
        return isinstance(self._objs[path], _Directory)

    @_normpath
    @_count_operation
    def listdir(self, path: str) -> Iterable[str]:
        """Returns the names of children of the directory at the given path.

        Note that the returned names are the base names, not the full paths.
        """
        if path not in self._objs:
            raise _NotFoundException(path)
        if isinstance(self._objs[path], _File):
            raise _IsFileException(path)
        return self._objs[path].children.copy()

    @_normpath
    @_count_operation
    def makedir(self, path: str):
        """Creates a new directory at the given path.

        Does nothing if the directory already exists.
        """
        parent_dir = posixpath.dirname(path)
        if parent_dir not in self._objs:
            raise _NotFoundException(parent_dir)
        if isinstance(self._objs[parent_dir], _File):
            raise _IsFileException(parent_dir)
        if path in self._objs and isinstance(self._objs[path], _File):
            raise _IsFileException(path)
        if path in self._objs and isinstance(self._objs[path], _Directory):
            return
        dirname = posixpath.basename(path)
        self._objs[parent_dir].children.add(dirname)
        self._objs[path] = _Directory()

    @_normpath
    @_count_operation
    def makedirs(self, path: str):
        """Creates a new directory at the given path.

        This will create all necessary parent directories.
        """
        parent_dir = posixpath.dirname(path)
        if parent_dir != "/":
            self.makedirs(parent_dir)
        self.makedir(path)

    @_normpath
    @_count_operation
    def removedir(self, path: str):
        """Removes the directory at the given path.

        This will also recursively remove all contained files and directories.
        """
        if path not in self._objs:
            raise _NotFoundException(path)
        if isinstance(self._objs[path], _File):
            raise _IsFileException(path)
        for child in self.listdir(path):
            child_path = posixpath.join(path, child)
            if isinstance(self._objs[child_path], _File):
                self.removefile(child_path)
            else:
                self.removedir(child_path)
        parent_dir = posixpath.dirname(path)
        dir_name = posixpath.basename(path)
        self._objs[parent_dir].children.remove(dir_name)
        del self._objs[path]

    @_normpath
    def watchdir(self, path: str, callback: Callable[[FileSystemEvent], None]):
        """Registers a callback for changes to the directory at the given path.

        The callback will be triggered when:
        - An immediate child file or subdirectory is added.
        - An immediate child file or subdirectory is removed.
        - An immmediate child file is modified.
        """
        self._watch_map[path] = callback

    @_normpath
    def unwatchdir(self, path: str):
        """Unregisters the callback for changes to the directory at the given path."""
        if path not in self._watch_map:
            raise _NotFoundException(path)
        del self._watch_map[path]

    def num_watched_dirs(self) -> int:
        """Returns the number of registered watch callbacks."""
        return len(self._watch_map)

    def get_num_operations(self, operation_name: str) -> int:
        """Returns the number of times the given operation has been called."""
        return self._operation_counts[operation_name]

    def get_dir_objs(self, path: str) -> Dict[str, _FileSystemObj]:
        """Get a map from relative path to file system objects within a directory.

        The relative path will be relative to the given directory path.
        """
        return {
            posixpath.relpath(obj_path, path): obj
            for obj_path, obj in self._objs.items()
            if obj_path.startswith(path)
        }

    def handle_event(self, event: FileSystemEvent):
        """Trigger watch callback for the given event.

        Does nothing if there is no watch callback for the parent directory.
        """
        parent_dir = posixpath.dirname(event.path)
        if parent_dir in self._watch_map:
            self._watch_map[parent_dir](event)

    def debug_string(self, path: str) -> str:
        """Returns a string representation of the path in the file system."""
        if path not in self._objs:
            raise _NotFoundException(path)

        def helper(_path: str) -> List[str]:
            if _path not in self._objs:
                raise _NotFoundException(_path)
            basename = posixpath.basename(_path)
            if isinstance(self._objs[_path], _File):
                return [f"{basename}: {self._objs[_path].content}"]
            lines = [f"{path}" if _path == path else f"/{basename}"]
            children = self._objs[_path].children
            for child_idx, child in enumerate(sorted(children)):
                child_lines = helper(posixpath.join(_path, child))
                for child_line_idx, child_line in enumerate(child_lines):
                    prefix = ""
                    if child_idx == len(children) - 1 and child_line_idx == 0:
                        prefix += "`"
                    elif child_idx != len(children) - 1:
                        prefix += "|"
                    else:
                        prefix += " "
                    if child_line_idx == 0:
                        prefix += "--"
                    else:
                        prefix += "  "
                    lines.append(f"{prefix} {child_line}")
            return lines

        return "\n".join(helper(path))

    def __str__(self):
        return str(dict(sorted(self._objs.items())))

    def __reduce__(self):
        raise TypeError("FileSystemImpl cannot be pickled")