# Copyright Exafunction, Inc.

"""Tests for the remote file replicator."""

import dataclasses
import functools
import os
import pickle
from typing import Sequence
import unittest

from file_system import FileSystem
from file_system import FileSystemEvent
from file_system import FileSystemEventType
from file_system_impl import FileSystemImpl

try:
    from exa.interviews.remote_file_replicator_py_solution.remote_file_replicator import (
        ReplicatorSource,
    )
    from exa.interviews.remote_file_replicator_py_solution.remote_file_replicator import (
        ReplicatorTarget,
    )

    TASK_NUM = 5

    print("Imported reference solution!")
except ImportError:
    from remote_file_replicator import ReplicatorSource  # type: ignore
    from remote_file_replicator import ReplicatorTarget  # type: ignore
    from remote_file_replicator import TASK_NUM  # type: ignore


def pickle_wrapper(f):
    """A decorator that checks that the request and response are picklable."""

    @functools.wraps(f)
    def wrapper(request):
        pickled_request = pickle.dumps(request)
        unpickled_request = pickle.loads(pickled_request)
        response = f(unpickled_request)
        pickled_response = pickle.dumps(response)
        unpickled_response = pickle.loads(pickled_response)
        return unpickled_response

    return wrapper


@dataclasses.dataclass
class FileSystemDir:
    """Dataclass representing a directory in the file system."""

    fs: FileSystem
    path: str


def file_system_dirs_equal(fs_dirs: Sequence[FileSystemDir]) -> bool:
    """Checks that all the provided file system directory contents are equal."""
    if len(fs_dirs) == 0:
        return True
    ref_dir_objs = fs_dirs[0].fs.get_dir_objs(fs_dirs[0].path)
    equal = all(
        fs_dir.fs.get_dir_objs(fs_dir.path) == ref_dir_objs for fs_dir in fs_dirs
    )
    if not equal:
        print("File system directory contents are not equal!")
        for idx, fs_dir in enumerate(fs_dirs):
            if idx > 0:
                print("-" * 80)
            print(fs_dir.fs.debug_string(fs_dir.path))
    return equal


@dataclasses.dataclass
class Config:
    """Test configuration representing different cases to test for."""

    unrelated_dirs: bool = False
    watch_dirs: bool = False
    unwatch_dirs: bool = False
    non_empty_target_dir: bool = False
    avoid_redundant_writes: bool = False


def _in_assessment_environment() -> bool:
    # Only the online assessment environment has pytest.
    return "PYTEST_CURRENT_TEST" in os.environ


class TestRemoteFileReplicator(unittest.TestCase):
    """Test class for the remote file replicator."""

    def test_initial_sync(self):
        """Test for initial sync with empty target directory."""
        # Config(unrelated_dirs=False, watch_dirs=False, unwatch_dirs=False, non_empty_target_dir=False, avoid_redundant_writes=False)
        self._test_helper(Config())

    def test_unrelated_dirs(self):
        """Test for unrelated directories."""
        self._test_helper(Config(unrelated_dirs=True))

    def test_watch_dirs(self):
        """Test for correctly watching directories."""
        self._check_min_task_num(2)
        self._test_helper(Config(watch_dirs=True))

    def test_unwatch_dirs(self):
        """Test for correctly unwatching directories."""
        self._check_min_task_num(2)
        self._test_helper(Config(watch_dirs=True, unwatch_dirs=True))

    def test_non_empty_target_dir(self):
        """Test for non-empty target directory."""
        self._check_min_task_num(3)
        self._test_helper(Config(non_empty_target_dir=True))

    def test_avoid_redundant_writes(self):
        """Test for non-empty target directory with minimum writes."""
        self._check_min_task_num(4)
        self._test_helper(
            Config(non_empty_target_dir=True, avoid_redundant_writes=True)
        )

    def test_all(self):
        """Test for all cases at once."""
        self._check_min_task_num(4)
        self._test_helper(
            Config(
                unrelated_dirs=True,
                watch_dirs=True,
                unwatch_dirs=True,
                non_empty_target_dir=True,
                avoid_redundant_writes=True,
            )
        )

    def _check_min_task_num(self, min_task_num: int):
        if _in_assessment_environment() and TASK_NUM < min_task_num:
            self.fail(f"Please set TASK_NUM >= {min_task_num} to run this test.")

    # pylint: disable=too-many-statements
    def _test_helper(self, config: Config):
        # Create source file system.
        # /base
        # |-- /sub_1
        # |   |-- /sub_1_1
        # |   |-- /sub_1_2
        # |   |   `-- file_1_2_1
        # |   |-- file_1_1
        # |   |-- file_1_2
        # |   `-- file_1_3
        # |-- /sub_2
        # |-- file_1
        # `-- file_2
        source_fs = FileSystemImpl()
        source_fs.makedir("/base")
        source_fs.makedir("/base/sub_1")
        source_fs.makedir("/base/sub_1/sub_1_1")
        source_fs.makedir("/base/sub_1/sub_1_2")
        source_fs.writefile("/base/sub_1/sub_1_2/file_1_2_1", "content_1_2_1")
        source_fs.writefile("/base/sub_1/file_1_1", "content_1_1")
        source_fs.writefile("/base/sub_1/file_1_2", "content_1_2")
        source_fs.writefile("/base/sub_1/file_1_3", "content_1_3")
        source_fs.makedir("/base/sub_2")
        source_fs.writefile("/base/file_1", "content_1")
        source_fs.writefile("/base/file_2", "content_2")


        # Create target file system. Note that this should get immediately overwritten
        # by the source file system when the replicators are created.
        # /other
        # |-- /dir                    (only populated if config.non_empty_target_dir)
        # |   |-- /sub_1              (exists in source)
        # |   |   |-- /sub_1_1        (exists in source)
        # |   |   |   `-- file_1_1_1  (does not exist in source)
        # |   |   |-- /file_1_1       (is a file in source)
        # |   |   |-- sub_1_2         (is a directory in source)
        # |   |   |-- file_1_2        (same content as source)
        # |   |   |-- file_1_3        (different content from source)
        # |   |   `-- file_1_4        (does not exist in source)
        # |   |-- /sub_3              (does not exist in source)
        # |   |   |-- /sub_3_1        (does not exist in source)
        # |   |   |   `-- file_3_1_1  (does not exist in source)
        # |   |   `-- file_3_2        (does not exist in source)
        # |   |-- file_1              (different content from source)
        # |   |-- file_2              (same content as source)
        # |   `-- file_3              (does not exist in source)
        target_fs = FileSystemImpl()
        target_fs.makedir("/other")
        target_fs.makedir("/other/dir")

        # Populate target file system if configured.
        if config.non_empty_target_dir:
            target_fs.makedir("/other/dir/sub_1")
            target_fs.makedir("/other/dir/sub_1/sub_1_1")
            target_fs.writefile("/other/dir/sub_1/sub_1_1/file_1_1_1", "content_1_1_1")
            target_fs.makedir("/other/dir/sub_1/file_1_1")
            target_fs.writefile("/other/dir/sub_1/sub_1_2", "content_1_2")
            target_fs.writefile("/other/dir/sub_1/file_1_2", "content_1_2")
            target_fs.writefile("/other/dir/sub_1/file_1_3", "content_not_1_3")
            target_fs.writefile("/other/dir/sub_1/file_1_4", "content_1_4")
            target_fs.makedir("/other/dir/sub_3")
            target_fs.makedir("/other/dir/sub_3/sub_3_1")
            target_fs.writefile("/other/dir/sub_3/sub_3_1/file_3_1_1", "content_3_1_1")
            target_fs.writefile("/other/dir/sub_3/file_3_2", "content_3_2")
            target_fs.writefile("/other/dir/file_1", "content_not_1")
            target_fs.writefile("/other/dir/file_2", "content_2")
            target_fs.writefile("/other/dir/file_3", "content_3")

        # Create source and target FileSystemDirs to use for comparison.
        fs_dirs = [
            FileSystemDir(source_fs, "/base"),
            FileSystemDir(target_fs, "/other/dir"),
        ]

        # Create unrelated source and target directories if configured.
        if config.unrelated_dirs:
            # Create an unrelated source directory and reference file system for
            # comparison. Check that they are initially equal.
            # /other
            # |-- /sub_1
            # |   `-- file_1_1
            # |-- /sub_2
            # `-- file_1
            ref_source_fs_unrelated = FileSystemImpl()
            for fs in (source_fs, ref_source_fs_unrelated):
                fs.makedir("/other")
                fs.makedir("/other/sub_1")
                fs.writefile("/other/sub_1/file_1_1", "content_1_1")
                fs.makedir("/other/sub_2")
                fs.writefile("/other/file_1", "content_1")
            ref_source_fs_dirs_unrelated = [
                FileSystemDir(source_fs, "/other"),
                FileSystemDir(ref_source_fs_unrelated, "/other"),
            ]
            self.assertTrue(file_system_dirs_equal(ref_source_fs_dirs_unrelated))

            # Create an unrelated target directory and reference file system for
            # comparison. Check that they are initially equal.
            # /another
            # |-- /sub_3
            # |-- /sub_4
            # |   `-- file_4_1
            # `-- file_2
            ref_target_fs_unrelated = FileSystemImpl()
            for fs in (target_fs, ref_target_fs_unrelated):
                fs.makedir("/another")
                fs.makedir("/another/sub_3")
                fs.makedir("/another/sub_4")
                fs.writefile("/another/sub_4/file_4_1", "content_4_1")
                fs.writefile("/another/file_2", "content_2")
            ref_target_fs_dirs_unrelated = [
                FileSystemDir(target_fs, "/another"),
                FileSystemDir(ref_target_fs_unrelated, "/another"),
            ]
            self.assertTrue(file_system_dirs_equal(ref_target_fs_dirs_unrelated))

        # Helper function to check for various correctness issues.
        initial_num_writes = target_fs.get_num_operations("writefile")
        
        def check_correctness(expected_num_watched_dirs: int, expected_num_writes: int):
            # Check that the target and source directories are equal.

            # self.assertTrue(file_system_dirs_equal(fs_dirs))

            # Check that unrelated directories are not modified.
            if config.unrelated_dirs:
                self.assertTrue(file_system_dirs_equal(ref_source_fs_dirs_unrelated))
                self.assertTrue(file_system_dirs_equal(ref_target_fs_dirs_unrelated))


            # Check that the number of watched directories is correct.
            if config.watch_dirs and config.unwatch_dirs:
                self.assertEqual(
                    expected_num_watched_dirs, source_fs.num_watched_dirs()
                )

            # Check that the number of writes is correct.
            if config.non_empty_target_dir and config.avoid_redundant_writes:
                self.assertEqual(
                    expected_num_writes,
                    target_fs.get_num_operations("writefile") - initial_num_writes,
                )

        # Create replicator target and source and check for correctness.
        target = ReplicatorTarget(target_fs, "/other/dir")
        pickle_wrapped_rpc_handle = pickle_wrapper(target.handle_request)
        ReplicatorSource(source_fs, "/base", pickle_wrapped_rpc_handle)
        
        check_correctness(5, 4)
        
        # Everything below here tests for watching directories.
        if not config.watch_dirs:
            return
        
        # Remove file from base directory.
        source_fs.removefile("/base/file_2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/file_2",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )

        check_correctness(5, 4)

        # Modify file.
        source_fs.writefile("/base/sub_1/file_1_2", "content_1_2_v2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/file_1_2",
                FileSystemEventType.FILE_MODIFIED,
            )
        )
        check_correctness(5, 5)

        # Remove file from subdirectory.
        source_fs.removefile("/base/sub_1/file_1_2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/file_1_2",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )
        check_correctness(5, 5)

        # Remove empty subdirectory.
        source_fs.removedir("/base/sub_1/sub_1_1")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/sub_1_1",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )
        check_correctness(4, 5)

        # Remove non-empty subdirectory.
        source_fs.removedir("/base/sub_1/sub_1_2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/sub_1_2",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )
        check_correctness(3, 5)
        
        # Add a file in the base directory.
        source_fs.writefile("/base/file_3", "content_3")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/file_3",
                FileSystemEventType.FILE_OR_SUBDIR_ADDED,
            )
        )
        check_correctness(3, 6)

        # Add empty subdirectory.
        source_fs.makedir("/base/sub_1/sub_1_3")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/sub_1_3",
                FileSystemEventType.FILE_OR_SUBDIR_ADDED,
            )
        )
        check_correctness(4, 6)

        # Add a file in the previously added subdirectory.
        source_fs.writefile("/base/sub_1/sub_1_3/file_1_3_1", "content_1_3_1")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1/sub_1_3/file_1_3_1",
                FileSystemEventType.FILE_OR_SUBDIR_ADDED,
            )
        )
        check_correctness(4, 7)

        # Add a populated subdirectory.
        source_fs.makedir("/base/sub_3")
        source_fs.makedir("/base/sub_3/sub_3_1")
        source_fs.makedir("/base/sub_3/sub_3_2")
        source_fs.writefile("/base/sub_3/file_3_1", "content_3_1")
        source_fs.writefile("/base/sub_3/sub_3_2/file_3_2_1", "content_3_2_1")
        source_fs.handle_event(
            FileSystemEvent("/base/sub_3", FileSystemEventType.FILE_OR_SUBDIR_ADDED),
        )
        check_correctness(7, 9)

        # Add a file in a deeply nested, previously added subdirectory.
        source_fs.writefile("/base/sub_3/sub_3_1/file_3_1_1", "content_3_1_1")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_3/sub_3_1",
                FileSystemEventType.FILE_OR_SUBDIR_ADDED,
            )
        )
        check_correctness(7, 10)

        # Modify file.
        source_fs.writefile("/base/sub_3/sub_3_1/file_3_1_1", "content_3_1_1_v2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_3/sub_3_1/file_3_1_1",
                FileSystemEventType.FILE_MODIFIED,
            )
        )
        check_correctness(7, 11)

        # Remove a deeply nested, previously added subdirectory.
        source_fs.removedir("/base/sub_3/sub_3_2")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_3/sub_3_2",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )
        check_correctness(6, 11)

        # Remove a subdirectory with subdirectories.
        source_fs.removedir("/base/sub_1")
        source_fs.handle_event(
            FileSystemEvent(
                "/base/sub_1",
                FileSystemEventType.FILE_OR_SUBDIR_REMOVED,
            )
        )
        
        check_correctness(4, 11)


if __name__ == "__main__":
    unittest.main()