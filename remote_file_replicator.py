import posixpath
from typing import Any, Callable

from file_system import FileSystem, FileSystemEvent, FileSystemEventType

# If you're completing this task in an online assessment, you can increment this
# constant to enable more unit tests relevant to the task you are on (1-5).
TASK_NUM = 5

class ReplicatorSource:
    """Class representing the source side of a file replicator."""

    def __init__(self, fs: FileSystem, dir_path: str, rpc_handle: Callable[[Any], Any]):
        self._fs = fs
        self._dir_path = dir_path
        self._rpc_handle = rpc_handle

        # Start watching the directory for changes
        self._fs.watchdir(dir_path, self.handle_event)
        self.addwatchdir(self._fs,dir_path)
        self.sendChildEvents(self._fs,dir_path, 'INIT')
        self._rpc_handle({'event_type': 'DELETE', 'relative_path': ''})
    
    def addwatchdir(self,fs: FileSystem, dir_path: str):
        for child_name in fs.listdir(dir_path):
            child_source_path = posixpath.join(dir_path, child_name)
            if fs.isdir(child_source_path):
                self._fs.watchdir(child_source_path, self.handle_event)
                self.addwatchdir(fs, child_source_path)

    def unwatch(self, dir_path: str):
        for key in list(self._fs._watch_map.keys()):
            if key == dir_path or  key.startswith(dir_path):
                self._fs.unwatchdir(key)
                
    def sendChildEvents(self, fs: FileSystem, dir_path: str, event_type):
        for child_name in fs.listdir(dir_path):
            child_source_path = posixpath.join(dir_path, child_name)
            if fs.isdir(child_source_path):
                request = {
                    'event_type': event_type,
                    'relative_path':  posixpath.relpath(child_source_path, self._dir_path),
                    'is_dir': True,
                }
                self._rpc_handle(request)
                self.sendChildEvents(fs, child_source_path,event_type)
            else:
                request = {
                    'event_type': event_type,
                    'relative_path':  posixpath.relpath(child_source_path, self._dir_path),
                    'is_dir': False,
                    'file_content': self._fs.readfile(child_source_path)
                }
                self._rpc_handle(request)
            
            


            

    def handle_event(self, event: FileSystemEvent):
        """Handle a file system event.

        Used as the callback provided to FileSystem.watchdir().
        """
        #print(event)
        if event.event_type == FileSystemEventType.FILE_OR_SUBDIR_REMOVED:
            try:
                self.unwatch(event.path)
            except Exception:
                print('not a directory')
            finally:
                request = {
                    'event_type': event.event_type,
                    'relative_path':  posixpath.relpath(event.path, self._dir_path)
                }
        elif  event.event_type == FileSystemEventType.FILE_OR_SUBDIR_ADDED:
            if self._fs.isdir(event.path):
                self._fs.watchdir(event.path, self.handle_event)
                self.addwatchdir(self._fs,event.path)
                self.sendChildEvents(self._fs, event.path, event.event_type)
                request = {
                    'event_type': event.event_type,
                    'relative_path':  posixpath.relpath(event.path, self._dir_path),
                    'is_dir': True
                }
            else :
                request = {
                    'event_type': event.event_type,
                    'relative_path':  posixpath.relpath(event.path, self._dir_path),
                    'is_dir': False,
                    'file_content': self._fs.readfile(event.path)
                }
        else:
            request = {
                'event_type': event.event_type,
                'relative_path':  posixpath.relpath(event.path, self._dir_path),
                'is_dir': False,
                'file_content': self._fs.readfile(event.path)
            }
        # Send the request to the target through the RPC handle
        self._rpc_handle(request)

class ReplicatorTarget:
    """Class representing the target side of a file replicator."""

    def __init__(self, fs: FileSystem, dir_path: str):
        self._fs = fs
        self._dir_path = dir_path
        self._file_paths = []
        self._dir_paths = []
        self.dir_file_paths(fs, dir_path)
    
    def dir_file_paths(self, fs: FileSystem, dir_path: str):
        for child_name in fs.listdir(dir_path):
            child_source_path = posixpath.join(dir_path, child_name)
            if fs.isdir(child_source_path):
                self._dir_paths.append(child_source_path)
                self.dir_file_paths(fs, child_source_path)
            else:
                self._file_paths.append(child_source_path)

    def delete_internal(self, fs: FileSystem, directory_path: str):
        for filename in fs.listdir(directory_path):
            file_path = posixpath.join(directory_path, filename)
            if fs.isfile(file_path):
                if file_path in self._file_paths:
                    fs.removefile(file_path)
            else:
                self.delete_internal(self._fs, file_path)
                if file_path in self._dir_paths:
                    fs.removedir(file_path)

    def handle_request(self, request: Any) -> Any:
        """Handle a request from the ReplicatorSource."""

        event_type = request['event_type']
        relative_path = request['relative_path']

        if event_type == FileSystemEventType.FILE_OR_SUBDIR_ADDED:
            target_path = posixpath.join(self._dir_path, relative_path)
            if request['is_dir']:
                self._fs.makedirs(target_path)
            else:
                self._fs.writefile(target_path, request['file_content'])

        elif event_type == FileSystemEventType.FILE_OR_SUBDIR_REMOVED:
            target_path = posixpath.join(self._dir_path, relative_path)
            if self._fs.exists(target_path):
                if self._fs.isdir(target_path):
                    self._fs.removedir(target_path)
                else:
                    self._fs.removefile(target_path)

        elif event_type == FileSystemEventType.FILE_MODIFIED:
            target_path = posixpath.join(self._dir_path, relative_path)
            self._fs.writefile(target_path, request['file_content'])
        
        elif event_type == 'INIT':
            target_path = posixpath.join(self._dir_path, relative_path)
            if not self._fs.exists(target_path):
                if request['is_dir']:
                    self._fs.makedirs(target_path)
                else:
                    self._fs.writefile(target_path, request['file_content'])

            else:
                if self._fs.isdir(target_path):
                    if(request['is_dir']):
                        self._dir_paths.remove(target_path)
                    else:
                        self._dir_paths.remove(target_path)
                        self._fs.removedir(target_path)
                        self._fs.writefile(target_path, request['file_content'])
                else:
                    if(request['is_dir']):
                        self._file_paths.remove(target_path)
                        self._fs.removefile(target_path)
                        self._fs.makedir(target_path)
                    else:
                        self._file_paths.remove(target_path)
                        if(request['file_content'] != self._fs.readfile(target_path)):
                            self._fs.writefile(target_path, request['file_content'])
                           
        elif event_type == 'DELETE':
            self.delete_internal(self._fs, self._dir_path)
        return {'status': 'success'}