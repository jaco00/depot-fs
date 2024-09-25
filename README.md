# Depot File System

Depot File System is a Go-based project that adopts a block management mechanism inspired by ext3. 
It organizes blocks in a tree-like structure to efficiently manage file data, similar to how ext3 handles block storage. 
With its efficient block and file operations management, Depot File System offers a robust foundation for developing large-value key-value storage system or 
desktop applications for file management similar to ZIP. 

## Features

- **Block Management**: Supports allocation and deallocation of blocks.
- **File Operations**: Simulates file creation, deletion, and modification.
- **Unique ID Assignment:** Each file is assigned a unique ID, ensuring easy identification and management within the storage system.
- **Bitmap Handling**: Efficiently manages bitmaps for block allocation.
- **File-Based Storage**: Uses a file as the backend storage.
- **Metadata Storage**: Supports storing and managing file metadata, which allows for advanced file attributes and efficient file system operations.
- **BigAlloc for Performance**: Implements `Big Alloc`, a performance optimization mechanism that handles large contiguous block allocations, reducing fragmentation and improving I/O performance.
- **Extensibility**: Ideal for developing KV storage systems or desktop tools such as zip.

## Installation

Clone the repository and build the project:

```bash
git clone https://github.com/jaco00/depot-fs
cd depot-fs
go build
```

## Testing
Run the unit tests using:
```bash
go test ./...
```

## API Reference

### `MakeFileSystem`
```go
func MakeFileSystem(groupNum, blocksInGroup uint32, 
        root, pattern, tpl string, shardId uint16, enableBigAlloc bool) (*FileSystem, error)
 ```
#### Description

The `MakeFileSystem` function initializes and creates a new file system instance. It sets up the underlying structure based on the specified parameters, allowing for efficient file management and storage operations.

#### Parameters

- **groupNum** (uint32): The number of data files in the file system. Suggested values are between 16 and 256
- **blocksInGroup** (uint32): The number of blocks per allocation group. Use 0 for default value (1M).
- **root** (string): The root directory for the data file.
- **pattern** (string): A regular expression used to identify data files during initialization. It can be left empty to use the default value. 
- **tpl** (string): A template string for generating underlying data file names. It can be left empty to use the default value. 
- **shardId** (uint16): Used in distributed systems as part of the unique ID generation for files. 
- **enableBigAlloc** (bool): A flag indicating whether to enable large allocation for improved performance.

#### Returns
- ***FileSystem**, A pointer to the newly created `FileSystem` instance
- **error**: An error if the creation fails. If successful, the file system is ready for use.

### `CreateFile`
```go
func (fs *FileSystem) CreateFile(name string, meta []byte) (*Vfile, string, error)
```
#### Description
The `CreateFile` method creates a new file within the depot file system. The file is initialized with a name and associated metadata.The encoded length of both the name and the meta must be less than the size of a block in the depot file system. The function also generates a unique ID for the file, which can be used for future references or operations on the file.
#### Parameters
- name (string): The name of the file to be created.
- meta ([]byte): Metadata associated with the file, which can be used to store additional file attributes.
#### Returns
- ***Vfile**: A pointer to the newly created Vfile structure representing the file.
- **string**: A unique identifier (ID) for the file, which ensures the file can be uniquely referenced within the file system.
- **error**: An error message if the file creation fails. If successful, this will be nil.

### `DeleteFile`
```go
func (fs *FileSystem) DeleteFile(uid string) error 
```
#### Description
The `DeleteFile` method deletes a file from the file system based on its unique identifier (UID). This ensures that the correct file is removed, even if multiple files share the same name. The method only requires the unique file ID to identify and delete the file.
#### Parameters
- **uid** (string): The unique identifier of the file to be deleted. This UID is generated when the file is created.
#### Returns
- **error**: Returns an error if the deletion fails (e.g., if the file does not exist or there is a system error). If the deletion is successful, the error will be nil.

### `OpenFile`
```go
func (fs *FileSystem) OpenFile(uid string) (*Vfile, error)
```
#### Description
The `OpenFile` method opens a file in the file system based on its unique identifier (UID). This allows users to access the file's contents and metadata. By using the unique file ID, the method ensures that the correct file is accessed, even if there are files with the same name in the system.
#### Parameters
- **uid** (string): The unique identifier of the file to be opened. This UID is assigned when the file is created.
Returns
#### Returns
- ***Vfile**: A pointer to the Vfile structure, representing the opened file. This includes access to the file's contents and metadata.
- **error**: Returns an error if the file cannot be opened (e.g., if the file does not exist or is inaccessible). If successful, the error will be nil.

### `Read`
```go
func (vf *Vfile) Read(data []byte) (int, error)
```
#### Description
The `Read` method reads data from an open Vfile into the provided byte slice. It reads up to the length of the provided slice and returns the number of bytes actually read. This function can be used to sequentially read through the contents of the file.
#### Parameters
- **data** ([]byte): A byte slice that will be filled with data read from the file. The size of the slice determines the maximum amount of data to be read in a single call.
#### Returns
- **int**: The number of bytes successfully read into the provided data slice.
- **error**: An error if the read operation fails (e.g., due to an I/O error or if the end of the file is reached). If the read is successful, the error will be nil.

### `Write`
```go
func (vf *Vfile) Write(data []byte) (int, error) {
```
#### Description
The Write method writes the provided byte slice to the open Vfile. It writes as many bytes as are available in the slice and returns the number of bytes written. This function can be used to append or overwrite data in the file.
#### Parameters
- **data** ([]byte): The byte slice containing the data to be written to the file.
#### Returns
- **int**: The number of bytes successfully written to the file.
- **error**: An error if the write operation fails (e.g., due to an I/O error or insufficient space). If the write is successful, the error will be nil.

### `SeekPos`
```go
func (vf *Vfile) SeekPos(pos int64) (VfileOffset, error)
```
#### Description
The SeekPos method sets the current position of the file pointer to the specified offset within the file. This allows for random access to different parts of the file, enabling read and write operations from the desired position.
For better performance when frequently seeking, it is recommended to use the `GetOffset` method to retrieve the actual address of the offset after seeking. Note that after calling GetOffset, you should use the `Seek` method to set the file pointer to the actual position.
#### Parameters
- **pos** (int64): The new position (offset) to seek to within the file. This is an absolute position from the beginning of the file.
#### Returns
- **VfileOffset**: The new position of the file after seeking.
- **error**: Any error that occurred during the seek operation. If successful, error will be nil.

## Future Work
### Journaling for Data Consistency
To improve data consistency, especially in the event of unexpected power loss or system crashes, implementing a journaling mechanism is a key next step. Journaling will ensure that all file system operations are logged before they are committed to disk, which helps in recovering from partial writes or corruptions by replaying or rolling back changes. This is crucial for ensuring data integrity in production environments.

## License
This project is licensed under the GNU General Public License v3.0. See the LICENSE file for more details.
