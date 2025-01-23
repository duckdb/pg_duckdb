#pragma once

#include "duckdb/main/client_data.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

class CachedFileHandle;

class LocalCacheFileSystem: public LocalFileSystem {

public:
	LocalCacheFileSystem(const std::string &_cache_file_directory) : cache_file_directory(_cache_file_directory) {
		// TODO: Should we forbid symlinks for cache_file_directory? Or have a more complex check?
	}

	std::string GetName() const override {
		return "LocalCacheFileSystem";
	}

	unique_ptr<FileHandle> OpenFile(const string &file_name, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		if (file_name.find("..") != string::npos || file_name.find("/") != string::npos) {
			throw PermissionException("Must provide a file name, not a path. Got: '", file_name, "'");

		}

		std::ostringstream oss;
		oss << cache_file_directory << "/" << file_name;
		return LocalFileSystem::OpenFile(oss.str(), flags, opener);
	}

	void AssertSameCacheDir(const string &other_dir) {
		if (other_dir != cache_file_directory) {
			throw PermissionException("BUG: expected cache directory to be '", cache_file_directory, "' but got '", other_dir, "'");
		}
	}

#define LOCAL_FILE_SYSTEM_METHOD(method_name) \
	method_name(const string &path, optional_ptr<FileOpener> opener = nullptr) override { \
		throw PermissionException("LocalCacheFileSystem cannot run " #method_name " on '", path, "'"); \
	}

	LOCAL_FILE_SYSTEM_METHOD(bool DirectoryExists)
	LOCAL_FILE_SYSTEM_METHOD(void CreateDirectory)
	LOCAL_FILE_SYSTEM_METHOD(void RemoveDirectory)
	LOCAL_FILE_SYSTEM_METHOD(bool FileExists)
	LOCAL_FILE_SYSTEM_METHOD(bool IsPipe)
	LOCAL_FILE_SYSTEM_METHOD(void RemoveFile)

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &,
	               FileOpener *opener = nullptr) override {
		throw PermissionException("LocalCacheFileSystem cannot run ListFiles on '", directory, "'");
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		throw PermissionException("LocalCacheFileSystem cannot run MoveFile on '", source, "' to '", target, "'");
	}

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		throw PermissionException("LocalCacheFileSystem cannot run Glob on '", path, "'");
	}

	bool CanHandleFile(const string &fpath) override {
		throw PermissionException("LocalCacheFileSystem cannot run CanHandleFile on '", fpath, "'");
	}

	static bool IsPrivateFile(const string &path_p, FileOpener *opener) {
		throw PermissionException("LocalCacheFileSystem cannot run IsPrivateFile on '", path_p, "'");
	}
private:
	std::string cache_file_directory;
};


//! Represents a file that is intended to be fully downloaded, then used in parallel by multiple threads
class CachedFile : public enable_shared_from_this<CachedFile> {
	friend class CachedFileHandle;

public:
	CachedFile(LocalCacheFileSystem &fs, const std::string &key, bool cache_file);

	unique_ptr<CachedFileHandle> GetHandle() {
		auto this_ptr = shared_from_this();
		return make_uniq<CachedFileHandle>(this_ptr);
	}

	bool Initialized() {
		return initialized;
	}

private:
	void GetDirectoryCacheLock();
	void ReleaseDirectoryCacheLock();

	// FileSystem
	LocalCacheFileSystem &fs;
	// File name
	std::string file_name;
	// Cache file FileDescriptor
	unique_ptr<FileHandle> handle;
	// Lock file
	unique_ptr<FileHandle> directory_lock_handle;
	//! Data capacity
	uint64_t capacity = 0;
	//! Size of file
	idx_t size;
	//! When initialized is set to true, the file is cached and safe for parallel reading without holding the lock
	atomic<bool> initialized = {false};
};

//! Handle to a CachedFile
class CachedFileHandle {
public:
	explicit CachedFileHandle(shared_ptr<CachedFile> &file_p);
	//! Allocate file size
	void Allocate(idx_t size);
	//! Grow file to new size, copying over `bytes_to_copy` to the new buffer
	void GrowFile(idx_t new_capacity, idx_t bytes_to_copy);
	//! Write cached file metadata
	void WriteMetadata(const string &cache_key, const string &remote_path, idx_t total_size);
	//! Indicate the file is fully downloaded and safe for parallel reading without lock
	void SetInitialized(idx_t total_size);
	//! Write to the buffer
	void Write(const char *buffer, idx_t length, idx_t offset = 0);
	//! Read data to buffer
	void Read(void *buffer, idx_t length, idx_t offset);

	bool Initialized() {
		return file->initialized;
	}
	uint64_t GetCapacity() {
		return file->capacity;
	}
	//! Return the size of the initialized file
	idx_t GetSize() {
		D_ASSERT(file->initialized);
		return file->size;
	}

private:
	shared_ptr<CachedFile> file;
};

class HTTPFileCache : public ClientContextState {
public:
	explicit HTTPFileCache(ClientContext &context) {
		db = context.db;
	}

	//! Get cache, create if not exists only if caching is enabled
	shared_ptr<CachedFile> GetCachedFile(const string &cache_dir, const string &key, bool create_cache);

private:
	LocalCacheFileSystem& GetFS(const string &cache_dir);

	unique_ptr<LocalCacheFileSystem> cached_fs;

	//! Database Instance
	shared_ptr<DatabaseInstance> db;

	//! Mutex to lock when getting the cached fs (Parallel Only)
	mutex cached_fs_mutex;
	//! Mutex to lock when getting the cached file (Parallel Only)
	mutex cached_files_mutex;
	//! In case of fully downloading the file, the cached files of this query
	unordered_map<string, shared_ptr<CachedFile>> cached_files;
};

} // namespace duckdb
