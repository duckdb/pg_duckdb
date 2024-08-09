#pragma once

#include "duckdb/main/client_data.hpp"

namespace duckdb {

class CachedFileHandle;

//! Represents a file that is intended to be fully downloaded, then used in parallel by multiple threads
class CachedFile : public enable_shared_from_this<CachedFile> {
	friend class CachedFileHandle;

public:
	CachedFile(const string &cache_dir, FileSystem &fs, const std::string &key, bool cache_file);
	~CachedFile();

	unique_ptr<CachedFileHandle> GetHandle() {
		auto this_ptr = shared_from_this();
		return make_uniq<CachedFileHandle>(this_ptr);
	}

	bool Initialized() {
		return initialized;
	}

private:
	void GetDirectoryCacheLock(const string &cache_dir);
	void ReleaseDirectoryCacheLock();

private:
	// FileSystem
	FileSystem &fs;
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
	//! Database Instance
	shared_ptr<DatabaseInstance> db;
	//! Mutex to lock when getting the cached file (Parallel Only)
	mutex cached_files_mutex;
	//! In case of fully downloading the file, the cached files of this query
	unordered_map<string, shared_ptr<CachedFile>> cached_files;
};

} // namespace duckdb
