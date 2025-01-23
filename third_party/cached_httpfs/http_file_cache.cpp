#include "http_file_cache.hpp"

namespace duckdb {

CachedFile::CachedFile(LocalCacheFileSystem &_fs, const string &key, bool cache_file) : fs(_fs), file_name(key) {
	GetDirectoryCacheLock();

	FileOpenFlags flags =
	    FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS | FileLockType::READ_LOCK;
	handle = fs.OpenFile(file_name, flags);
	if (handle) {
		initialized = true;
		size = handle->GetFileSize();
	} else if (cache_file) {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE | FileLockType::WRITE_LOCK;
		handle = fs.OpenFile(file_name, flags);
	}

	ReleaseDirectoryCacheLock();
}

void CachedFile::GetDirectoryCacheLock() {
	constexpr const char* lock_file_name = ".lock";
	FileOpenFlags flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                      FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE | FileFlags::FILE_FLAGS_NULL_IF_EXISTS |
	                      FileLockType::WRITE_LOCK;
	directory_lock_handle = fs.OpenFile(lock_file_name, flags);
	if (directory_lock_handle == nullptr) {
		flags = FileFlags::FILE_FLAGS_WRITE | FileLockType::WRITE_LOCK;
		directory_lock_handle = fs.OpenFile(lock_file_name, flags);
	}
}

void CachedFile::ReleaseDirectoryCacheLock() {
	directory_lock_handle->Close();
	directory_lock_handle.reset();
}

CachedFileHandle::CachedFileHandle(shared_ptr<CachedFile> &file_p) {
	file = file_p;
}

void CachedFileHandle::WriteMetadata(const string &cache_key, const string &remote_path, idx_t total_size) {
	D_ASSERT(!file->initialized);
	FileOpenFlags flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE | FileLockType::WRITE_LOCK;
	auto handle = file->fs.OpenFile(cache_key + ".meta", flags);
	auto cached_file_timestamp = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	string metadata_info = cache_key + "," + remote_path + "," + std::to_string(total_size) + "," + std::to_string(cached_file_timestamp);
	handle->Write((void *)metadata_info.c_str(), metadata_info.length(), 0);
	handle->Close();

}

void CachedFileHandle::SetInitialized(idx_t total_size) {
	D_ASSERT(!file->initialized);
	file->size = total_size;
	file->initialized = true;
	file->handle->Close();
	FileOpenFlags flags = FileFlags::FILE_FLAGS_READ | FileLockType::READ_LOCK;
	file->handle = file->fs.OpenFile(file->file_name, flags);
}

void CachedFileHandle::Allocate(idx_t size) {
	D_ASSERT(!file->initialized);
	file->handle->Trim(0, size);
	file->capacity = size;
}

void CachedFileHandle::GrowFile(idx_t new_capacity, idx_t bytes_to_copy) {
	D_ASSERT(!file->initialized);
	file->handle->Trim(bytes_to_copy, new_capacity);
}

void CachedFileHandle::Write(const char *buffer, idx_t length, idx_t offset) {
	D_ASSERT(!file->initialized);
	file->handle->Write((void *)buffer, length, offset);
}

void CachedFileHandle::Read(void *buffer, idx_t length, idx_t offset) {
	D_ASSERT(file->initialized);
	file->handle->Read((void *)buffer, length, offset);
}

LocalCacheFileSystem& HTTPFileCache::GetFS(const string &cache_dir) {
	if (cached_fs) {
		cached_fs->AssertSameCacheDir(cache_dir);
		return *cached_fs;
	}

	{
		lock_guard<mutex> lock(cached_fs_mutex);
		if (!cached_fs) {
			cached_fs = make_uniq<LocalCacheFileSystem>(cache_dir);
		}
	}
	return HTTPFileCache::GetFS(cache_dir);
}

//! Get cache entry, create if not exists only if caching is enabled
shared_ptr<CachedFile> HTTPFileCache::GetCachedFile(const string &cache_dir, const string &key, bool cache_file) {
	lock_guard<mutex> lock(cached_files_mutex);
	auto it = cached_files.find(key);
	if (it != cached_files.end()) {
		return it->second;
	}

	auto& fs = GetFS(cache_dir);
	auto cache_entry = make_shared_ptr<CachedFile>(fs, key, cache_file);
	if (cache_entry->Initialized() || cache_file) {
		cached_files[key] = cache_entry;
		return cache_entry;
	} else {
		return nullptr;
	}
}

} // namespace duckdb
