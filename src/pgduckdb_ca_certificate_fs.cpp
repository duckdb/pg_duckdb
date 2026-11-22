#include "pgduckdb/pgduckdb_ca_certificate_fs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/main/database.hpp"

namespace pgduckdb {

namespace {

/*
 * The CA certificate bundles that httpfs searches when ca_cert_file is not
 * set (CERT_FILE_LOCATIONS in httpfs_certificates.cpp). Since DuckDB 2.0
 * httpfs reads them through the database's file system, so disabling
 * LocalFileSystem for a role without pg_read_server_files also made every
 * https:// and s3:// read fail. These bundles are public, root-owned system
 * files, not user data, so serving them through a separate file system does
 * not give such a role access to any of the server's other files.
 */
constexpr const char *CA_CERTIFICATE_BUNDLES[] = {
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem",
};

/*
 * A LocalFileSystem that only handles the exact paths above, and only for
 * reading. Being its own sub-system, it is not affected by LocalFileSystem
 * being in disabled_filesystems, but it can be disabled itself by adding its
 * name to duckdb.disabled_filesystems.
 */
class CACertificateFileSystem : public duckdb::LocalFileSystem {
public:
	bool
	CanHandleFile(const std::string &path) override {
		for (auto bundle : CA_CERTIFICATE_BUNDLES) {
			if (path == bundle) {
				return true;
			}
		}
		return false;
	}

	std::string
	GetName() const override {
		return "PgDuckDBCACertificateFileSystem";
	}

	/*
	 * The paths are absolute already. Resolving symlinks, like
	 * LocalFileSystem does, would turn them into paths that this file system
	 * does not handle: e.g. /etc is a symlink to /private/etc on macOS and
	 * several distributions ship the bundles as symlinks. The symlinks are
	 * still followed when the file is opened.
	 */
	std::string
	CanonicalizePath(const std::string &path, duckdb::optional_ptr<duckdb::FileOpener>) override {
		return path;
	}

	duckdb::unique_ptr<duckdb::FileHandle>
	OpenFile(const std::string &path, duckdb::FileOpenFlags flags,
	         duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override {
		if (flags.OpenForWriting()) {
			ThrowReadOnly(path);
		}
		return LocalFileSystem::OpenFile(path, flags, opener);
	}

	duckdb::unique_ptr<duckdb::MemoryMappedFile>
	MemoryMapFile(const duckdb::OpenFileInfo &file, duckdb::FileOpenFlags flags, const duckdb::MMapOptions &options,
	              duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override {
		if (flags.OpenForWriting()) {
			ThrowReadOnly(file.path);
		}
		return LocalFileSystem::MemoryMapFile(file, flags, options, opener);
	}

	void
	CreateDirectory(const std::string &directory, duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(directory);
	}

	bool
	CreateDirectoryExtended(const std::string &directory, const duckdb::CreateDirectoryOptions &,
	                        duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(directory);
	}

	void
	CreateDirectoriesRecursive(const std::string &path, duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(path);
	}

	void
	RemoveDirectory(const std::string &directory, duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(directory);
	}

	bool
	RemoveDirectoryExtended(const std::string &directory, const duckdb::RemoveDirectoryOptions &,
	                        duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(directory);
	}

	void
	MoveFile(const std::string &source, const std::string &,
	         duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(source);
	}

	void
	RemoveFile(const std::string &filename, duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(filename);
	}

	bool
	TryRemoveFile(const std::string &filename, duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(filename);
	}

	void
	RemoveFiles(const duckdb::vector<std::string> &filenames,
	            duckdb::optional_ptr<duckdb::FileOpener> = nullptr) override {
		ThrowReadOnly(filenames.empty() ? std::string() : filenames[0]);
	}

private:
	[[noreturn]] void
	ThrowReadOnly(const std::string &path) const {
		throw duckdb::PermissionException("%s only allows reading \"%s\"", GetName(), path);
	}
};

} // namespace

void
RegisterCACertificateFileSystem(duckdb::DatabaseInstance &db) {
	db.GetFileSystem().RegisterSubSystem(duckdb::make_uniq<CACertificateFileSystem>());
}

} // namespace pgduckdb
