#include "pgduckdb/pg/permissions.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"         // GetUserId
#include "catalog/pg_authid.h" // ROLE_PG_WRITE_SERVER_FILES, ROLE_PG_READ_SERVER_FILES
#include "utils/acl.h"         // is_member_of_role
}

namespace pgduckdb::pg {

bool
AllowRawFileAccess() {
#if PG_VERSION_NUM < 130000
	return is_member_of_role(GetUserId(), DEFAULT_ROLE_WRITE_SERVER_FILES) &&
	       is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_SERVER_FILES);
#else
	return is_member_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) &&
	       is_member_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES);
#endif
}

} // namespace pgduckdb::pg
