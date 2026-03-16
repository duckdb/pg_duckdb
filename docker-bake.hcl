variable "REPO" {
  default = "pgduckdb/pgduckdb"
}

variable "POSTGRES_VERSION" {
  default = "16"
}

target "shared" {
  platforms = [
    "linux/amd64",
    "linux/arm64"
  ]
}

target "postgres" {
  inherits = ["shared"]

  contexts = {
    postgres_base = "docker-image://postgres:${POSTGRES_VERSION}-bookworm"
  }

  args = {
    POSTGRES_VERSION = "${POSTGRES_VERSION}"
  }

  tags = [
    "${REPO}:${POSTGRES_VERSION}-dev",
  ]
}

# when inheriting it has to come last because it should overwrite tags and target
target "cnpg" {
  tags = [
      "${REPO}-cnpg:${POSTGRES_VERSION}-dev"
  ]

  target = "output-cnpg"
}

target "pg_duckdb" {
  inherits = ["postgres"]
  target = "output"
}

target "pg_duckdb_14" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = "14"
  }
}

target "pg_duckdb_15" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = "15"
  }
}

target "pg_duckdb_16" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = "16"
  }
}

target "pg_duckdb_17" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = "17"
  }
}

target "pg_duckdb_18" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = "18"
  }
}

target "pg_duckdb_cnpg_18" {
  inherits = ["pg_duckdb_18", "cnpg"]
}

target "default" {
  inherits = ["pg_duckdb_18"]
}

group "pg_duckdb_14_all" {
    targets = ["pg_duckdb_14"]
}

group "pg_duckdb_15_all" {
    targets = ["pg_duckdb_15"]
}

group "pg_duckdb_16_all" {
    targets = ["pg_duckdb_16"]
}

group "pg_duckdb_17_all" {
    targets = ["pg_duckdb_17"]
}

group "pg_duckdb_18_all" {
    targets = ["pg_duckdb_18", "pg_duckdb_cnpg_18"]
}
