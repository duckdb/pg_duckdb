variable "REPO" {
  # temp repo on dockerhub
  default = "wuputah/pg_duckdb"
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
    // pg_duckdb = "target:pg_duckdb_${POSTGRES_VERSION}"
    postgres_base = "docker-image://postgres:${POSTGRES_VERSION}-bookworm"
  }

  args = {
    POSTGRES_VERSION = "${POSTGRES_VERSION}"
  }

  tags = [
    "${REPO}:${POSTGRES_VERSION}",
  ]
}

target "pg_duckdb" {
  inherits = ["postgres"]
  target = "output"
}

target "pg_duckdb_16" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = 16
  }
}

target "pg_duckdb_17" {
  inherits = ["pg_duckdb"]

  args = {
    POSTGRES_VERSION = 17
  }
}

target "default" {
  inherits = ["pg_duckdb_16"]
}
