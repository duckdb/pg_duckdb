----------------------------------------
-- Timezone numeric offset tests
----------------------------------------

-- Positive numeric offset: SET TIME ZONE '+07'
SET TIME ZONE '+07';
SHOW TIME ZONE;
SELECT * FROM duckdb.query($$ SELECT * FROM duckdb_settings() WHERE name = 'TimeZone' $$) r;

-- Negative numeric offset: SET TIME ZONE '-05'
SET TIME ZONE '-05';
SHOW TIME ZONE;
SELECT * FROM duckdb.query($$ SELECT * FROM duckdb_settings() WHERE name = 'TimeZone' $$) r;

-- Zero offset: SET TIME ZONE '+00'
SET TIME ZONE '+00';
SHOW TIME ZONE;
SELECT * FROM duckdb.query($$ SELECT * FROM duckdb_settings() WHERE name = 'TimeZone' $$) r;

--  Positive offset without plus symbol
SET TIME ZONE '04';
SHOW TIME ZONE;
SELECT * FROM duckdb.query($$ SELECT * FROM duckdb_settings() WHERE name = 'TimeZone' $$) r;

-- IANA timezone
SET TIME ZONE 'Asia/Novosibirsk';
SHOW TIME ZONE;
SELECT * FROM duckdb.query($$ SELECT * FROM duckdb_settings() WHERE name = 'TimeZone' $$) r;

-- Reset
SET TIME ZONE DEFAULT;
