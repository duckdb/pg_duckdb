-- Test all ALTER TABLE commands supported by pgduckdb_get_alterdef
-- Set up a test table
SET duckdb.force_execution = false;
CREATE TEMP TABLE alter_test(
    id INT,
    name TEXT,
    value DOUBLE PRECISION DEFAULT 0.0,
    created_at TIMESTAMP
) USING duckdb;

INSERT INTO alter_test VALUES (1, 'test1', 10.5, '2023-01-01 12:00:00');
INSERT INTO alter_test VALUES (2, 'test2', 20.5, '2023-01-02 12:00:00');

-- Verify initial state
SELECT * FROM alter_test ORDER BY id;
SELECT * FROM duckdb.query('DESCRIBE pg_temp.alter_test');

-- 1. ADD COLUMN
ALTER TABLE alter_test ADD COLUMN description TEXT;
ALTER TABLE alter_test ADD COLUMN active BOOLEAN DEFAULT true;
ALTER TABLE alter_test ADD COLUMN score INT DEFAULT 100 NOT NULL;

-- Verify columns were added
SELECT * FROM alter_test ORDER BY id;
SELECT * FROM duckdb.query('DESCRIBE pg_temp.alter_test');

-- 2. ALTER COLUMN TYPE
ALTER TABLE alter_test ALTER COLUMN id TYPE BIGINT;
ALTER TABLE alter_test ALTER COLUMN value TYPE REAL;

-- Verify column types were changed
SELECT * FROM duckdb.query('DESCRIBE pg_temp.alter_test');

-- 3. DROP COLUMN
ALTER TABLE alter_test DROP COLUMN description;

-- Verify column was dropped
SELECT * FROM duckdb.query('DESCRIBE pg_temp.alter_test');

-- 4. SET/DROP DEFAULT
ALTER TABLE alter_test ALTER COLUMN name SET DEFAULT 'unnamed';
INSERT INTO alter_test(id) VALUES (3);
SELECT * FROM alter_test WHERE id = 3;

ALTER TABLE alter_test ALTER COLUMN name DROP DEFAULT;
INSERT INTO alter_test(id) VALUES (4);
SELECT * FROM alter_test WHERE id = 4;

-- 5. SET/DROP NOT NULL
ALTER TABLE alter_test ALTER COLUMN name SET NOT NULL;
-- This should fail
\set ON_ERROR_STOP 0
UPDATE alter_test SET name = NULL WHERE id = 1;
\set ON_ERROR_STOP 1

ALTER TABLE alter_test ALTER COLUMN name DROP NOT NULL;
-- This should succeed
UPDATE alter_test SET name = NULL WHERE id = 1;
SELECT * FROM alter_test WHERE id = 1;

-- 6. ADD CONSTRAINT (CHECK)
ALTER TABLE alter_test ADD CONSTRAINT positive_id CHECK (id > 0);
-- This should fail
\set ON_ERROR_STOP 0
INSERT INTO alter_test(id, name) VALUES (-1, 'negative');
\set ON_ERROR_STOP 1

-- 6. ADD CONSTRAINT (PRIMARY KEY)
ALTER TABLE alter_test ADD PRIMARY KEY (id);
-- This should fail due to duplicate key
\set ON_ERROR_STOP 0
INSERT INTO alter_test(id, name) VALUES (1, 'duplicate');
\set ON_ERROR_STOP 1

-- 6. ADD CONSTRAINT (UNIQUE)
ALTER TABLE alter_test ADD CONSTRAINT unique_name UNIQUE (name);
-- This should fail due to duplicate name
\set ON_ERROR_STOP 0
UPDATE alter_test SET name = 'test2' WHERE id = 3;
\set ON_ERROR_STOP 1

-- 7. DROP CONSTRAINT
ALTER TABLE alter_test DROP CONSTRAINT unique_name;
-- This should now succeed
UPDATE alter_test SET name = 'test2' WHERE id = 3;
SELECT * FROM alter_test WHERE id = 3;

ALTER TABLE alter_test DROP CONSTRAINT positive_id;
-- This should now succeed
INSERT INTO alter_test(id, name) VALUES (-1, 'negative');
SELECT * FROM alter_test WHERE id = -1;

-- 8. SET/RESET table options
-- Note: DuckDB supports limited table options compared to PostgreSQL
ALTER TABLE alter_test SET (fillfactor = 90);
ALTER TABLE alter_test RESET (fillfactor);

-- Clean up
DROP TABLE alter_test;

