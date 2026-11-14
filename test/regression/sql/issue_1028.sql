-- Postgres validates the encoding of text input, but C functions can still
-- create invalid text, like this internal function that concatenates byteas
-- but is declared to return text. Such values must not reach DuckDB (#1028).
CREATE FUNCTION issue_1028_text(bytea, bytea) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'byteacat';

SET duckdb.force_execution = false;
CREATE TABLE issue_1028_invalid(id int, t text, v varchar, a text[]);
INSERT INTO issue_1028_invalid
SELECT 1, issue_1028_text('caf', '\xc3'), issue_1028_text('caf', '\xc3'), ARRAY['ok', issue_1028_text('caf', '\xc3')];
CREATE TABLE issue_1028_valid(t text);
INSERT INTO issue_1028_valid VALUES ('café');
RESET duckdb.force_execution;

SELECT reverse(t) FROM issue_1028_invalid;
SELECT substring(t, 1, 4) FROM issue_1028_invalid;
SELECT v FROM issue_1028_invalid;
SELECT a FROM issue_1028_invalid;
-- Columns without invalid text can still be read
SELECT id FROM issue_1028_invalid;

-- DuckDB is still usable afterwards
SELECT reverse(t) FROM issue_1028_valid;
SELECT * FROM duckdb.query($$ SELECT 42 AS answer $$);

DROP TABLE issue_1028_invalid, issue_1028_valid;
DROP FUNCTION issue_1028_text;
