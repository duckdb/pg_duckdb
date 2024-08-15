-- CHAR
CREATE TABLE chr(a CHAR);
INSERT INTO chr SELECT CAST(a AS CHAR) from (VALUES (-128), (0), (127)) t(a);
SELECT * FROM chr;

-- SMALLINT
CREATE TABLE small(a SMALLINT);
INSERT INTO small SELECT CAST(a AS SMALLINT) from (VALUES (-32768), (0), (32767)) t(a);
SELECT * FROM small;

-- INTEGER
CREATE TABLE intgr(a INTEGER);
INSERT INTO intgr SELECT CAST(a AS INTEGER) from (VALUES (-2147483648), (0), (2147483647)) t(a);
SELECT * FROM intgr;

-- BIGINT
CREATE TABLE big(a BIGINT);
INSERT INTO big SELECT CAST(a AS BIGINT) from (VALUES (-9223372036854775808), (0), (9223372036854775807)) t(a);
SELECT * FROM big;

--- BOOL
CREATE TABLE bool_tbl(a BOOL);
INSERT INTO bool_tbl SELECT CAST(a AS BOOL) from (VALUES (False), (NULL), (True)) t(a);
SELECT * FROM bool_tbl;

--- VARCHAR
CREATE TABLE varchar_tbl(a VARCHAR);
INSERT INTO varchar_tbl SELECT CAST(a AS VARCHAR) from (VALUES (''), (NULL), ('test'), ('this is a long string')) t(a);
SELECT * FROM varchar_tbl;
SELECT * FROM varchar_tbl WHERE a = 'test';

-- DATE
CREATE TABLE date_tbl(a DATE);
INSERT INTO date_tbl SELECT CAST(a AS DATE) FROM (VALUES ('2022-04-29'::DATE), (NULL), ('2023-05-15'::DATE)) t(a);
SELECT * FROM date_tbl;

-- TIMESTAMP
CREATE TABLE timestamp_tbl(a TIMESTAMP);
INSERT INTO timestamp_tbl SELECT CAST(a AS TIMESTAMP) FROM (VALUES
	('2022-04-29 10:15:30'::TIMESTAMP),
	(NULL),
	('2023-05-15 12:30:45'::TIMESTAMP)
) t(a);
SELECT * FROM timestamp_tbl;

-- FLOAT4
CREATE TABLE float4_tbl(a FLOAT4);
INSERT INTO float4_tbl SELECT CAST(a AS FLOAT4) FROM (VALUES
	(0.234234234::FLOAT4),
	(NULL),
	(458234502034234234234.000012::FLOAT4)
) t(a);
SELECT * FROM float4_tbl;

-- FLOAT8
CREATE TABLE float8_tbl(a FLOAT8);
INSERT INTO float8_tbl SELECT CAST(a AS FLOAT8) FROM (VALUES
	(0.234234234::FLOAT8),
	(NULL),
	(458234502034234234234.000012::FLOAT8)
) t(a);
SELECT * FROM float8_tbl;

-- NUMERIC as DOUBLE
CREATE TABLE numeric_as_double(a NUMERIC);
INSERT INTO numeric_as_double SELECT a FROM (VALUES
	(0.234234234),
	(NULL),
	(458234502034234234234.000012)
) t(a);
SELECT * FROM numeric_as_double;

-- NUMERIC with a physical type of SMALLINT
CREATE TABLE smallint_numeric(a NUMERIC(4, 2));
INSERT INTO smallint_numeric SELECT a FROM (VALUES
	(0.23),
	(NULL),
	(45.12)
) t(a);
SELECT * FROM smallint_numeric;

-- NUMERIC with a physical type of INTEGER
CREATE TABLE integer_numeric(a NUMERIC(9, 6));
INSERT INTO integer_numeric SELECT a FROM (VALUES
	(243.345035::NUMERIC(9,6)),
	(NULL),
	(45.000012::NUMERIC(9,6))
) t(a);
SELECT * FROM integer_numeric;

-- NUMERIC with a physical type of BIGINT
CREATE TABLE bigint_numeric(a NUMERIC(18, 12));
INSERT INTO bigint_numeric SELECT a FROM (VALUES
	(856324.111122223333::NUMERIC(18,12)),
	(NULL),
	(12.000000000001::NUMERIC(18,12))
) t(a);
SELECT * FROM bigint_numeric;

-- NUMERIC with a physical type of HUGEINT
CREATE TABLE hugeint_numeric(a NUMERIC(38, 24));
INSERT INTO hugeint_numeric SELECT a FROM (VALUES
	(32942348563242.111222333444555666777888::NUMERIC(38,24)),
	(NULL),
	(123456789.000000000000000000000001::NUMERIC(38,24))
) t(a);
SELECT * FROM hugeint_numeric;

-- UUID
CREATE TABLE uuid_tbl(a UUID);
INSERT INTO uuid_tbl SELECT CAST(a as UUID) FROM (VALUES
	('80bf0be9-89be-4ef8-bc58-fc7d691c5544'),
	(NULL),
	('00000000-0000-0000-0000-000000000000')
) t(a);
SELECT * FROM uuid_tbl;

-- JSON
CREATE TABLE json_tbl(a JSON);
INSERT INTO json_tbl SELECT CAST(a as JSON) FROM (VALUES
    ('{"key1": "value1", "key2": "value2"}'),
    ('["item1", "item2", "item3"]'),
    (NULL),
    ('{}')
) t(a);
SELECT * FROM json_tbl;

-- REGCLASSOID
CREATE TABLE regclass_tbl (a REGCLASS);
INSERT INTO regclass_tbl VALUES (42), (3_000_000_000);
SELECT * FROM regclass_tbl;

DROP TABLE chr;
DROP TABLE small;
DROP TABLE intgr;
DROP TABLE big;
DROP TABLE varchar_tbl;
DROP TABLE date_tbl;
DROP TABLE timestamp_tbl;
DROP TABLE float4_tbl;
DROP TABLE float8_tbl;
DROP TABLE numeric_as_double;
DROP TABLE smallint_numeric;
DROP TABLE integer_numeric;
DROP TABLE bigint_numeric;
DROP TABLE hugeint_numeric;
DROP TABLE uuid_tbl;
DROP TABLE json_tbl;
DROP TABLE regclass_tbl;
