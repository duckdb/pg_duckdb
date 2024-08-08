CREATE TABLE t(
    bool BOOLEAN,
    i2 SMALLINT,
    i4 INT,
    i8 BIGINT,
    fl4 REAL,
    fl8 DOUBLE PRECISION,
    t1 TEXT,
    t2 VARCHAR,
    d DATE,
    ts TIMESTAMP);
INSERT INTO t VALUES (true, 2, 4, 8, 4.0, 8.0, 't1', 't2', '2024-05-04', '2020-01-01 01:02:03');

select fl4 from t;

SELECT bool, i2, i4, i8, fl4, fl8, t1, t2, d, ts FROM t WHERE
    bool = $1
    and i2 = $2
    and i4 = $3
    and i8 = $4
    -- FIXME: The following "larger than" comparisons all have a bugs
    -- somewhere, the comparison reports that they are larger, but they clearly
    -- are not. The floats are actually smaler (to rule out any floating point
    -- funkyness) and the strings are equal.
    and fl4 > $5
    and fl8 > $6
    and t1 > $7
    and t2 > $8
    and d = $9
    and ts = $10
\bind true 2 4 8 5.0 9.0 t1 t2 '2024-05-04' '2020-01-01 01:02:03' \g

-- TODO: Fix this by supporting the UNKNOWN type somehow
SELECT $1 FROM t \bind something \g

drop table t;
