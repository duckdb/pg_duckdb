-- Using aclitem as a type that we wil not support in pg_duckdb any time soon.
CREATE TABLE ddl_table(id int, myacl aclitem);
INSERT INTO ddl_table VALUES (1, NULL);
-- Should fail because aclitem is not supported.
SELECT * from ddl_table;
-- Should succed because we don't actually query aclitem
SELECT id from ddl_table;
ALTER TABLE ddl_table ADD COLUMN a int DEFAULT 10;
ALTER TABLE ddl_table ADD COLUMN b int DEFAULT 20;
ALTER TABLE ddl_table ADD COLUMN c int DEFAULT NULL;
ALTER TABLE ddl_table ADD COLUMN d int DEFAULT 30;
ALTER TABLE ddl_table DROP COLUMN myacl;
select * from ddl_table;
select a, c, d id from ddl_table;
