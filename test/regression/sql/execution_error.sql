SET duckdb.execution = true;
create table int_as_varchar(a varchar);
insert into int_as_varchar SELECT * from (
	VALUES
		('abc')
) t(a);

select a::INTEGER from int_as_varchar;
