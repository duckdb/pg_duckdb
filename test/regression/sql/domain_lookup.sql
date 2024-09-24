create domain my_string as varchar;
CREATE TABLE t(a my_string);

insert into t values ('test');
insert into t values ('apple');
select a > 'hello' from t;

DROP TABLE t;
drop domain my_string;
