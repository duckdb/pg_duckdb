create domain domainvarchar varchar(5);
create domain domainint4 int4;
create domain domaintext text;

create table basictest
           ( testint4 domainint4
           , testtext domaintext
           , testvarchar domainvarchar
           );

INSERT INTO basictest values ('88', 'haha', 'short');      -- Good
INSERT INTO basictest values ('88', 'haha', 'short text'); -- Bad varchar
INSERT INTO basictest values ('888', 'haha', 'short');

select * from basictest;

select testtext || testvarchar as concat, testint4 + 12 as sum
from basictest;

drop table basictest;
drop domain domainvarchar restrict;
drop domain domainint4 restrict;
drop domain domaintext;
