CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
create table tbl (a mood);

insert into tbl select 'happy';
insert into tbl select 'neutral';
insert into tbl select 'sad';

select * from tbl;

drop table tbl;
drop type mood;
