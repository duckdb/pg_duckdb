CREATE TYPE mood AS ENUM (
	'sad',
	'neutral',
	'happy'
);
create table tbl (a mood);

insert into tbl select 'happy';
insert into tbl select 'neutral';
insert into tbl select 'sad';

select * from tbl;

select * from tbl where a = 'sad';
select * from tbl where a > 'neutral';

ALTER TYPE mood ADD VALUE 'a-bit-happy' BEFORE 'happy';
ALTER TYPE mood ADD VALUE 'very-happy' AFTER 'happy';
ALTER TYPE mood ADD VALUE 'very-sad' BEFORE 'sad';

insert into tbl values ('very-sad'), ('very-happy'), ('a-bit-happy');
select * from tbl where a > 'neutral';
select * from tbl;

drop table tbl;
drop type mood;
