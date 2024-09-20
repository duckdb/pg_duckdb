\getenv pwd PWD
CREATE TABLE webpages AS SELECT * FROM read_csv(:'pwd' || '/data/web_page.csv') as (column00 int, column01 text, column02 date);

select * from webpages order by column00 limit 2;
select count(*) from webpages;

DROP TABLE webpages;
