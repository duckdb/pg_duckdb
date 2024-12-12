\set pwd `pwd`
CREATE TABLE webpages AS SELECT r['column00'], r['column01'], r['column02'] FROM read_csv(:'pwd' || '/data/web_page.csv') r;

select * from webpages order by column00 limit 2;
select count(*) from webpages;

DROP TABLE webpages;
