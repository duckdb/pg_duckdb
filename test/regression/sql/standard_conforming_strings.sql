CREATE TABLE foo(t text);

set standard_conforming_strings = off;
select * from foo where t = 'foo\'bar';

DROP TABLE foo;
