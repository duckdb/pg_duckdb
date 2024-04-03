CREATE SCHEMA quack;

CREATE OR REPLACE FUNCTION quack.quack_am_handler(internal)
  RETURNS table_am_handler
  LANGUAGE C
AS 'MODULE_PATHNAME', 'quack_am_handler';

CREATE ACCESS METHOD quack
  TYPE TABLE
  HANDLER quack.quack_am_handler;