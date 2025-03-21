-- This script is used to enable MotherDuck support if
-- the token is provided in the environment variables.

\getenv lc_token motherduck_token
\getenv uc_token MOTHERDUCK_TOKEN

\if :{?lc_token}
    SELECT duckdb.enable_motherduck(:'lc_token'::TEXT);
\elif :{?uc_token}
    SELECT duckdb.enable_motherduck(:'uc_token'::TEXT);
\endif
