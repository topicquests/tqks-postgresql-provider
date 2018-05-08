--
-- File: clean_multidb.sql
--
-- Copyright 2018, TopicQuests
--
-- This SQL script removes the roles, tables, and indexes
-- necessary for a multi db test.
--

-- Run as superuser.
-- sudo -u postgres psql -a -f <path to clean_multdb.sql> template1

SET ROLE usradmin;

DROP DATABASE testdb0;
DROP DATABASE testdb1;
DROP DATABASE testdb2;

SET ROLE postgres;

DROP USER usr0;
DROP USER usr1;
DROP USER usr2;
DROP USER usradmin;

\q
