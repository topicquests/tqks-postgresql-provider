--
-- File: multidb.sql
--
-- Copyright 2018, TopicQuests
--
-- This SQL script creates the roles, schemas, tables, and indexes
-- necessary for a multi db test.
--

-- Run as superuser.
-- sudo -u postgres psql -a -f <path to multdb.sql> template1

-- Create the tablespace for TopicQuests data.
-- 2018/02/15 - created
--

CREATE USER usr0 PASSWORD 'usr0pwd';
CREATE USER usr1 PASSWORD 'usr1pwd';
CREATE USER usr2 PASSWORD 'usr2pwd';

CREATE USER usradmin PASSWORD 'usradminpwd'  -- full access
    CREATEDB;

-- Switch to the usradmin user to create the database for the multi DB tests.
SET ROLE usradmin;

-- Create the database.
CREATE DATABASE testdb0 ENCODING UTF8;

-- Create the database.
CREATE DATABASE testdb1 ENCODING UTF8;

-- Create the database.
CREATE DATABASE testdb2 ENCODING UTF8;

-- Switch to testdb0.
\c testdb0

SET ROLE usradmin;

CREATE TABLE IF NOT EXISTS
db0_table (
  idx     integer,
  c1      text
);

GRANT ALL PRIVILEGES ON db0_table TO usr0;

-- Switch to testdb1.
\c testdb1

SET ROLE usradmin;

CREATE TABLE IF NOT EXISTS
db1_table (
  idx     integer,
  c1      text
);

GRANT ALL PRIVILEGES ON db1_table TO usr1;

-- Switch to testdb2.
\c testdb2

SET ROLE usradmin;

CREATE TABLE IF NOT EXISTS
db2_table (
  idx     integer,
  c1      text
);

GRANT ALL PRIVILEGES ON db2_table TO usr2;

