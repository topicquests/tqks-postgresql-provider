-- Switch to the tq_admin user to drop the database for TQ objects.
SET ROLE tq_admin;

\c tq_database

DROP INDEX IF EXISTS users_idx;
DROP TABLE tq_authentication.users;
DROP SCHEMA tq_authentication CASCADE;

DROP TABLE tq_contents.proxy;
DROP SCHEMA tq_contents CASCADE;

\c postgres

DROP DATABASE tq_database;

SET ROLE postgres;

-- Primary roles
DROP ROLE tq_users;    -- full access to user information
DROP ROLE tq_users_ro; -- read-only access user information
DROP ROLE tq_proxy;    -- full access to proxy information
DROP ROLE tq_proxy_ro; -- read-only access to proxy information

-- DROP TABLESPACE tq_space;

DROP USER tq_admin;
DROP USER tq_user;
