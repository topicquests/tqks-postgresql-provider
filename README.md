
# tqks-postgress-provider

## Build

To build the JAR file, run the command

```
mvn clean install
```

The generated JAR file will be found in the `target` directory.

## Running tests

To run the unit tests, run the command

```
mvn test
```

## Setup the database for Topic Maps

### PostgreSQL database configuration

Install the repository RPM:

```
yum install https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/pgdg-centos10-10-1.noarch.rpm
```

Install the client packages:

```
yum install postgresql10
```

Optionally install the server packages:

```
yum install postgresql10-server
```

Optionally initialize the database and enable automatic start:

```
/usr/pgsql-10/bin/postgresql-10-setup initdb
systemctl enable postgresql-10
systemctl start postgresql-10

yum -y install postgresql10-contrib
```


### How to set up a PostgreSQL database for topic maps

The SQL script postgres-setup.sql must be run as the database
superuser to set up the necessary roles, database, schemas
and tables.

Run the SQL script `postgres-setup.sql` in the `config` directory
by running the command

```
sudo -u postgres psql -a -f <path to postgres-setup.sql> template1
```