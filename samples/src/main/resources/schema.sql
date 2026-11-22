
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE test WITH OWNER = test;

CREATE USER test_big WITH PASSWORD 'test_big';
CREATE DATABASE test_big WITH OWNER = test_big;

GRANT ALL ON SCHEMA public TO test;
GRANT ALL ON SCHEMA public TO test_big;

create type Gender as enum ('female','male');
create cast (varchar as Gender) with inout as implicit;
create cast (Gender as varchar) with inout as implicit;

create type Education as enum ('high','middle','school','scienceDegree','undefined');
create cast (varchar as Education) with inout as implicit;
create cast (Education as varchar) with inout as implicit;

create table person (
    id bigint not null,
    name varchar(255),
    gender Gender,
    birth_date date,
    work varchar(255),
    about varchar(255),
    height integer not null,
    city varchar(255),
    location geography(Point,4326),
    education Education,
    activity_date date,
    primary key (id)
);

create table person_auto (
     id bigint not null generated always as identity,
     name varchar(255),
     gender Gender,
     birth_date date,
     work varchar(255),
     about varchar(255),
     height integer not null,
     city varchar(255),
     education Education,
     primary key (id)
);

create table collections (
     id bigint not null generated always as identity,
     name varchar(255),
     num integer[],
     primary key (id)
);
alter table collections add column "some data" integer;
alter table collections add column edu Education[];
alter table collections add column edu2 Education[];
alter table collections add column num2 integer[];

alter table collections RENAME TO "some table";
alter table "some table" RENAME COLUMN name TO "first-name";
alter type Education RENAME TO edu;
alter table "some table" add column edus edu[];
alter table "some table" add column num_arr integer[];

create table json_collections (
      id bigint not null generated always as identity,
      name varchar(255),
      edu varchar(255),
      primary key (id)
);
alter table json_collections add column edua varchar(255);
alter table json_collections add column edus varchar(255);
alter table json_collections add column relations varchar(255);
alter table json_collections add column num_arr varchar(255);
alter table json_collections add column num varchar(255);
alter table json_collections add column num_u_byte varchar(255);
alter table json_collections add column num_u_short varchar(255);

create table binary_data (
      id bigint not null generated always as identity,
      name varchar(255),
      bin bytea,
      large oid,
      primary key (id)
);

create table unsigned_types (
      id bigint not null generated always as identity,
      name varchar(255),
      num int not null,
      num2 smallint,
      arr bigint[],
      arr2 bytea,
      primary key (id)
);
create table pal (
     id bigint not null generated always as identity,
     edu education,
     primary key (id)
);