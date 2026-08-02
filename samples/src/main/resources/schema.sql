
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

create table person2 (
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

create table person3 (
                     id bigint not null generated always as identity,
                     name varchar(255),
                     num integer[],
                     primary key (id)
);
alter table person3 add column "some data" integer;
alter table person3 add column edu Education[];
alter table person3 add column edu2 Education[];
alter table person3 add column num2 integer[];

ALTER TABLE person3 RENAME TO "some table";
ALTER TABLE "some table" RENAME COLUMN name TO "first-name";
ALTER type Education RENAME TO edu;
alter table "some table" add column edus edu[];
alter table "some table" drop column edus;
alter table "some table" add column num_arr integer[];

create table person_j (
                      id bigint not null generated always as identity,
                      name varchar(255),
                      edu varchar(255),
                      primary key (id)
);
alter table person_j add column edua varchar(255);
alter table person_j add column edus varchar(255);
alter table person_j add column relations varchar(255);
alter table person_j add column num_arr varchar(255);
alter table person_j add column num varchar(255);
alter table person_j add column num_u_byte varchar(255);
alter table person_j add column num_u_short varchar(255);

create table person_b (
                      id bigint not null generated always as identity,
                      name varchar(255),
                      bin bytea,
                      large oid,
                      primary key (id)
);

create table person_u (
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