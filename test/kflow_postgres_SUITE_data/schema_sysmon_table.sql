SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'latin-1';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = public, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

create role kflow with login password '123';
create role grafana with login password '123';

-----------------------------------------------------------------------------------
-- opstat table
-----------------------------------------------------------------------------------

create table opstat (
    ts timestamp without time zone,
    node text,
    name text,
    data text,
    unit text,
    sess text
);

alter table opstat owner to postgres;
grant insert on table opstat to kflow;
grant select on table opstat to grafana;

-----------------------------------------------------------------------------------
-- prc table
-----------------------------------------------------------------------------------

create table prc (
    node text not null,
    ts timestamp without time zone not null,
    pid text not null,
    dreductions double precision not null,
    dmemory double precision not null,
    reductions bigint not null,
    memory bigint not null,
    message_queue_len bigint not null,
    current_function text,
    initial_call text,
    registered_name text,
    stack_size bigint,
    heap_size bigint,
    total_heap_size bigint,
    current_stacktrace text,
    group_leader text
);

alter table prc owner to postgres;
grant insert on table prc to kflow;
grant select on table prc to grafana;

-----------------------------------------------------------------------------------
-- app_top table
-----------------------------------------------------------------------------------

create type app_top_unit as enum ('reductions', 'memory', 'processes');

create table app_top (
    node text,
    ts timestamp without time zone not null,
    application text,
    unit app_top_unit,
    value numeric
);

alter table app_top owner to postgres;
grant insert on table app_top to kflow;
grant select on table app_top to grafana;

-----------------------------------------------------------------------------------
-- fun_top table
-----------------------------------------------------------------------------------

create type fun_type as enum ('initial_call', 'current_function');

create table fun_top (
    node text,
    ts timestamp without time zone not null,
    fun text,
    fun_type fun_type,
    num_processes numeric
);

alter table fun_top owner to postgres;
grant insert on table fun_top to kflow;
grant select on table fun_top to grafana;

-----------------------------------------------------------------------------------
-- node_role table
-----------------------------------------------------------------------------------

create table node_role (
    node text not null,
    ts timestamp without time zone not null,
    data text
);

alter table node_role owner to postgres;
grant delete on table node_role to kflow;
grant select on table node_role to kflow;
grant insert on table node_role to kflow;
grant select on table node_role to grafana;

create index node_role_ts_idx on node_role(ts);

-----------------------------------------------------------------------------------
-- upserts test table
-----------------------------------------------------------------------------------

create table upserts_test (
    foo text not null,
    bar text,
    primary key(foo)
);

alter table upserts_test owner to postgres;
grant delete on table upserts_test to kflow;
grant select on table upserts_test to kflow;
grant insert on table upserts_test to kflow;
grant select on table upserts_test to grafana;
