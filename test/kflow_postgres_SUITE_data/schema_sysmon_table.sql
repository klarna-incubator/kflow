create role kflow with login password '123';
create role grafana with login password '123';

-----------------------------------------------------------------------------------
-- node_role table
-----------------------------------------------------------------------------------

create table node_role (
    node text not null,
    ts timestamp without time zone not null,
    data text
);

alter table node_role owner to kflow;

-----------------------------------------------------------------------------------
-- upserts test table
-----------------------------------------------------------------------------------

create table upserts_test (
    foo text not null,
    bar text,
    primary key(foo)
);

alter table upserts_test owner to kflow;

-----------------------------------------------------------------------------------
-- partition test table
-----------------------------------------------------------------------------------

create table part_table (
    node text not null,
    ts timestamp without time zone not null,
    data text
) partition by range(ts);

alter table part_table owner to kflow;
