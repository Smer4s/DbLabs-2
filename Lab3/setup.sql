-- T1 T2 T3
create table developer.t1 (
   id   number primary key,
   val varchar(10),
   t3_id number
);

alter table developer.t1
   add FOREIGN key (t3_id) REFERENCES developer.t3(id);

alter table developer.t3
add foreign key(t2_id) references developer.t2(id);

alter table developer.t2
add foreign key(t3_id) references developer.t3(id);

create table developer.t2 (
   id   number primary key,
   val varchar(10),
   t3_id number
);

create table developer.t3 (
   id   number primary key,
   val varchar(10),
   t2_id number
);

drop table developer.t1;
drop table developer.t2;
drop table developer.t3;

-- DIFFERENT TABLES FIELDS

create table developer.test_table (
   id    number,
   count varchar(70)
);
drop table developer.test_table;


create table production.test_table (
   id    number,
   count varchar(60)
);
drop table production.test_table;


-- PROCEDURES DIFF DDL
create or replace procedure developer.test as
begin
   dbms_output.put_line('Hello World!');
end;
/
drop procedure developer.test;

create or replace procedure production.test as
begin
   dbms_output.put_line('Hello World!');
end;
/

drop procedure production.test;

-- FUNCTIONS DIFF DDL
create or replace function developer.test_function (
   input_data number
) return number is
begin
   dbms_output.put_line(input_data);
   return 5;
end;
/

create or replace function production.test_function (
   input_data number
) return number is
begin
   dbms_output.put_line('Hello world!');
   return 20;
end;
/


-- USER SETUP
drop user developer cascade;
drop user production

cascade;
/

create user developer identified by admin;
grant all privileges to developer;
alter session set current_schema = developer;


create user production identified by admin;
grant all privileges to production;
alter session set current_schema = production;