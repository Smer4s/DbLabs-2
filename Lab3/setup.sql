drop user developer cascade;
drop user production cascade;

commit;

-- Environment Configuration
create user developer identified by admin;
grant all privileges to developer;
alter session set current_schema = developer;

create user production identified by admin;
grant all privileges to production;
alter session set current_schema = production;

create table developer.test (
   id number not null
);

drop table developer.test;

create table production.test (
   id varchar(10) not null
);

drop table production.test;


create or replace procedure developer.executestepone (
   inputparam int
) is
begin
   dbms_output.put_line('Input value: ' || to_char(inputparam));
end;
/

create or replace procedure production.executestepone (
   input varchar
) is
begin
   dbms_output.put_line(input);
end;
/