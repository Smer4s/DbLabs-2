create table employees (
   id     number primary key,
   name   varchar2(100),
   status varchar2(20)
);

drop table employees cascade constraints;

 
insert into employees (
   id,
   name,
   status
) values ( 1,
           'Alice',
           'ACTIVE' );
insert into employees (
   id,
   name,
   status
) values ( 2,
           'Bob',
           'INACTIVE' );
insert into employees (
   id,
   name,
   status
) values ( 3,
           'Carol',
           'ACTIVE' );

commit;
/

select *
  from employees;