create user dev identified by admin;
grant all privileges to dev;
alter session set current_schema = dev;

create user prod identified by admin;
grant all privileges to prod;
alter session set current_schema = prod;


drop user dev cascade;
drop user prod cascade;

create table prod.osisp (
   osisp_id_id number not null
);

create table dev.osisp (
   osisp_id_id varchar(10) not null
);

drop table dev.osisp;

select * 
  from all_tables
  WHERE TABLESPACE_NAME = 'USERS';