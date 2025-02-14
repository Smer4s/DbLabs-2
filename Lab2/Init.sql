create table groups (
   id    number primary key,
   name  varchar2(100),
   c_val number
);

create table students (
   id       number primary key,
   name     varchar2(100),
   group_id number,
   constraint fk_group foreign key ( group_id )
      references groups ( id )
);

select table_name
  from user_tables
 where table_name in ( 'STUDENTS',
                       'GROUPS' );
/