create table students_log (
   log_id         number
      generated always as identity
   primary key,
   operation      varchar2(10),
   student_id     number,
   student_name   varchar2(100),
   group_id       number,
   operation_date timestamp
);

select table_name
  from user_tables
 where table_name = 'STUDENTS_LOG';
/


-- CREATE

create or replace trigger log_students_create after
   insert on students
   for each row
begin
   insert into students_log (
      operation,
      student_id,
      student_name,
      group_id,
      operation_date
   ) values ( 'CREATE',
              :new.id,
              :new.name,
              :new.group_id,
              systimestamp );
end;
/

-- UPDATE

create or replace trigger log_students_update after
   update on students
   for each row
begin
   insert into students_log (
      operation,
      student_id,
      student_name,
      group_id,
      operation_date
   ) values ( 'UPDATE',
              :old.id,
              :new.name,
              :new.group_id,
              systimestamp );
end;
/


-- DELETE

create or replace trigger log_students_delete after
   delete on students
   for each row
begin
   insert into students_log (
      operation,
      student_id,
      student_name,
      group_id,
      operation_date
   ) values ( 'DELETE',
              :old.id,
              :old.name,
              :old.group_id,
              systimestamp );
end;
/


-- TESTING FIELD


insert into students (
   name,
   group_id
) values ( 'NikitaStud',
           3 );

update students
   set
   name = 'NikitaStud2'
 where name = 'NikitaStud';


delete from students
 where id = 21;

select *
  from students;


select *
  from students_log;

commit;