create table students_log (
   log_id           number
      generated always as identity
   primary key,
   operation        varchar2(10),
   student_id       number,
   student_old_name varchar2(100),
   student_new_name varchar2(100),
   group_old_id     number,
   group_new_id     number,
   group_old_name   varchar2(100),
   group_new_name   varchar2(100),
   operation_date   timestamp
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
   declare
      v_group_name varchar2(100);
   begin
      select name
        into v_group_name
        from groups
       where id = :new.group_id;
      insert into students_log (
         operation,
         student_id,
         student_new_name,
         group_new_id,
         group_new_name,
         operation_date
      ) values ( 'CREATE',
                 :new.id,
                 :new.name,
                 :new.group_id,
                 v_group_name,
                 systimestamp );
   end;
end;
/



-- UPDATE

create or replace trigger log_students_update after
   update on students
   for each row
begin
   declare
      v_group_old_name varchar2(100);
      v_group_new_name varchar2(100);
   begin
      select name
        into v_group_old_name
        from groups
       where id = :old.group_id;
      select name
        into v_group_new_name
        from groups
       where id = :new.group_id;
      insert into students_log (
         operation,
         student_id,
         student_old_name,
         student_new_name,
         group_old_id,
         group_new_id,
         group_old_name,
         group_new_name,
         operation_date
      ) values ( 'UPDATE',
                 :old.id,
                 :old.name,
                 :new.name,
                 :old.group_id,
                 :new.group_id,
                 v_group_old_name,
                 v_group_new_name,
                 systimestamp );
   end;
end;
/




-- DELETE

create or replace trigger log_students_delete after
   delete on students
   for each row
begin
   declare
      v_group_old_name varchar2(100);
   begin
      if triggers_functions.is_cascade = false then
         dbms_output.put_line('Current var state is false');
      elsif triggers_functions.is_cascade = true then
         dbms_output.put_line('true');
      elsif triggers_functions.is_cascade is null then
         dbms_output.put_line('awdawd');
      elsif triggers_functions.is_cascade is not null then
         dbms_output.put_line('Not null');
      end if;

      if triggers_functions.is_cascade = false then
         select name
           into v_group_old_name
           from groups
          where id = :old.group_id;
         insert into students_log (
            operation,
            student_id,
            student_old_name,
            group_old_id,
            group_old_name,
            operation_date
         ) values ( 'DELETE',
                    :old.id,
                    :old.name,
                    :old.group_id,
                    v_group_old_name,
                    systimestamp );
      end if;
   end;
end;
/




-- TESTING FIELD

insert into groups (
   name,
   c_val
) values ( '253503',
           0 );




update students
   set name = 'NikitaStud3',
       group_id = 3
 where id = 40;

insert into students (
   id,
   name,
   group_id
) values ( 40,
           'NikitaStud',
           11 );


delete from students
 where id = 40;

select *
  from students;

select *
  from groups;



select *
  from students_log;

delete from students_log;

commit;