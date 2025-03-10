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

create or replace package triggers_functions is
   is_cascade boolean := false;
end;
/

create sequence students_seq start with 1 increment by 1;

create or replace trigger students_auto_increment before
   insert on students
   for each row
begin
   if :new.id is null then
      select students_seq.nextval
        into :new.id
        from dual;
   end if;
end;
/

create or replace trigger students_check_unique_id before
   insert on students
   for each row
declare
   v_count number;
begin
   select count(*)
     into v_count
     from students
    where id = :new.id;
   if v_count > 0 then
      raise_application_error(
         -20001,
         'ID уже существует в таблице STUDENTS'
      );
   end if;
end;
/

create sequence groups_seq start with 1 increment by 1 nocache;

create or replace trigger groups_auto_increment before
   insert on groups
   for each row
begin
   if :new.id is null then
      select groups_seq.nextval
        into :new.id
        from dual;
   end if;
end;
/

create or replace trigger groups_check_unique_id before
   insert on groups
   for each row
declare
   v_count number;
begin
   select count(*)
     into v_count
     from groups
    where id = :new.id;
   if v_count > 0 then
      raise_application_error(
         -20002,
         'ID уже существует в таблице GROUPS'
      );
   end if;
end;
/

create or replace trigger groups_check_unique_name before
   insert on groups
   for each row
declare
   v_count number;
begin
   select count(*)
     into v_count
     from groups
    where name = :new.name;
   if v_count > 0 then
      raise_application_error(
         -20003,
         'Название группы уже существует в таблице GROUPS'
      );
   end if;
end;
/


create or replace trigger groups_cascade_delete before
   delete on groups
   for each row
begin
   triggers_functions.is_cascade := true;
   delete from students
    where group_id = :old.id;
   triggers_functions.is_cascade := false;
end;
/


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


create or replace procedure restore_students_at_time (
   p_restore_time timestamp
) is
   dummy_variable number;
begin
   delete from students;

   for rec in (
      select *
        from students_log
       where operation_date <= p_restore_time
       order by operation_date asc
   ) loop
      if rec.operation = 'CREATE' then
         begin
            select 1
              into dummy_variable
              from groups
             where id = rec.group_new_id;
         exception
            when no_data_found then
               insert into groups (
                  id,
                  name,
                  c_val
               ) values ( rec.group_new_id,
                          rec.group_new_name,
                          0 );
         end;

         insert into students (
            id,
            name,
            group_id
         ) values ( rec.student_id,
                    rec.student_new_name,
                    rec.group_new_id );

      elsif rec.operation = 'UPDATE' then
         begin
            select 1
              into dummy_variable
              from groups
             where id = rec.group_new_id;
         exception
            when no_data_found then
               insert into groups (
                  id,
                  name,
                  c_val
               ) values ( rec.group_new_id,
                          rec.group_new_name,
                          0 );
         end;

         update students
            set name = rec.student_new_name,
                group_id = rec.group_new_id
          where id = rec.student_id;

      elsif rec.operation = 'DELETE' then
         delete from students
          where id = rec.student_id;
      end if;
   end loop;
end;
/

create or replace procedure restore_students_at_offset (
   p_offset interval day to second
) is
   v_current_time timestamp;
begin
   select systimestamp
     into v_current_time
     from dual;
   restore_students_at_time(v_current_time - p_offset);
end;
/

create or replace trigger update_group_c_val after
   insert or update or delete on students
   for each row
begin
   if triggers_functions.is_cascade = false then
      if inserting then
         update groups
            set
            c_val = c_val + 1
          where id = :new.group_id;
      elsif updating then
         if :old.group_id != :new.group_id then
            update groups
               set
               c_val = c_val - 1
             where id = :old.group_id;
            update groups
               set
               c_val = c_val + 1
             where id = :new.group_id;
         end if;
      elsif deleting then
         update groups
            set
            c_val = c_val - 1
          where id = :old.group_id;
      end if;
   end if;
end;
/

drop trigger update_group_c_val;
drop trigger students_auto_increment;
drop trigger students_check_unique_id;
drop trigger log_students_delete;
drop trigger log_students_update;
drop trigger log_students_create;
drop trigger groups_auto_increment;
drop trigger groups_check_unique_id;
drop trigger groups_check_unique_name;
drop trigger groups_cascade_delete;


-- TESTING FIELD

insert into groups (
   id,
   name,
   c_val
) values ( 11,
           '253503',
           0 );




update students
   set name = 'NikitaStud3',
       group_id = 11
 where id = 40;

insert into students (
   id,
   name,
   group_id
) values ( 47,
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

delete from groups
 where id = 11;

begin
   restore_students_at_time(timestamp '2025-03-10 16:11:08.272');
end;
/

begin
   restore_students_at_offset(interval '2' minute);
end;
/

commit;

rollback;