-- STUDENTS

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

drop trigger students_auto_increment;

drop trigger students_check_unique_id;

-- GROUPS


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

drop trigger groups_auto_increment;

drop trigger groups_check_unique_id;

drop trigger groups_check_unique_name;

-- TESTING FIELD

insert into groups (
   name,
   c_val
) values ( '253505',
           0 );

select *
  from groups;

delete from groups where id = 4;

select *
  from students;

insert into students (
   name,
   group_id
) values ( 'NikitaStud2',
           7 );

select *
  from students
 where name = 'NikitaStud2';

commit;