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


-- TESTING FIELD

drop trigger groups_cascade_delete;

insert into groups (
   name,
   c_val
) values ( '253504',
           0 );

select *
  from groups;

insert into students (
   name,
   group_id
) values ( 'NikitaStud',
           10 );

select *
  from students
 where name = 'NikitaStud';


delete from groups
 where name = '253504';

delete from students
 where students.id = 1;

commit;