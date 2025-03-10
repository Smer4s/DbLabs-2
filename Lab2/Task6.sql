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


-- TESTING FIELD

select *
  from groups;

select *
  from students;

insert into students (
   id,
   name,
   group_id
) values ( 7,
           'Nikita',
           3 );

update students
   set
   group_id = 6
 where id = 7;

delete from students
 where id = 7;