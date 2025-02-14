drop trigger log_students_delete;
drop trigger log_students_update;
drop trigger log_students_create;

create or replace procedure restore_students_at_time (
   p_restore_time timestamp
) is
begin
   delete from students;

   for rec in (
      select *
        from students_log
       where operation_date <= p_restore_time
       order by operation_date asc
   ) loop
      if rec.operation = 'CREATE' then
         insert into students (
            id,
            name,
            group_id
         ) values ( rec.student_id,
                    rec.student_new_name,
                    rec.group_new_id );
      elsif rec.operation = 'UPDATE' then
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



-- TESTING FIELD

commit;


rollback;


select *
  from students_log;

begin
   restore_students_at_time(timestamp '2025-02-14 16:52:51.4');
end;
/

select *
  from students;

delete from students;
/

select *
  from groups;

insert into groups (
   id,
   name,
   c_val
) values ( 4,
           253506,
           0 );

insert into students (
   name,
   group_id
) values ( 'nikita2',
           4 );

delete from groups;
/

begin
   restore_students_at_offset(interval '2' minute);
end;
/


commit;