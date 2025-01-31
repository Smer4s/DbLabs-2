-- TASK 1

create table mytable (
   id  number,
   val number
)
/

select table_name
  from user_tables
 where table_name = 'MYTABLE';
/

-- TASK 2

declare
   number_count number := 10000;
begin
   for i in 1..number_count loop
      insert into mytable (
         id,
         val
      ) values ( i,
                 round(dbms_random.value(
                    1,
                    number_count
                 )) );
   end loop;
end;
/

select count(*)
  from mytable;
/

select *
  from mytable;
/

delete from mytable;
/

-- TASK 3

create or replace function check_even_odd_balance return varchar2 is
   even_count number := 0;
   odd_count  number := 0;
begin
   select count(*)
     into even_count
     from mytable
    where mod(
      val,
      2
   ) = 0;

   select count(*)
     into odd_count
     from mytable
    where mod(
      val,
      2
   ) = 1;

   if even_count > odd_count then
      return 'TRUE';
   elsif even_count < odd_count then
      return 'FALSE';
   else
      return 'EQUAL';
   end if;
end;
/

select check_even_odd_balance()
  from dual;
/

-- TASK 4

create or replace procedure generate_insert_command (
   p_id in number
) is
   v_id  number;
   v_val number;
   v_sql varchar2(100);
begin
   select id,
          val
     into
      v_id,
      v_val
     from mytable
    where id = p_id;

   v_sql := 'INSERT INTO MYTABLE (id, val) VALUES ('
            || v_id
            || ', '
            || v_val
            || ');';
   dbms_output.put_line(v_sql);
exception
   when others then
      dbms_output.put_line('Нет записи с таким ID: ' || p_id);
end;
/

begin
   generate_insert_command(5);
end;
/

-- TASK 5
create or replace procedure insert_record (
   p_id  in number,
   p_val in number
) is
   v_sql varchar2(100);
begin
   v_sql := 'INSERT INTO MyTable (id, val) VALUES ('
            || p_id
            || ', '
            || p_val
            || ')';
   execute immediate v_sql;
exception
   when others then
      dbms_output.put_line('Ошибка вставки: ' || sqlerrm);
end;
/

create or replace procedure update_record (
   p_id  in number,
   p_val in number
) is
   v_sql varchar2(4000);
begin
   v_sql := 'UPDATE MyTable SET val = '
            || p_val
            || ' WHERE id = '
            || p_id;
   execute immediate v_sql;
exception
   when others then
      dbms_output.put_line('Ошибка обновления: ' || sqlerrm);
end;
/

create or replace procedure delete_record (
   p_id in number
) is
   v_sql varchar2(4000);
begin
   v_sql := 'DELETE FROM MyTable WHERE id = ' || p_id;
   execute immediate v_sql;
exception
   when others then
      dbms_output.put_line('Ошибка удаления: ' || sqlerrm);
end;
/


begin
   insert_record(
      1,
      2
   );
   update_record(
      1,
      3
   );
   delete_record(1);
end;
/

select *
  from mytable
 where id = 1;
/


-- TASK 6
create or replace function calculate_reward (
   percent number,
   salary  number
) return number is
   percent_rate number;
   total        number;
   percent_out_of_range exception;
   salary_out_of_range exception;
begin
   if percent > 100
   or percent < 0 then
      raise percent_out_of_range;
   end if;
   if salary <= 0 then
      raise salary_out_of_range;
   end if;

   percent_rate := percent / 100;
   total := ( 1 + percent_rate ) * 12 * salary;
   return total;
exception
   when percent_out_of_range then
      raise_application_error(
         -20000,
         'Entered percent '
         || percent
         || '% must be in range [0, 100]'
      );
   when salary_out_of_range then
      raise_application_error(
         -20001,
         'Entered salary ('
         || salary
         || ') must be positive'
      );
end;
/

select calculate_reward(
   1e100,
   1e100
)
  from dual;
/