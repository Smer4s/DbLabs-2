create or replace procedure generate_sync_script (
   p_dev_schema  varchar2,
   p_prod_schema varchar2
)
   authid current_user
is

   function objects_different (
      p_object_name varchar2,
      p_object_type varchar2
   ) return boolean is
      v_dev_ddl  clob;
      v_prod_ddl clob;
   begin
        -- ddl для дева
      begin
         select dbms_metadata.get_ddl(
            p_object_type,
            p_object_name,
            p_dev_schema
         )
           into v_dev_ddl
           from dual;
      exception
         when others then
            return true;
      end;
        -- аналогчно прод
      begin
         select dbms_metadata.get_ddl(
            p_object_type,
            p_object_name,
            p_prod_schema
         )
           into v_prod_ddl
           from dual;
      exception
         when others then
            return true;
      end;

      v_dev_ddl := replace(
         v_dev_ddl,
         'EDITIONABLE ',
         ''
      );
      v_prod_ddl := replace(
         v_prod_ddl,
         'EDITIONABLE ',
         ''
      );
      return v_dev_ddl <> v_prod_ddl;
   end;

   procedure process_objects (
      p_object_type varchar2
   ) is
   begin
      dbms_output.put_line(chr(10)
                           || '/* '
                           || p_object_type
                           || ' DIFFERENCES */');
        --новые объекты (есть в деве, нет в проде)
      for obj in (
         select object_name
           from all_objects
          where owner = p_dev_schema
            and object_type = p_object_type
            and object_name not in (
            select object_name
              from all_objects
             where owner = p_prod_schema
               and object_type = p_object_type
         )
      ) loop
         dbms_output.put_line('-- Create '
                              || p_object_type
                              || ': '
                              || obj.object_name);
         dbms_output.put_line(replace(
            dbms_metadata.get_ddl(
               p_object_type,
               obj.object_name,
               p_dev_schema
            ),
            '"'
            || p_dev_schema
            || '"',
            '"'
            || p_prod_schema
            || '"'
         )
                              || '/');
      end loop;

        -- измененнные объекты - есть двух схемах, но DDL разл
      for obj in (
         select object_name
           from all_objects
          where owner = p_dev_schema
            and object_type = p_object_type
            and object_name in (
            select object_name
              from all_objects
             where owner = p_prod_schema
               and object_type = p_object_type
         )
      ) loop
         if objects_different(
            obj.object_name,
            p_object_type
         ) then
            dbms_output.put_line('-- Update '
                                 || p_object_type
                                 || ': '
                                 || obj.object_name);
            dbms_output.put_line(replace(
               dbms_metadata.get_ddl(
                  p_object_type,
                  obj.object_name,
                  p_dev_schema
               ),
               '"'
               || p_dev_schema
               || '"',
               '"'
               || p_prod_schema
               || '"'
            )
                                 || '/');
         end if;
      end loop;

        -- есть на проде и нет в деве
      for obj in (
         select object_name
           from all_objects
          where owner = p_prod_schema
            and object_type = p_object_type
            and object_name not in (
            select object_name
              from all_objects
             where owner = p_dev_schema
               and object_type = p_object_type
         )
      ) loop
         dbms_output.put_line('DROP '
                              || p_object_type
                              || ' '
                              || p_prod_schema
                              || '.'
                              || obj.object_name
                              || ';');
      end loop;
   end;

begin
   dbms_output.put_line('======================================================');
   dbms_output.put_line('======================================================');
   process_objects('PROCEDURE');
   process_objects('FUNCTION');
   dbms_output.put_line('======================================================');
   dbms_output.put_line('======================================================');
exception
   when others then
      dbms_output.put_line('Error generating script: ' || sqlerrm);
end;
/

begin
   generate_sync_script(
      'developer',
      'production'
   );
end;
/

create or replace procedure developer.executestepone (
   inputparam int
) is
begin
   dbms_output.put_line('Input value: ' || to_char(inputparam));
end;
/

create or replace procedure production.executestepone (
   input varchar
) is
begin
   dbms_output.put_line(input);
end;
/
create or replace procedure developer.performactiontwo (
   msg varchar2
) is
begin
   dbms_output.put_line(msg);
end;
/
create or replace procedure production.performactiontwo (
   msg varchar2
) is
begin
   dbms_output.put_line(msg);
   dbms_output.put_line(msg);
end;
/
create or replace procedure developer.handlestepthree (
   data varchar2
) is
begin
   dbms_output.put_line(data);
end;
/
create or replace procedure production.specialoperation (
   param varchar2
) is
begin
   dbms_output.put_line(param);
end;
/
create or replace procedure developer.testproc (
   data varchar2
) is
begin
   dbms_output.put_line(data);
end;
/
create or replace function developer.calculatevalue (
   inputdata varchar2
) return number is
begin
   dbms_output.put_line(inputdata);
   return 5;
end;
/

create table developer.testagain (
   test_id  number not null,
   test_str varchar2(59) not null
);
drop table developer.testagain;

create table developer.t1 (
   id number(10) primary key not null
);
create table developer.t2 (
   id number(10) primary key not null
);
create table developer.t3 (
   id number(10) primary key not null
);

drop table developer.t1 cascade constraints;
drop table developer.t3;
drop table developer.t2;

-- t1->t3->t2

alter table developer.t1 add c1 number(20)
   constraint tab1_c1_fk
      references developer.t3 ( id );
alter table developer.t3 add c1 number(20)
   constraint tab3_c1_fk
      references developer.t2 ( id )
/


commit;