create or replace procedure sync_functions (
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
            upper(p_dev_schema)
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
            upper(p_prod_schema)
         )
           into v_prod_ddl
           from dual;
      exception
         when others then
            return true;
      end;

      v_prod_ddl := replace(v_prod_ddl, upper(p_prod_schema),'');
      v_dev_ddl := replace(v_dev_ddl, upper(p_dev_schema),'');

      return v_prod_ddl <> v_dev_ddl;
   end;

   procedure process_objects (
      p_object_type varchar2
   ) is
   begin
      dbms_output.put_line(lower(p_object_type)
                           || ' diff:');
        --новые объекты (есть в деве, нет в проде)
      for obj in (
         select object_name
           from all_objects
          where owner = upper(p_dev_schema)
            and object_type = p_object_type
            and object_name not in (
            select object_name
              from all_objects
             where owner = upper(p_prod_schema)
               and object_type = p_object_type
         )
      ) loop
         dbms_output.put_line(obj.object_name);
         dbms_output.put_line(replace(
            dbms_metadata.get_ddl(
               p_object_type,
               obj.object_name,
               upper(p_dev_schema)
            ),
            '"'
            || upper(p_dev_schema)
            || '"',
            '"'
            || upper(p_prod_schema)
            || '"'
         ));
      end loop;

        -- измененнные объекты - есть двух схемах, но DDL разл
      for obj in (
         select object_name
           from all_objects
          where owner = upper(p_dev_schema)
            and object_type = p_object_type
            and object_name in (
            select object_name
              from all_objects
             where owner = upper(p_prod_schema)
               and object_type = p_object_type
         )
      ) loop
         if objects_different(
            obj.object_name,
            p_object_type
         ) then
            dbms_output.put_line(obj.object_name || ': ');
            dbms_output.put_line(replace(
               dbms_metadata.get_ddl(
                  p_object_type,
                  obj.object_name,
                  upper(p_dev_schema)
               ),
               '"'
               || upper(p_dev_schema)
               || '"',
               '"'
               || upper(p_prod_schema)
               || '"'
            ));
         end if;
      end loop;

        -- есть на проде и нет в деве
      for obj in (
         select object_name
           from all_objects
          where owner = upper(p_prod_schema)
            and object_type = p_object_type
            and object_name not in (
            select object_name
              from all_objects
             where owner = upper(p_dev_schema)
               and object_type = p_object_type
         )
      ) loop
         dbms_output.put_line('DROP '
                              || p_object_type
                              || ' '
                              || upper(p_prod_schema)
                              || '.'
                              || obj.object_name
                              || ';');
      end loop;
   end;

begin
   process_objects('PROCEDURE');
   process_objects('FUNCTION');
exception
   when others then
      dbms_output.put_line('Error generating script: ' || sqlerrm);
end;
/

drop procedure sync_functions
/

begin
   sync_functions(
      'developer',
      'production'
   );
end;
/


begin
   for obj in (
      select dbms_metadata.get_ddl(
         'PROCEDURE',
         'TEST',
         'PRODUCTION'
      )
        from dual
   ) loop
      dbms_output.put_line(obj);
   end loop;
end;
/

select dbms_metadata.get_ddl(
   'PROCEDURE',
   'TEST',
   'DEVELOPER'
)
  from dual;
/

select dbms_metadata.get_ddl(
   'PROCEDURE',
   'TEST',
   'DEVELOP'
)
  from dual
/

begin
   for obj in (
      select object_name
        from all_objects
       where owner = 'DEVELOPER'
         and object_type = 'PROCEDURE'
         and object_name not in (
         select object_name
           from all_objects
          where owner = 'PRODUCTION'
            and object_type = 'PROCEDURE'
      )
   ) loop
      dbms_output.put_line(obj.object_name);
   end loop;
end;
/