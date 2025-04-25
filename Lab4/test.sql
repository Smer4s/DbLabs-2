declare
   l_cursor sys_refcursor;
   l_id     number;
   l_name   varchar2(100);
   l_status varchar2(20);
   l_json   clob := '{
      "queryType": "SELECT",
      "columns": ["id", "name", "status"],
      "tables": ["employees"],
      "filters": {
         "conditions": [
            {"column": "status", "operator": "=", "value": "''ACTIVE''"}
         ]
      }
   }';
begin
   exec_dynamic_select(
      p_json   => l_json,
      p_result => l_cursor
   );
   
   loop
      fetch l_cursor into
         l_id,
         l_name,
         l_status;
      exit when l_cursor%notfound;
      dbms_output.put_line('id: '
                           || l_id
                           || ', name: '
                           || l_name
                           || ', status: '
                           || l_status);
   end loop;
   close l_cursor;
end;
/