select * from employees;


declare
   l_json clob := '{
      "queryType": "INSERT",
      "table": "employees",
      "columns": ["id", "name", "status"],
      "values": ["4", "''Dave''", "''ACTIVE''"]
   }';
begin
   exec_dynamic_dml(p_json => l_json);
   dbms_output.put_line('INSERT выполнен успешно.');
end;
/

declare
   l_json clob := '{
      "queryType": "UPDATE",
      "table": "employees",
      "set": [
         {"column": "name", "value": "''Dave Updated''"},
         {"column": "status", "value": "''ACTIVE''"}
      ],
      "filters": {
         "conditions": [
            {"column": "id", "operator": "=", "value": "4"}
         ]
      }
   }';
begin
   exec_dynamic_dml(p_json => l_json);
   dbms_output.put_line('UPDATE выполнен успешно.');
end;
/

declare
   l_json clob := '{
      "queryType": "DELETE",
      "table": "employees",
      "filters": {
         "nested": {
            "type": "EXISTS",
            "subquery": {
               "queryType": "SELECT",
               "columns": ["id"],
               "tables": ["employees"],
               "filters": {
                  "conditions": [
                     {"column": "name", "operator": "LIKE", "value": "''D%''"}
                  ]
               }
            }
         }
      }
   }';
begin
   exec_dynamic_dml(p_json => l_json);
   dbms_output.put_line('DELETE выполнен успешно.');
end;
/
