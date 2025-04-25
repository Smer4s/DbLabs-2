drop table employees;

select *
  from my_table;

declare
   l_cursor sys_refcursor;
   l_id     number;
   l_name   varchar2(100);
   l_status varchar2(20);
   l_json   clob := '{
      "queryType": "SELECT",
      "columns": ["id", "name"],
      "tables": ["my_table"],
      "filters": {
         "conditions": [
            {"column": "id", "operator": "=", "value": "1"}
         ],
         "nested": {
            "type": "IN",
            "column": "id",
            "subquery": {
               "queryType": "SELECT",
               "columns": ["id"],
               "tables": ["my_table"],
               "filters": {
                  "conditions": [
                     {"column": "name", "operator": "LIKE", "value": "''A%''"},
                     {"column": "name", "operator": "LIKE", "value": "''A%''"}
                  ]
               }
            }
         }
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
         l_name;
      exit when l_cursor%notfound;
      dbms_output.put_line('id: '
                           || l_id
                           || ' | name: '
                           || l_name);
   end loop;
   close l_cursor;
end;
/

declare
   l_cursor          sys_refcursor;
   l_id              number;
   l_employee_name   varchar2(100);
   l_department_name varchar2(100);
   l_json            clob := '{
      "queryType": "SELECT",
      "columns": ["e.id", "e.name", "d.name AS department_name"],
      "tables": ["employees e", "departments d"],
      "joins": [
         {
            "table1": "employees",
            "column1": "department_id",
            "operator": "=",
            "table2": "departments",
            "column2": "id"
         }
      ]
   }';
begin
   exec_dynamic_select(
      p_json   => l_json,
      p_result => l_cursor
   );
   loop
      fetch l_cursor into
         l_id,
         l_employee_name,
         l_department_name;
      exit when l_cursor%notfound;
      dbms_output.put_line('ID: '
                           || l_id
                           || ' | Employee: '
                           || l_employee_name
                           || ' | Department: '
                           || l_department_name);
   end loop;
   close l_cursor;
end;
/