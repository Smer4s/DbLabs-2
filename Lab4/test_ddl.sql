select table_name
  from user_tables
 where table_name = upper('test_table');
/

declare
   l_json clob := '{
       "queryType": "CREATE_TABLE",
       "tableName": "test_table",
       "fields": [
           {"name": "id", "type": "NUMBER", "constraints": "PRIMARY KEY"},
           {"name": "name", "type": "VARCHAR2(100)"}
       ]
    }';
begin
   exec_dynamic_ddl(p_json => l_json);
end;
/

declare
   l_json clob := '{
       "queryType": "DROP_TABLE",
       "tableName": "test_table"
    }';
begin
   exec_dynamic_ddl(p_json => l_json);
end;
/