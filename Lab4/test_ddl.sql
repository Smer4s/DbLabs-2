select table_name
  from user_tables
 where table_name = upper('test_table');
/

insert into test_table ( name ) values ( 'TEST' );

select user
  from dual;
grant
   create table
to system;
grant
   create sequence
to system;
grant
   create trigger
to system;

select *
  from test_table;

declare
   l_json clob := '{
       "queryType": "CREATE_TABLE",
       "tableName": "test_table_1",
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
       "tableName": "test_table_1"
    }';
begin
   exec_dynamic_ddl(p_json => l_json);
end;
/