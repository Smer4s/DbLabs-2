create or replace procedure exec_dynamic_ddl (
   p_json in clob
) is
   l_query_type   varchar2(20);
   l_tablename    varchar2(100);
   l_fieldsclause varchar2(32767);
   l_query        varchar2(32767);
begin
    
   l_query_type := json_value(p_json,
           '$.queryType');
   if l_query_type = 'CREATE_TABLE' then
      l_tablename := json_value(p_json,
           '$.tableName');
        
      select
         listagg(field_clause,
                 ', ') within group(
          order by rn)
        into l_fieldsclause
        from (
         select ( name
                  || ' '
                  || type
                  ||
                  case
                     when constraints is not null then
                        ' ' || constraints
                     else
                        ''
                  end
         ) as field_clause,
                rn
           from
            json_table ( p_json,'$.fields[*]'
               columns (
                  rn for ordinality,
                  name varchar2 ( 100 ) path '$.name',
                  type varchar2 ( 100 ) path '$.type',
                  constraints varchar2 ( 100 ) path '$.constraints'
               )
            )
      );
        
        
      l_query := 'CREATE TABLE '
                 || l_tablename
                 || ' ('
                 || l_fieldsclause
                 || ')';
   elsif l_query_type = 'DROP_TABLE' then
      l_tablename := json_value(p_json,
           '$.tableName');
      l_query := 'DROP TABLE ' || l_tablename;
   else
      raise_application_error(
         -20011,
         'Unsupported DDL query type: ' || l_query_type
      );
   end if;

   dbms_output.put_line('Сформированный DDL запрос: ' || l_query);
   execute immediate l_query;
   dbms_output.put_line('DDL запрос выполнен успешно.');
exception
   when others then
      dbms_output.put_line('Ошибка выполнения DDL: ' || sqlerrm);
      raise;
end;
/