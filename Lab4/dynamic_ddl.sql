create or replace procedure exec_dynamic_ddl (
   p_json in clob
) is
   l_query_type   varchar2(20);
   l_tablename    varchar2(100);
   l_fieldsclause varchar2(32767);
   l_query        varchar2(32767);
    
    
   l_pk_column    varchar2(100);
   l_seq_name     varchar2(100);
   l_trg_name     varchar2(100);
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
    
    
    
   if l_query_type = 'CREATE_TABLE' then
      begin
            
         select name
           into l_pk_column
           from (
            select name,
                   constraints
              from
               json_table ( p_json,'$.fields[*]'
                  columns (
                     name varchar2 ( 100 ) path '$.name',
                     constraints varchar2 ( 100 ) path '$.constraints'
                  )
               )
             where upper(constraints) like '%PRIMARY KEY%'
         )
          where rownum = 1;
            
            
         if l_pk_column is not null then
            l_seq_name := l_tablename || '_SEQ';
            l_trg_name := l_tablename || '_TRG_PK';
                
                
            execute immediate 'CREATE SEQUENCE '
                              || l_seq_name
                              || ' START WITH 1 INCREMENT BY 1 NOCACHE';
            dbms_output.put_line('Создана последовательность: ' || l_seq_name);
                
            declare
               l_trigger_sql varchar2(32767);
            begin
               l_trigger_sql := 'CREATE OR REPLACE TRIGGER '
                                || l_trg_name
                                || chr(10)
                                || 'BEFORE INSERT ON '
                                || l_tablename
                                || chr(10)
                                || 'FOR EACH ROW'
                                || chr(10)
                                || 'WHEN (new.'
                                || l_pk_column
                                || ' IS NULL)'
                                || chr(10)
                                || 'BEGIN'
                                || chr(10)
                                || '  SELECT '
                                || l_seq_name
                                || '.NEXTVAL INTO :new.'
                                || l_pk_column
                                || ' FROM dual;'
                                || chr(10)
                                || 'END;';

               execute immediate l_trigger_sql;
               dbms_output.put_line('Создан триггер для авто-генерации первичного ключа: ' || l_trg_name);
            end;
         end if;
      exception
         when no_data_found then
            dbms_output.put_line('Не найден первичный ключ в определении таблицы. Авто-триггер не создан.');
      end;
   end if;

exception
   when others then
      dbms_output.put_line('Ошибка выполнения DDL: ' || sqlerrm);
      raise;
end;
/