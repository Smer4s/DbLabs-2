create or replace procedure exec_dynamic_dml (
   p_json in clob
) is
   l_query_type       varchar2(20);
   l_table            varchar2(100);
   l_query            varchar2(32767);
   l_filters          varchar2(32767) := '';
   l_conditions       varchar2(32767);
   l_nested_condition varchar2(32767);
   l_subquery_sql     varchar2(32767);
begin
   
   l_query_type := json_value(p_json,
           '$.queryType');
   l_table := json_value(p_json,
           '$.table');
   if l_query_type = 'INSERT' then
      declare
         l_cols_clause   varchar2(2000);
         l_values_clause varchar2(2000);
      begin
         
         select
            listagg(column_value,
                    ', ') within group(
             order by rn)
           into l_cols_clause
           from (
            select column_value,
                   rn
              from
               json_table ( p_json,'$.columns[*]'
                  columns (
                     rn for ordinality,
                     column_value varchar2 ( 100 ) path '$'
                  )
               )
         );

         
         select
            listagg(column_value,
                    ', ') within group(
             order by rn)
           into l_values_clause
           from (
            select column_value,
                   rn
              from
               json_table ( p_json,'$.values[*]'
                  columns (
                     rn for ordinality,
                     column_value varchar2 ( 100 ) path '$'
                  )
               )
         );

         l_query := 'INSERT INTO '
                    || l_table
                    || ' ('
                    || l_cols_clause
                    || ') VALUES ('
                    || l_values_clause
                    || ')';
      end;
   elsif l_query_type = 'UPDATE' then
      declare
         l_set_clause varchar2(32767);
      begin
         
         select
            listagg(set_expr,
                    ', ') within group(
             order by rn)
           into l_set_clause
           from (
            select ( col_val
                     || ' = '
                     || to_char(val) ) as set_expr,
                   rn
              from
               json_table ( p_json,'$.set[*]'
                  columns (
                     rn for ordinality,
                     col_val varchar2 ( 100 ) path '$.column',
                     val varchar2 ( 100 ) path '$.value'
                  )
               )
         );
         l_query := 'UPDATE '
                    || l_table
                    || ' SET '
                    || l_set_clause;
      end;
   elsif l_query_type = 'DELETE' then
      l_query := 'DELETE FROM ' || l_table;
   else
      raise_application_error(
         -20010,
         'Unsupported DML type: ' || l_query_type
      );
   end if;
   
   
   
   if json_exists(
      p_json,
      '$.filters'
   ) then
      declare
         l_cond            varchar2(32767);
         l_nested_type     varchar2(20);
         l_nested_column   varchar2(100);
         l_nested_subquery clob;
         l_sq_cols         varchar2(2000);
         l_sq_tables       varchar2(2000);
         l_sq_conditions   varchar2(2000);
      begin
         
         begin
            select
               listagg(condition_str,
                       ' AND ') within group(
                order by rn)
              into l_cond
              from (
               select ( col_field
                        || ' '
                        || operator
                        || ' '
                        || to_char(value) ) as condition_str,
                      rn
                 from
                  json_table ( p_json,'$.filters.conditions[*]'
                     columns (
                        rn for ordinality,
                        col_field varchar2 ( 100 ) path '$.column',
                        operator varchar2 ( 10 ) path '$.operator',
                        value varchar2 ( 100 ) path '$.value'
                     )
                  )
            );
         exception
            when no_data_found or value_error then
               l_cond := null;
         end;
         
         
         if json_exists(
            p_json,
            '$.filters.nested'
         ) then
            l_nested_type := json_value(p_json,
           '$.filters.nested.type');
            if l_nested_type in ( 'IN',
                                  'NOT IN' ) then
               l_nested_column := json_value(p_json,
           '$.filters.nested.column');
               if l_nested_column is null then
                  raise_application_error(
                     -20002,
                     'Для вложенного запроса типа '
                     || l_nested_type
                     || ' требуется указание "column".'
                  );
               end if;
            end if;
            l_nested_subquery := json_query(p_json,
           '$.filters.nested.subquery' returning clob);
            
            
            declare
               l_sq_cols_local       varchar2(2000);
               l_sq_tables_local     varchar2(2000);
               l_sq_conditions_local varchar2(2000);
            begin
               select
                  listagg(col,
                          ', ') within group(
                   order by rn)
                 into l_sq_cols_local
                 from (
                  select column_value as col,
                         rn
                    from
                     json_table ( l_nested_subquery,'$.columns[*]'
                        columns (
                           rn for ordinality,
                           column_value varchar2 ( 100 ) path '$'
                        )
                     )
               );

               select
                  listagg(tbl,
                          ', ') within group(
                   order by rn)
                 into l_sq_tables_local
                 from (
                  select column_value as tbl,
                         rn
                    from
                     json_table ( l_nested_subquery,'$.tables[*]'
                        columns (
                           rn for ordinality,
                           column_value varchar2 ( 100 ) path '$'
                        )
                     )
               );

               begin
                  select
                     listagg(condition_str,
                             ' AND ') within group(
                      order by rn)
                    into l_sq_conditions_local
                    from (
                     select ( col_field
                              || ' '
                              || operator
                              || ' '
                              || to_char(value) ) as condition_str,
                            rn
                       from
                        json_table ( l_nested_subquery,'$.filters.conditions[*]'
                           columns (
                              rn for ordinality,
                              col_field varchar2 ( 100 ) path '$.column',
                              operator varchar2 ( 10 ) path '$.operator',
                              value varchar2 ( 100 ) path '$.value'
                           )
                        )
                  );
               exception
                  when no_data_found or value_error then
                     l_sq_conditions_local := null;
               end;

               l_subquery_sql := 'SELECT '
                                 || l_sq_cols_local
                                 || ' FROM '
                                 || l_sq_tables_local;
               if l_sq_conditions_local is not null then
                  l_subquery_sql := l_subquery_sql
                                    || ' WHERE '
                                    || l_sq_conditions_local;
               end if;
            end;
            
            
            if l_nested_type in ( 'IN',
                                  'NOT IN' ) then
               l_nested_condition := l_nested_column
                                     || ' '
                                     || l_nested_type
                                     || ' ('
                                     || l_subquery_sql
                                     || ')';
            elsif l_nested_type in ( 'EXISTS',
                                     'NOT EXISTS' ) then
               l_nested_condition := l_nested_type
                                     || ' ('
                                     || l_subquery_sql
                                     || ')';
            else
               raise_application_error(
                  -20003,
                  'Не поддерживаемый тип вложенного запроса: ' || l_nested_type
               );
            end if;

            if l_cond is not null then
               l_cond := l_cond
                         || ' AND '
                         || l_nested_condition;
            else
               l_cond := l_nested_condition;
            end if;
         end if;

         if l_cond is not null then
            l_conditions := l_cond;
         end if;
      end;
   end if;
   
   
   if l_conditions is not null then
      l_query := l_query
                 || ' WHERE '
                 || l_conditions;
   end if;

   dbms_output.put_line('Сформированный DML запрос: ' || l_query);
   
   
   execute immediate l_query;
   commit;
exception
   when others then
      rollback;
      raise;
end;
/