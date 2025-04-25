create or replace procedure exec_dynamic_select (
   p_json   in clob,
   p_result out sys_refcursor
) is
   l_query            varchar2(32767);
   l_query_type       varchar2(20);
   l_cols             varchar2(2000);
   l_tables           varchar2(2000);
   l_conditions       varchar2(2000);
   l_nested_condition varchar2(2000);
   l_nested_type      varchar2(20);
   l_nested_column    varchar2(100);
   l_nested_subquery  clob;
   l_subquery_sql     varchar2(32767);
begin
   l_query_type := json_value(p_json,
           '$.queryType');
   if l_query_type <> 'SELECT' then
      raise_application_error(
         -20001,
         'Unsupported query type: ' || l_query_type
      );
   end if;

   select
      listagg(col,
              ', ') within group(
       order by rn)
     into l_cols
     from (
      select column_value as col,
             row_number()
             over(
                 order by column_value
             ) rn
        from
         json_table ( p_json,'$.columns[*]'
            columns (
               column_value varchar2 ( 100 ) path '$'
            )
         )
   );

   select
      listagg(tbl,
              ', ') within group(
       order by rn)
     into l_tables
     from (
      select column_value as tbl,
             row_number()
             over(
                 order by column_value
             ) rn
        from
         json_table ( p_json,'$.tables[*]'
            columns (
               column_value varchar2 ( 100 ) path '$'
            )
         )
   );

   begin
      select
         listagg(condition_str,
                 ' AND ') within group(
          order by rn)
        into l_conditions
        from (
         select ( col_field
                  || ' '
                  || operator
                  || ' '
                  || value ) as condition_str,
                row_number()
                over(
                    order by col_field
                ) rn
           from
            json_table ( p_json,'$.filters.conditions[*]'
               columns (
                  col_field varchar2 ( 100 ) path '$.column',
                  operator varchar2 ( 10 ) path '$.operator',
                  value varchar2 ( 100 ) path '$.value'
               )
            )
      );
   exception
      when no_data_found or value_error then
         l_conditions := null;
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
               || ' требуется указать "column".'
            );
         end if;
      end if;

      l_nested_subquery := json_query(p_json,
           '$.filters.nested.subquery' returning clob);
      declare
         l_sq_cols       varchar2(2000);
         l_sq_tables     varchar2(2000);
         l_sq_conditions varchar2(2000);
      begin
         select
            listagg(col,
                    ', ') within group(
             order by rn)
           into l_sq_cols
           from (
            select column_value as col,
                   row_number()
                   over(
                       order by column_value
                   ) rn
              from
               json_table ( l_nested_subquery,'$.columns[*]'
                  columns (
                     column_value varchar2 ( 100 ) path '$'
                  )
               )
         );

         select
            listagg(tbl,
                    ', ') within group(
             order by rn)
           into l_sq_tables
           from (
            select column_value as tbl,
                   row_number()
                   over(
                       order by column_value
                   ) rn
              from
               json_table ( l_nested_subquery,'$.tables[*]'
                  columns (
                     column_value varchar2 ( 100 ) path '$'
                  )
               )
         );

         begin
            select
               listagg(condition_str,
                       ' AND ') within group(
                order by rn)
              into l_sq_conditions
              from (
               select ( col_field
                        || ' '
                        || operator
                        || ' '
                        || value ) as condition_str,
                      row_number()
                      over(
                          order by col_field
                      ) rn
                 from
                  json_table ( l_nested_subquery,'$.filters.conditions[*]'
                     columns (
                        col_field varchar2 ( 100 ) path '$.column',
                        operator varchar2 ( 10 ) path '$.operator',
                        value varchar2 ( 100 ) path '$.value'
                     )
                  )
            );
         exception
            when no_data_found or value_error then
               l_sq_conditions := null;
         end;

         l_subquery_sql := 'SELECT '
                           || l_sq_cols
                           || ' FROM '
                           || l_sq_tables;
         if l_sq_conditions is not null then
            l_subquery_sql := l_subquery_sql
                              || ' WHERE '
                              || l_sq_conditions;
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

      if l_conditions is not null then
         l_conditions := l_conditions
                         || ' AND '
                         || l_nested_condition;
      else
         l_conditions := l_nested_condition;
      end if;
   end if;

   l_query := 'SELECT '
              || l_cols
              || ' FROM '
              || l_tables;
   if l_conditions is not null then
      l_query := l_query
                 || ' WHERE '
                 || l_conditions;
   end if;

   dbms_output.put_line('Сформированный запрос: ' || l_query);
   open p_result for l_query;
end;
/