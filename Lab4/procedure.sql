create or replace procedure exec_dynamic_select (
   p_json   in clob,
   p_result out sys_refcursor
) is
   l_query      varchar2(32767);
   l_query_type varchar2(20);
   l_cols       varchar2(2000);
   l_tables     varchar2(2000);
   l_conditions varchar2(2000);
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