create or replace type dep_rec as object (
      table_name varchar2(128),
      depends_on varchar2(128)
);
/

create or replace type dep_tab as
   table of dep_rec;
/

create or replace procedure sync_schema_structures (
   p_source_schema in varchar2,
   p_target_schema in varchar2
) as

   cnt_src_schema number;
   cnt_tgt_schema number;
   ddl_script     clob;
   hascycle       boolean := false;
   fkdeps         dep_tab := dep_tab();
   type t_obj_rec is record (
         tbl_name varchar2(128),
         cyclic   boolean
   );
   type t_obj_table is
      table of t_obj_rec;
   sortedobjs     t_obj_table := t_obj_table();
   cursor cur_missing_diff is
   select table_name
     from (
      select table_name
        from all_tables
       where owner = upper(p_source_schema)
      minus
      select table_name
        from all_tables
       where owner = upper(p_target_schema)
   )
   union
   select table_name
     from (
      select table_name,
             count(*) as num_cols,
             listagg(column_name
                     || ':'
                     || data_type,
                     ',') within group(
              order by column_name) as structure
        from all_tab_columns
       where owner = upper(p_source_schema)
       group by table_name
      minus
      select table_name,
             count(*) as num_cols,
             listagg(column_name
                     || ':'
                     || data_type,
                     ',') within group(
              order by column_name) as structure
        from all_tab_columns
       where owner = upper(p_target_schema)
       group by table_name
   );




   cursor cur_extra_objs is
   select table_name
     from (
      select table_name
        from all_tables
       where owner = upper(p_target_schema)
      minus
      select table_name
        from all_tables
       where owner = upper(p_source_schema)
   );




   procedure perform_topology_sort is

      type t_bool_map is
         table of boolean index by varchar2(128);
      visited    t_bool_map;
      tempmarks  t_bool_map;
      addednodes t_bool_map;
      localorder t_obj_table := t_obj_table();


      procedure visit_node (
         p_tbl_name varchar2
      ) is
         localcycle boolean := false;
      begin
         if tempmarks.exists(p_tbl_name) then
            hascycle := true;
            localcycle := true;
            if not addednodes.exists(p_tbl_name) then
               localorder.extend;
               localorder(localorder.last) := t_obj_rec(
                  p_tbl_name,
                  localcycle
               );
               addednodes(p_tbl_name) := true;
            end if;
            return;
         end if;

         if not visited.exists(p_tbl_name) then
            tempmarks(p_tbl_name) := true;
            for idx in 1..fkdeps.count loop
               if fkdeps(idx).table_name = p_tbl_name then
                  visit_node(fkdeps(idx).depends_on);
               end if;
            end loop;
            visited(p_tbl_name) := true;
            tempmarks.delete(p_tbl_name);
            if not addednodes.exists(p_tbl_name) then
               localorder.extend;
               localorder(localorder.last) := t_obj_rec(
                  p_tbl_name,
                  localcycle
               );
               addednodes(p_tbl_name) := true;
            end if;
         end if;
      end visit_node;

   begin
      for rec in cur_missing_diff loop
         if not visited.exists(rec.table_name) then
            visit_node(rec.table_name);
         end if;
      end loop;


      sortedobjs := localorder;
      if hascycle then
         dbms_output.put_line('### ВАЖНО: Обнаружены циклические зависимости между таблицами! ###');
      else
         dbms_output.put_line('### Сортировка зависимостей завершена успешно ###');
      end if;
   end perform_topology_sort;




   procedure refine_order is

      type t_sort_info is record (
            name       varchar2(128),
            cycle_flag boolean,
            depcount   number
      );
      type t_sort_array is
         table of t_sort_info index by pls_integer;
      sortdata  t_sort_array;
      temprec   t_sort_info;
      totalobjs pls_integer := sortedobjs.count;


      function has_dep (
         pa varchar2,
         pb varchar2
      ) return boolean is
      begin
         for k in 1..fkdeps.count loop
            if
               fkdeps(k).table_name = pa
               and fkdeps(k).depends_on = pb
            then
               return true;
            end if;
         end loop;
         return false;
      end;

   begin
      for i in 1..totalobjs loop
         sortdata(i).name := sortedobjs(i).tbl_name;
         sortdata(i).cycle_flag := sortedobjs(i).cyclic;
         sortdata(i).depcount := 0;
         for j in 1..fkdeps.count loop
            if fkdeps(j).table_name = sortedobjs(i).tbl_name then
               sortdata(i).depcount := sortdata(i).depcount + 1;
            end if;
         end loop;
      end loop;


      for i in 1..totalobjs - 1 loop
         for j in i + 1..totalobjs loop
            if sortdata(i).cycle_flag = sortdata(j).cycle_flag then
               if not sortdata(i).cycle_flag then
                  if sortdata(i).depcount > sortdata(j).depcount then
                     temprec := sortdata(i);
                     sortdata(i) := sortdata(j);
                     sortdata(j) := temprec;
                  elsif sortdata(i).depcount = sortdata(j).depcount then
                     if has_dep(
                        sortdata(i).name,
                        sortdata(j).name
                     ) then
                        temprec := sortdata(i);
                        sortdata(i) := sortdata(j);
                        sortdata(j) := temprec;
                     end if;
                  end if;
               else
                  if sortdata(i).depcount < sortdata(j).depcount then
                     temprec := sortdata(i);
                     sortdata(i) := sortdata(j);
                     sortdata(j) := temprec;
                  end if;
               end if;
            elsif
               sortdata(i).cycle_flag
               and ( not sortdata(j).cycle_flag )
            then
               temprec := sortdata(i);
               sortdata(i) := sortdata(j);
               sortdata(j) := temprec;
            end if;
         end loop;
      end loop;


      sortedobjs.delete;
      for i in 1..totalobjs loop
         sortedobjs.extend;
         sortedobjs(sortedobjs.last) := t_obj_rec(
            sortdata(i).name,
            sortdata(i).cycle_flag
         );
      end loop;
   end refine_order;

begin
   select count(*)
     into cnt_src_schema
     from all_users
    where username = upper(p_source_schema);
   if cnt_src_schema = 0 then
      raise_application_error(
         -20001,
         'Схема-источник '
         || p_source_schema
         || ' не найдена.'
      );
   end if;

   select count(*)
     into cnt_tgt_schema
     from all_users
    where username = upper(p_target_schema);
   if cnt_tgt_schema = 0 then
      raise_application_error(
         -20002,
         'Целевая схема '
         || p_target_schema
         || ' не найдена.'
      );
   end if;


   select dep_rec(
      table_name,
      referenced_table_name
   )
   bulk collect
     into fkdeps
     from (
      select distinct ac.table_name,
                      ac2.table_name as referenced_table_name
        from all_constraints ac
        join all_cons_columns acc
      on ac.constraint_name = acc.constraint_name
         and ac.owner = acc.owner
        join all_constraints ac2
      on ac.r_constraint_name = ac2.constraint_name
         and ac2.owner = ac.owner
       where ac.owner = upper(p_source_schema)
         and ac.constraint_type = 'R'
   );

   dbms_output.put_line(chr(10)
                        || '<<< Таблицы для обновления/создания в схеме '
                        || upper(p_target_schema)
                        || ' >>>');
   for rec in cur_missing_diff loop
      dbms_output.put_line('Обнаружена: ' || rec.table_name);
   end loop;


   perform_topology_sort;
   refine_order;
   dbms_output.put_line(chr(10)
                        || '<<< Генерация DDL-скриптов >>>');
   for idx in 1..sortedobjs.count loop
      dbms_output.put_line('### Обрабатываем таблицу: '
                           || sortedobjs(idx).tbl_name
                           || case
         when sortedobjs(idx).cyclic then
            ' [имеет циклическую зависимость]'
         else ''
      end);
      dbms_output.put_line('!! Выполняется удаление: DROP TABLE "'
                           || upper(p_target_schema)
                           || '"."'
                           || sortedobjs(idx).tbl_name
                           || '";');

      ddl_script := 'CREATE TABLE "'
                    || upper(p_target_schema)
                    || '"."'
                    || sortedobjs(idx).tbl_name
                    || '" ('
                    || chr(10);
      for colrec in (
         select column_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable
           from all_tab_columns
          where owner = upper(p_source_schema)
            and table_name = sortedobjs(idx).tbl_name
          order by column_id
      ) loop
         ddl_script := ddl_script
                       || '    "'
                       || colrec.column_name
                       || '" '
                       || colrec.data_type;
         if colrec.data_type in ( 'VARCHAR2',
                                  'CHAR' ) then
            ddl_script := ddl_script
                          || '('
                          || colrec.data_length
                          || ')';
         elsif
            colrec.data_type = 'NUMBER'
            and colrec.data_precision is not null
         then
            ddl_script := ddl_script
                          || '('
                          || colrec.data_precision
                          || ','
                          || nvl(
               colrec.data_scale,
               0
            )
                          || ')';
         end if;
         if colrec.nullable = 'N' then
            ddl_script := ddl_script || ' NOT NULL';
         end if;
         ddl_script := ddl_script
                       || ','
                       || chr(10);
      end loop;


      for consrec in (
         select constraint_name,
                constraint_type,
                r_owner,
                r_constraint_name
           from all_constraints
          where owner = upper(p_source_schema)
            and table_name = sortedobjs(idx).tbl_name
            and constraint_type in ( 'P',
                                     'R' )
          order by constraint_type desc
      ) loop
         ddl_script := ddl_script
                       || '    CONSTRAINT "'
                       || consrec.constraint_name
                       || '" ';
         if consrec.constraint_type = 'P' then
            ddl_script := ddl_script || 'PRIMARY KEY (';
            for colcons in (
               select column_name
                 from all_cons_columns
                where owner = upper(p_source_schema)
                  and constraint_name = consrec.constraint_name
                order by position
            ) loop
               ddl_script := ddl_script
                             || '"'
                             || colcons.column_name
                             || '",';
            end loop;
            ddl_script := rtrim(
               ddl_script,
               ','
            )
                          || ')';
         elsif consrec.constraint_type = 'R' then
            ddl_script := ddl_script || 'FOREIGN KEY (';
            for colcons in (
               select column_name
                 from all_cons_columns
                where owner = upper(p_source_schema)
                  and constraint_name = consrec.constraint_name
                order by position
            ) loop
               ddl_script := ddl_script
                             || '"'
                             || colcons.column_name
                             || '",';
            end loop;
            ddl_script := rtrim(
               ddl_script,
               ','
            )
                          || ') REFERENCES "'
                          || upper(p_target_schema)
                          || '"."';
            declare
               ref_tbl varchar2(128);
            begin
               select table_name
                 into ref_tbl
                 from all_constraints
                where owner = consrec.r_owner
                  and constraint_name = consrec.r_constraint_name
                  and rownum = 1;
               ddl_script := ddl_script
                             || ref_tbl
                             || '" (';
               for refcol in (
                  select column_name
                    from all_cons_columns
                   where owner = consrec.r_owner
                     and constraint_name = consrec.r_constraint_name
                   order by position
               ) loop
                  ddl_script := ddl_script
                                || '"'
                                || refcol.column_name
                                || '",';
               end loop;
               ddl_script := rtrim(
                  ddl_script,
                  ','
               )
                             || ')';
            exception
               when no_data_found then
                  null;
            end;
         end if;
         ddl_script := ddl_script
                       || ','
                       || chr(10);
      end loop;

      ddl_script := rtrim(
         ddl_script,
         ',' || chr(10)
      )
                    || chr(10)
                    || ');';
      dbms_output.put_line(ddl_script);
      dbms_output.put_line('/');
   end loop;

   dbms_output.put_line(chr(10)
                        || '<<< Скрипты на удаление устаревших таблиц >>>');
   for recextra in cur_extra_objs loop
      dbms_output.put_line('!! Удалить: DROP TABLE "'
                           || upper(p_target_schema)
                           || '"."'
                           || recextra.table_name
                           || '";');
   end loop;

   dbms_output.put_line(chr(10)
                        || '### Синхронизация схем завершена ###');
exception
   when others then
      dbms_output.put_line('*** Ошибка: ' || sqlerrm);
      raise;
end sync_schema_structures;
/
commit;


begin
   sync_schema_structures(
      'developer',
      'production'
   );
end;
/