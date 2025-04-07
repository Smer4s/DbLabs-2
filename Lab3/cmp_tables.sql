create or replace type dep_rec as object (
      table_name varchar2(128),
      depends_on varchar2(128)
);
/

create or replace type dep_tab as
   table of dep_rec;
/

create or replace procedure sync_database (
   dev_schema_name  in varchar2,
   prod_schema_name in varchar2
) as
   v_count          number;
   v_cycle_detected boolean := false;
   v_ddl            clob;
   v_dependencies   dep_tab := dep_tab();
   type table_rec is record (
         object_name varchar2(128),
         has_cycle   boolean
   );
   type table_tab is
      table of table_rec;
   v_sorted_tables  table_tab := table_tab();

--все таблицы которые нужно создать или обновить в проде
   cursor object_diff_to_prod is
   select t.table_name as object_name
     from all_tables t
    where t.owner = upper(dev_schema_name)
   minus
   select t2.table_name
     from all_tables t2
    where t2.owner = upper(prod_schema_name)
   union
        --сравнение структуры таблиц 
   select tc1.table_name
     from (
      select table_name,
             count(column_name) as col_count,
             listagg(column_name
                     || ':'
                     || data_type,
                     ',') within group(
              order by column_name) as structure
        from all_tab_columns
       where owner = upper(dev_schema_name)
       group by table_name
      minus
      select table_name,
             count(column_name) as col_count,
             listagg(column_name
                     || ':'
                     || data_type,
                     ',') within group(
              order by column_name) as structure
        from all_tab_columns
       where owner = upper(prod_schema_name)
       group by table_name
   ) tc1;

-- все таблицы, которые есть в проде и нет в леве
   cursor object_diff_to_drop is
   select t.table_name as object_name
     from all_tables t
    where t.owner = upper(prod_schema_name)
   minus
   select t2.table_name
     from all_tables t2
    where t2.owner = upper(dev_schema_name);

   procedure topological_sort is
      type visited_tab is
         table of boolean index by varchar2(128);
      v_visited   visited_tab;
      v_temp_mark visited_tab;
      type added_tab is
         table of boolean index by varchar2(128);
      v_added     added_tab;
      v_tables    table_tab := table_tab();

      procedure visit (
         p_table_name in varchar2
      ) is
         v_has_cycle boolean := false;
      begin
         if v_temp_mark.exists(p_table_name) then
            v_cycle_detected := true;
            v_has_cycle := true;
            if not v_added.exists(p_table_name) then
               v_tables.extend;
               v_tables(v_tables.last) := table_rec(
                  p_table_name,
                  v_has_cycle
               );
               v_added(p_table_name) := true;
            end if;
            return;
         end if;
         if not v_visited.exists(p_table_name) then
            v_temp_mark(p_table_name) := true;
                -- если p_table_name имеет завис от др таблицы – вызываем visit ей
            for i in 1..v_dependencies.count loop
               if v_dependencies(i).table_name = p_table_name then
                  visit(v_dependencies(i).depends_on);
               end if;
            end loop;
            v_visited(p_table_name) := true;
            v_temp_mark.delete(p_table_name);
            if not v_added.exists(p_table_name) then
               v_tables.extend;
               v_tables(v_tables.last) := table_rec(
                  p_table_name,
                  v_has_cycle
               );
               v_added(p_table_name) := true;
            end if;
         end if;
      end visit;
   begin
      for rec in object_diff_to_prod loop
         if not v_visited.exists(rec.object_name) then
            visit(rec.object_name);
         end if;
      end loop;
      v_sorted_tables := v_tables;
      if v_cycle_detected then
         dbms_output.put_line('Cyclic reference!');
      end if;
   end topological_sort;

   procedure sort_tables is
      type sort_rec is record (
            object_name varchar2(128),
            has_cycle   boolean,
            dep_count   number
      );
      type sort_tab is
         table of sort_rec index by pls_integer;
      v_sort sort_tab;
      v_temp sort_rec;
      n      pls_integer := v_sorted_tables.count;
        
        -- проверка завис ли табл а от б
      function has_dependency (
         a varchar2,
         b varchar2
      ) return boolean is
      begin
         for i in 1..v_dependencies.count loop
            if
               v_dependencies(i).table_name = a
               and v_dependencies(i).depends_on = b
            then
               return true;
            end if;
         end loop;
         return false;
      end;

   begin
        -- заполн массива v_sort
      for i in 1..n loop
         v_sort(i).object_name := v_sorted_tables(i).object_name;
         v_sort(i).has_cycle := v_sorted_tables(i).has_cycle;
         v_sort(i).dep_count := 0;
         for j in 1..v_dependencies.count loop
            if v_dependencies(j).table_name = v_sorted_tables(i).object_name then
               v_sort(i).dep_count := v_sort(i).dep_count + 1; -- подсчет кол-ва завис для кажд таблицы
            end if;
         end loop;
      end loop;

      for i in 1..n - 1 loop
         for j in i + 1..n loop
            if v_sort(i).has_cycle = v_sort(j).has_cycle then -- чекнем одиаковый ли статус с циклами
               if v_sort(i).has_cycle = false then --если без цикла то по зависимстям
                  if v_sort(i).dep_count > v_sort(j).dep_count then
                     v_temp := v_sort(i);
                     v_sort(i) := v_sort(j);
                     v_sort(j) := v_temp;
                  elsif v_sort(i).dep_count = v_sort(j).dep_count then
                     if has_dependency(
                        v_sort(i).object_name,
                        v_sort(j).object_name
                     ) then  -- если табл v_sort(i) завис от v_sort(j) то v_sort(j) будет раньше
                        v_temp := v_sort(i);
                        v_sort(i) := v_sort(j);
                        v_sort(j) := v_temp;
                     end if;
                  end if;
               else
                  if v_sort(i).dep_count < v_sort(j).dep_count then -- по убыванию в случае циклов
                     v_temp := v_sort(i);
                     v_sort(i) := v_sort(j);
                     v_sort(j) := v_temp;
                  end if;
               end if;
            elsif
               v_sort(i).has_cycle = true
               and v_sort(j).has_cycle = false
            then --то табл без цикла раньше чем с
               v_temp := v_sort(i);
               v_sort(i) := v_sort(j);
               v_sort(j) := v_temp;
            end if;
         end loop;
      end loop;

      v_sorted_tables.delete;
      for i in 1..n loop
         v_sorted_tables.extend;
         v_sorted_tables(v_sorted_tables.last) := table_rec(
            v_sort(i).object_name,
            v_sort(i).has_cycle
         );
      end loop;
   end sort_tables;


begin
   select count(*)
     into v_count
     from all_users
    where username = upper(dev_schema_name);
   if v_count = 0 then
      raise_application_error(
         -20001,
         'Schema: '
         || dev_schema_name
         || ' is missing!'
      );
   end if;

   select count(*)
     into v_count
     from all_users
    where username = upper(prod_schema_name);
   if v_count = 0 then
      raise_application_error(
         -20002,
         'Schema: '
         || prod_schema_name
         || ' is missing!'
      );
   end if;
    
    -- сбор зависимостей по внеш ключам
   select dep_rec(
      table_name,
      referenced_table_name
   )
   bulk collect
     into v_dependencies
     from (
      select distinct ac.table_name, -- табл которая содер внеш ключ
                      ac2.table_name as referenced_table_name -- имя табл на которую ссылается внешний ключ
        from all_constraints ac
        join all_cons_columns acc
      on ac.constraint_name = acc.constraint_name
         and ac.owner = acc.owner
        --ac.r_constraint_name — внеш ключ в одной таблице (имя родительского ограничения на кот ссылается текущ огран)
        --ac2.constraint_name — первичный ключ в другой таблице
        join all_constraints ac2
      on ac.r_constraint_name = ac2.constraint_name
         and ac2.owner = ac.owner
       where ac.owner = upper(dev_schema_name)
         and ac.constraint_type = 'R'
   ); -- информация, на что ссылается внешн ключ в текущ табл

   dbms_output.put_line('Diff/Prod difference:');
   for rec in object_diff_to_prod loop
      dbms_output.put_line('# TABLE ' || rec.object_name);
   end loop;

   topological_sort;
   sort_tables;
    
    -- генерим DDL для таблиц
   for i in 1..v_sorted_tables.count loop
      if v_sorted_tables(i).has_cycle then
         dbms_output.put_line('Cyclic Reference:' || v_sorted_tables(i).object_name);
      end if;
      dbms_output.put_line('DROP TABLE "'
                           || upper(prod_schema_name)
                           || '"."'
                           || v_sorted_tables(i).object_name
                           || '";');

      v_ddl := 'CREATE TABLE "'
               || upper(prod_schema_name)
               || '"."'
               || v_sorted_tables(i).object_name
               || '" ('
               || chr(10);
      for col in (
         select column_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable
           from all_tab_columns
          where owner = upper(dev_schema_name)
            and table_name = v_sorted_tables(i).object_name
          order by column_id
      ) loop
         v_ddl := v_ddl
                  || '    "'
                  || col.column_name
                  || '" '
                  || col.data_type;
         if col.data_type in ( 'VARCHAR2',
                               'CHAR' ) then
            v_ddl := v_ddl
                     || '('
                     || col.data_length
                     || ')';
         elsif
            col.data_type = 'NUMBER'
            and col.data_precision is not null
         then
            v_ddl := v_ddl
                     || '('
                     || col.data_precision
                     || ','
                     || nvl(
               col.data_scale,
               0
            )
                     || ')';
         end if;
         if col.nullable = 'N' then
            v_ddl := v_ddl || ' NOT NULL';
         end if;
         v_ddl := v_ddl
                  || ','
                  || chr(10);
      end loop;
        -- генерация ограничений (pk fk)
      for cons in (
         select constraint_name,
                constraint_type,
                r_owner,
                r_constraint_name
           from all_constraints
          where owner = upper(dev_schema_name)
            and table_name = v_sorted_tables(i).object_name
            and constraint_type in ( 'P',
                                     'R' )
          order by constraint_type desc
      ) loop
         v_ddl := v_ddl
                  || '    CONSTRAINT "'
                  || cons.constraint_name
                  || '" ';
         if cons.constraint_type = 'P' then
            v_ddl := v_ddl || 'PRIMARY KEY (';
            for col in (
               select column_name
                 from all_cons_columns
                where owner = upper(dev_schema_name)
                  and constraint_name = cons.constraint_name
                order by position
            ) loop
               v_ddl := v_ddl
                        || '"'
                        || col.column_name
                        || '",';
            end loop;
            v_ddl := rtrim(
               v_ddl,
               ','
            )
                     || ')';
         elsif cons.constraint_type = 'R' then
            v_ddl := v_ddl || 'FOREIGN KEY (';
            for col in (
               select column_name
                 from all_cons_columns
                where owner = upper(dev_schema_name)
                  and constraint_name = cons.constraint_name
                order by position
            ) loop
               v_ddl := v_ddl
                        || '"'
                        || col.column_name
                        || '",';
            end loop;
            v_ddl := rtrim(
               v_ddl,
               ','
            )
                     || ') REFERENCES "'
                     || upper(prod_schema_name)
                     || '"."';
            declare
               v_ref_table varchar2(128);
            begin
               select table_name
                 into v_ref_table
                 from all_constraints
                where owner = cons.r_owner
                  and constraint_name = cons.r_constraint_name
                  and rownum = 1;
               v_ddl := v_ddl
                        || v_ref_table
                        || '" (';
               for col in (
                  select column_name
                    from all_cons_columns
                   where owner = cons.r_owner
                     and constraint_name = cons.r_constraint_name
                   order by position
               ) loop
                  v_ddl := v_ddl
                           || '"'
                           || col.column_name
                           || '",';
               end loop;
               v_ddl := rtrim(
                  v_ddl,
                  ','
               )
                        || ')'; -- лишнюю запятую скипаем
            exception
               when no_data_found then
                  null;
            end;
         end if;
         v_ddl := v_ddl
                  || ','
                  || chr(10);
      end loop;

      v_ddl := rtrim(
         v_ddl,
         ',' || chr(10)
      )
               || chr(10)
               || ');';
      dbms_output.put_line(v_ddl);
      dbms_output.put_line('/');
   end loop;
    
    --дроп тех, что нет в деве
   for rec in object_diff_to_drop loop
      dbms_output.put_line('DROP TABLE "'
                           || upper(prod_schema_name)
                           || '"."'
                           || rec.object_name
                           || '";');
   end loop;
exception
   when others then
      dbms_output.put_line('Error: ' || sqlerrm);
      raise;
end sync_database;
/


begin
   sync_database(
      'developer',
      'production'
   );
end;