create or replace package pkg_dml_report as
   g_last_report_date timestamp with time zone := systimestamp;
   procedure generate_report (
      p_start_date in timestamp with time zone default null
   );
end pkg_dml_report;
/
create or replace package body pkg_dml_report as
   procedure generate_report (
      p_start_date in timestamp with time zone default null
   ) is
      v_start_date timestamp with time zone;
      v_curr_date  timestamp with time zone;
      v_html       clob := '';
      v_ins        number;
      v_upd        number;
      v_del        number;
   begin
      if p_start_date is null then
         v_start_date := g_last_report_date;
      else
         v_start_date := p_start_date;
      end if;

      if pkg_dml_rollback.g_rollback_date is null then
         v_curr_date := systimestamp;
      else
         v_curr_date := pkg_dml_rollback.g_rollback_date;
      end if;
      v_html := '<html><head><meta charset="UTF-8"><title>Отчет изменений</title></head><body>'
                || '<h2>Отчет изменений с '
                || to_char(
         v_start_date,
         'YYYY-MM-DD HH24:MI:SS'
      )
                || ' по '
                || to_char(
         v_curr_date,
         'YYYY-MM-DD HH24:MI:SS'
      )
                || '</h2>'
                || '<table border="1" cellspacing="0" cellpadding="5">'
                || '<tr><th>Таблица</th><th>INSERT</th><th>UPDATE</th><th>DELETE</th></tr>';
    
      /* Статистика для таблицы CUSTOMERS */
      begin
         select nvl(
            sum(
               case
                  when operation = 'INSERT' then
                     cnt
                  else 0
               end
            ),
            0
         ),
                nvl(
                   sum(
                      case
                         when operation = 'UPDATE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                ),
                nvl(
                   sum(
                      case
                         when operation = 'DELETE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                )
           into
            v_ins,
            v_upd,
            v_del
           from (
            select operation,
                   count(*) cnt
              from dml_log
             where table_name = 'CUSTOMERS'
               and change_date >= v_start_date
             group by operation
         );
      exception
         when no_data_found then
            v_ins := 0;
            v_upd := 0;
            v_del := 0;
      end;
      v_html := v_html
                || '<tr><td>CUSTOMERS</td><td>'
                || v_ins
                || '</td><td>'
                || v_upd
                || '</td><td>'
                || v_del
                || '</td></tr>';
    
      /* Статистика для таблицы ORDERS */
      begin
         select nvl(
            sum(
               case
                  when operation = 'INSERT' then
                     cnt
                  else 0
               end
            ),
            0
         ),
                nvl(
                   sum(
                      case
                         when operation = 'UPDATE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                ),
                nvl(
                   sum(
                      case
                         when operation = 'DELETE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                )
           into
            v_ins,
            v_upd,
            v_del
           from (
            select operation,
                   count(*) cnt
              from dml_log
             where table_name = 'ORDERS'
               and change_date >= v_start_date
             group by operation
         );
      exception
         when no_data_found then
            v_ins := 0;
            v_upd := 0;
            v_del := 0;
      end;
      v_html := v_html
                || '<tr><td>ORDERS</td><td>'
                || v_ins
                || '</td><td>'
                || v_upd
                || '</td><td>'
                || v_del
                || '</td></tr>';
    
      /* Статистика для таблицы PAYMENTS */
      begin
         select nvl(
            sum(
               case
                  when operation = 'INSERT' then
                     cnt
                  else 0
               end
            ),
            0
         ),
                nvl(
                   sum(
                      case
                         when operation = 'UPDATE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                ),
                nvl(
                   sum(
                      case
                         when operation = 'DELETE' then
                            cnt
                         else 0
                      end
                   ),
                   0
                )
           into
            v_ins,
            v_upd,
            v_del
           from (
            select operation,
                   count(*) cnt
              from dml_log
             where table_name = 'PAYMENTS'
               and change_date >= v_start_date
             group by operation
         );
      exception
         when no_data_found then
            v_ins := 0;
            v_upd := 0;
            v_del := 0;
      end;
      v_html := v_html
                || '<tr><td>PAYMENTS</td><td>'
                || v_ins
                || '</td><td>'
                || v_upd
                || '</td><td>'
                || v_del
                || '</td></tr>';


      v_html := v_html || '</table>';
      v_html := v_html || '<h2>Детали изменений</h2>';
    
      /* Детали для таблицы CUSTOMERS */
      v_html := v_html
                || '<h3>CUSTOMERS</h3>'
                || '<table border="1" cellspacing="0" cellpadding="5">'
                || '<tr><th>Операция</th><th>ROW_ID</th><th>CHANGE_DATE</th><th>OLD_DATA</th><th>NEW_DATA</th></tr>';
      for rec in (
         select operation,
                row_id,
                to_char(
                   change_date,
                   'YYYY-MM-DD HH24:MI:SS'
                ) as change_date,
                nvl(
                   old_data,
                   ' '
                ) as old_data,
                nvl(
                   new_data,
                   ' '
                ) as new_data
           from dml_log
          where table_name = 'CUSTOMERS'
            and change_date >= v_start_date
          order by change_date asc
      ) loop
         v_html := v_html
                   || '<tr><td>'
                   || rec.operation
                   || '</td>'
                   || '<td>'
                   || rec.row_id
                   || '</td>'
                   || '<td>'
                   || rec.change_date
                   || '</td>'
                   || '<td>'
                   || rec.old_data
                   || '</td>'
                   || '<td>'
                   || rec.new_data
                   || '</td></tr>';
      end loop;
      v_html := v_html || '</table>';
    
      /* Детали для таблицы ORDERS */
      v_html := v_html
                || '<h3>ORDERS</h3>'
                || '<table border="1" cellspacing="0" cellpadding="5">'
                || '<tr><th>Операция</th><th>ROW_ID</th><th>CHANGE_DATE</th><th>OLD_DATA</th><th>NEW_DATA</th></tr>';
      for rec in (
         select operation,
                row_id,
                to_char(
                   change_date,
                   'YYYY-MM-DD HH24:MI:SS'
                ) as change_date,
                nvl(
                   old_data,
                   ' '
                ) as old_data,
                nvl(
                   new_data,
                   ' '
                ) as new_data
           from dml_log
          where table_name = 'ORDERS'
            and change_date >= v_start_date
          order by change_date asc
      ) loop
         v_html := v_html
                   || '<tr><td>'
                   || rec.operation
                   || '</td>'
                   || '<td>'
                   || rec.row_id
                   || '</td>'
                   || '<td>'
                   || rec.change_date
                   || '</td>'
                   || '<td>'
                   || rec.old_data
                   || '</td>'
                   || '<td>'
                   || rec.new_data
                   || '</td></tr>';
      end loop;
      v_html := v_html || '</table>';
    
      /* Детали для таблицы PAYMENTS */
      v_html := v_html
                || '<h3>PAYMENTS</h3>'
                || '<table border="1" cellspacing="0" cellpadding="5">'
                || '<tr><th>Операция</th><th>ROW_ID</th><th>CHANGE_DATE</th><th>OLD_DATA</th><th>NEW_DATA</th></tr>';
      for rec in (
         select operation,
                row_id,
                to_char(
                   change_date,
                   'YYYY-MM-DD HH24:MI:SS'
                ) as change_date,
                nvl(
                   old_data,
                   ' '
                ) as old_data,
                nvl(
                   new_data,
                   ' '
                ) as new_data
           from dml_log
          where table_name = 'PAYMENTS'
            and change_date >= v_start_date
          order by change_date asc
      ) loop
         v_html := v_html
                   || '<tr><td>'
                   || rec.operation
                   || '</td>'
                   || '<td>'
                   || rec.row_id
                   || '</td>'
                   || '<td>'
                   || rec.change_date
                   || '</td>'
                   || '<td>'
                   || rec.old_data
                   || '</td>'
                   || '<td>'
                   || rec.new_data
                   || '</td></tr>';
      end loop;
      v_html := v_html || '</table>';
      v_html := v_html || '</body></html>';
      dbms_output.put_line(v_html);
      g_last_report_date := v_curr_date;
   end generate_report;

end pkg_dml_report;
/




begin
   pkg_dml_report.generate_report(to_date('2025-05-04 14:45:00',
                                  'YYYY-MM-DD HH24:MI:SS'));
end;
/

begin
   pkg_dml_report.generate_report;
end;
/


select *
  from customers;
select *
  from payments;
select *
  from orders;


begin
   dbms_output.put_line('awd');
   dbms_output.put_line(systimestamp);
end;
/