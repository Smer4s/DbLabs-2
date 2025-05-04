create or replace package pkg_dml_rollback as
   procedure rollback_changes (
      p_target_date in date
   );

   procedure rollback_changes (
      p_interval_ms in number
   );
end pkg_dml_rollback;
/

-- Реализация пакета
create or replace package body pkg_dml_rollback as

   procedure rollback_changes (
      p_target_date in date
   ) is
    -- Выбираем записи аудита с изменениями после заданного момента времени
      cursor cur_audit is
      select log_id,
             table_name,
             operation,
             row_id,
             old_data,
             change_date
        from dml_log
       where change_date > p_target_date
       order by change_date desc;

      v_temp  varchar2(4000);
      v_start number;
      v_end   number;
   begin
    -- Отключаем логирование изменений в триггерах
      g_disable_audit := true;
      begin
      -- Обрабатываем записи аудита в обратном хронологическом порядке
         for rec in cur_audit loop
            if rec.table_name = 'CUSTOMERS' then
               if rec.operation = 'INSERT' then
            -- Отмена вставки: удаляем запись
                  execute immediate 'DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = :id'
                     using rec.row_id;
               elsif rec.operation = 'UPDATE' then
                  declare
                     v_name     varchar2(100);
                     v_reg_date date;
                  begin
              -- Формат OLD_DATA: 'Name=Иванов, RegistrationDate=2025-05-04 14:47:00'
                     v_temp := rec.old_data;
                     v_start := instr(
                        v_temp,
                        'Name='
                     ) + length('Name=');
                     v_end := instr(
                        v_temp,
                        ', RegistrationDate='
                     );
                     v_name := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := v_end + length(', RegistrationDate=');
                     v_reg_date := to_date ( substr(
                        v_temp,
                        v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );
                     execute immediate 'UPDATE CUSTOMERS SET CUSTOMER_NAME = :name, REGISTRATION_DATE = :rdate WHERE CUSTOMER_ID = :id'
                        using v_name,v_reg_date,
                        rec.row_id;
                  end;
               elsif rec.operation = 'DELETE' then
                  declare
                     v_name     varchar2(100);
                     v_reg_date date;
                  begin
                     v_temp := rec.old_data;
                     v_start := instr(
                        v_temp,
                        'Name='
                     ) + length('Name=');
                     v_end := instr(
                        v_temp,
                        ', RegistrationDate='
                     );
                     v_name := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := v_end + length(', RegistrationDate=');
                     v_reg_date := to_date ( substr(
                        v_temp,
                        v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );
                     execute immediate 'INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, REGISTRATION_DATE) VALUES (:id, :name, :rdate)'
                        using rec.row_id,v_name,v_reg_date;
                  end;
               end if;

            elsif rec.table_name = 'ORDERS' then
               if rec.operation = 'INSERT' then
                  execute immediate 'DELETE FROM ORDERS WHERE ORDER_ID = :id'
                     using rec.row_id;
               elsif rec.operation = 'UPDATE' then
                  declare
                     v_customer_id number;
                     v_status      varchar2(20);
                     v_order_date  date;
                     v_total       number;
                  begin
                     v_temp := rec.old_data;
              -- Формат OLD_DATA: 'CustomerID=1, Status=NEW, OrderDate=2025-05-04 14:47:00, Total=200.50'
                     v_start := instr(
                        v_temp,
                        'CustomerID='
                     ) + length('CustomerID=');
                     v_end := instr(
                        v_temp,
                        ', Status='
                     );
                     v_customer_id := to_number ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ) );
                     v_start := instr(
                        v_temp,
                        ', Status='
                     ) + length(', Status=');
                     v_end := instr(
                        v_temp,
                        ', OrderDate='
                     );
                     v_status := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := instr(
                        v_temp,
                        ', OrderDate='
                     ) + length(', OrderDate=');
                     v_end := instr(
                        v_temp,
                        ', Total='
                     );
                     v_order_date := to_date ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );

                     v_start := instr(
                        v_temp,
                        ', Total='
                     ) + length(', Total=');
                     v_total := to_number ( substr(
                        v_temp,
                        v_start
                     ) );
                     execute immediate 'UPDATE ORDERS SET CUSTOMER_ID = :cid, ORDER_STATUS = :status, ORDER_DATE = :odate, ORDER_TOTAL = :total WHERE ORDER_ID = :id'
                        using v_customer_id,v_status,v_order_date,v_total,
                        rec.row_id;
                  end;
               elsif rec.operation = 'DELETE' then
                  declare
                     v_customer_id number;
                     v_status      varchar2(20);
                     v_order_date  date;
                     v_total       number;
                  begin
                     v_temp := rec.old_data;
                     v_start := instr(
                        v_temp,
                        'CustomerID='
                     ) + length('CustomerID=');
                     v_end := instr(
                        v_temp,
                        ', Status='
                     );
                     v_customer_id := to_number ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ) );
                     v_start := instr(
                        v_temp,
                        ', Status='
                     ) + length(', Status=');
                     v_end := instr(
                        v_temp,
                        ', OrderDate='
                     );
                     v_status := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := instr(
                        v_temp,
                        ', OrderDate='
                     ) + length(', OrderDate=');
                     v_end := instr(
                        v_temp,
                        ', Total='
                     );
                     v_order_date := to_date ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );

                     v_start := instr(
                        v_temp,
                        ', Total='
                     ) + length(', Total=');
                     v_total := to_number ( substr(
                        v_temp,
                        v_start
                     ) );
                     execute immediate 'INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_DATE, ORDER_TOTAL) VALUES (:id, :cid, :status, :odate, :total)'
                        using rec.row_id,v_customer_id,v_status,v_order_date,v_total;
                  end;
               end if;
            elsif rec.table_name = 'PAYMENTS' then
               if rec.operation = 'INSERT' then
                  execute immediate 'DELETE FROM PAYMENTS WHERE PAYMENT_ID = :id'
                     using rec.row_id;
               elsif rec.operation = 'UPDATE' then
                  declare
                     v_order_id       number;
                     v_payment_method varchar2(50);
                     v_payment_date   date;
                     v_amount         number;
                  begin
                     v_temp := rec.old_data;
              -- Формат OLD_DATA: 'OrderID=100, PaymentMethod=Credit Card, PaymentDate=2025-05-04 14:47:00, Amount=200.50'
                     v_start := instr(
                        v_temp,
                        'OrderID='
                     ) + length('OrderID=');
                     v_end := instr(
                        v_temp,
                        ', PaymentMethod='
                     );
                     v_order_id := to_number ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ) );
                     v_start := instr(
                        v_temp,
                        ', PaymentMethod='
                     ) + length(', PaymentMethod=');
                     v_end := instr(
                        v_temp,
                        ', PaymentDate='
                     );
                     v_payment_method := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := instr(
                        v_temp,
                        ', PaymentDate='
                     ) + length(', PaymentDate=');
                     v_end := instr(
                        v_temp,
                        ', Amount='
                     );
                     v_payment_date := to_date ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );

                     v_start := instr(
                        v_temp,
                        ', Amount='
                     ) + length(', Amount=');
                     v_amount := to_number ( substr(
                        v_temp,
                        v_start
                     ) );
                     execute immediate 'UPDATE PAYMENTS SET ORDER_ID = :oid, PAYMENT_METHOD = :pm, PAYMENT_DATE = :pdate, AMOUNT = :amount WHERE PAYMENT_ID = :id'
                        using v_order_id,v_payment_method,v_payment_date,v_amount,
                        rec.row_id;
                  end;
               elsif rec.operation = 'DELETE' then
                  declare
                     v_order_id       number;
                     v_payment_method varchar2(50);
                     v_payment_date   date;
                     v_amount         number;
                  begin
                     v_temp := rec.old_data;
                     v_start := instr(
                        v_temp,
                        'OrderID='
                     ) + length('OrderID=');
                     v_end := instr(
                        v_temp,
                        ', PaymentMethod='
                     );
                     v_order_id := to_number ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ) );
                     v_start := instr(
                        v_temp,
                        ', PaymentMethod='
                     ) + length(', PaymentMethod=');
                     v_end := instr(
                        v_temp,
                        ', PaymentDate='
                     );
                     v_payment_method := substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     );
                     v_start := instr(
                        v_temp,
                        ', PaymentDate='
                     ) + length(', PaymentDate=');
                     v_end := instr(
                        v_temp,
                        ', Amount='
                     );
                     v_payment_date := to_date ( substr(
                        v_temp,
                        v_start,
                        v_end - v_start
                     ),'YYYY-MM-DD HH24:MI:SS' );

                     v_start := instr(
                        v_temp,
                        ', Amount='
                     ) + length(', Amount=');
                     v_amount := to_number ( substr(
                        v_temp,
                        v_start
                     ) );
                     execute immediate 'INSERT INTO PAYMENTS (PAYMENT_ID, ORDER_ID, PAYMENT_METHOD, PAYMENT_DATE, AMOUNT) VALUES (:id, :oid, :pm, :pdate, :amount)'
                        using rec.row_id,v_order_id,v_payment_method,v_payment_date,v_amount;
                  end;
               end if;
            end if;
         end loop;
         commit;
      exception
         when others then
            rollback;
            g_disable_audit := false;
            raise;
      end;
      g_disable_audit := false;
   end rollback_changes;

   procedure rollback_changes (
      p_interval_ms in number
   ) is
      v_target_date date;
   begin
      v_target_date := sysdate - ( p_interval_ms / 86400000 );
      rollback_changes(v_target_date);
   end rollback_changes;

end pkg_dml_rollback;
/


select *
  from dml_log order by change_date asc;

begin
   pkg_dml_rollback.rollback_changes(to_date('2025-05-04 12:38:34',
                                     'YYYY-MM-DD HH24:MI:SS'));
   commit;
end;
/

begin
   pkg_dml_rollback.rollback_changes(30000);
   commit;
end;
/


select *
  from customers;
select *
  from payments;
select *
  from orders;

delete from dml_log;
commit;