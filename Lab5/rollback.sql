create or replace package pkg_dml_rollback as
   g_disable_logging boolean := false;
   g_rollback_date timestamp with time zone := null;
   procedure rollback_changes (
      p_target_date in date
   );
   procedure rollback_changes (
      p_interval_ms in number
   );
end pkg_dml_rollback;
/

create or replace package body pkg_dml_rollback as
   procedure restore_customers (
      p_target_date in date
   ) is
      cursor cur_cust is
      select log_id,
             operation,
             row_id,
             new_data
        from dml_log
       where table_name = 'CUSTOMERS'
         and change_date <= p_target_date
       order by change_date asc;

      v_temp      varchar2(4000);
      v_start     number;
      v_end       number;
      v_name      varchar2(100);
      v_reg_date  date;
      l_row_id    varchar2(100);
      l_operation varchar2(10);
      l_new_data  clob;
   begin
      for rec in cur_cust loop
         l_row_id := rec.row_id;
         l_operation := rec.operation;
         l_new_data := rec.new_data;
         if l_operation = 'INSERT' then
            v_temp := l_new_data;
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
            execute immediate 'INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, REGISTRATION_DATE)
           VALUES (:id, :name, :rdate)'
               using l_row_id,v_name,v_reg_date;
         elsif l_operation = 'UPDATE' then
            v_temp := l_new_data;
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
            begin
               execute immediate 'UPDATE CUSTOMERS SET CUSTOMER_NAME = :name, REGISTRATION_DATE = :rdate
             WHERE CUSTOMER_ID = :id'
                  using v_name,v_reg_date,l_row_id;
            exception
               when no_data_found then
                  execute immediate 'INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, REGISTRATION_DATE)
               VALUES (:id, :name, :rdate)'
                     using l_row_id,v_name,v_reg_date;
            end;

         elsif l_operation = 'DELETE' then
            execute immediate 'DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = :id'
               using l_row_id;
         end if;
      end loop;
   end restore_customers;

   procedure restore_orders (
      p_target_date in date
   ) is
      cursor cur_orders is
      select log_id,
             operation,
             row_id,
             new_data
        from dml_log
       where table_name = 'ORDERS'
         and change_date <= p_target_date
       order by change_date asc;

      v_temp        varchar2(4000);
      v_start       number;
      v_end         number;
      v_customer_id number;
      v_status      varchar2(20);
      v_order_date  date;
      v_total       number;
      l_row_id      varchar2(100);
      l_operation   varchar2(10);
      l_new_data    clob;
   begin
      for rec in cur_orders loop
         l_row_id := rec.row_id;
         l_operation := rec.operation;
         l_new_data := rec.new_data;
         if l_operation = 'INSERT' then
            v_temp := l_new_data;
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
            execute immediate 'INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_DATE, ORDER_TOTAL)
           VALUES (:id, :cid, :status, :odate, :total)'
               using l_row_id,v_customer_id,v_status,v_order_date,v_total;
         elsif l_operation = 'UPDATE' then
            v_temp := l_new_data;
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
            begin
               execute immediate 'UPDATE ORDERS SET CUSTOMER_ID = :cid, ORDER_STATUS = :status, ORDER_DATE = :odate, ORDER_TOTAL = :total
             WHERE ORDER_ID = :id'
                  using v_customer_id,v_status,v_order_date,v_total,l_row_id;
            exception
               when no_data_found then
                  execute immediate 'INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_DATE, ORDER_TOTAL)
               VALUES (:id, :cid, :status, :odate, :total)'
                     using l_row_id,v_customer_id,v_status,v_order_date,v_total;
            end;

         elsif l_operation = 'DELETE' then
            execute immediate 'DELETE FROM ORDERS WHERE ORDER_ID = :id'
               using l_row_id;
         end if;
      end loop;
   end restore_orders;

   procedure restore_payments (
      p_target_date in date
   ) is
      cursor cur_pay is
      select log_id,
             operation,
             row_id,
             new_data
        from dml_log
       where table_name = 'PAYMENTS'
         and change_date <= p_target_date
       order by change_date asc;

      v_temp           varchar2(4000);
      v_start          number;
      v_end            number;
      v_order_id       number;
      v_payment_method varchar2(50);
      v_payment_date   date;
      v_amount         number;
      l_row_id         varchar2(100);
      l_operation      varchar2(10);
      l_new_data       clob;
   begin
      for rec in cur_pay loop
         l_row_id := rec.row_id;
         l_operation := rec.operation;
         l_new_data := rec.new_data;
         if l_operation = 'INSERT' then
            v_temp := l_new_data;
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
            execute immediate 'INSERT INTO PAYMENTS (PAYMENT_ID, ORDER_ID, PAYMENT_METHOD, PAYMENT_DATE, AMOUNT)
           VALUES (:id, :oid, :pm, :pdate, :amount)'
               using l_row_id,v_order_id,v_payment_method,v_payment_date,v_amount;
         elsif l_operation = 'UPDATE' then
            v_temp := l_new_data;
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
            begin
               execute immediate 'UPDATE PAYMENTS SET ORDER_ID = :oid, PAYMENT_METHOD = :pm, PAYMENT_DATE = :pdate, AMOUNT = :amount
             WHERE PAYMENT_ID = :id'
                  using v_order_id,v_payment_method,v_payment_date,v_amount,l_row_id;
            exception
               when no_data_found then
                  execute immediate 'INSERT INTO PAYMENTS (PAYMENT_ID, ORDER_ID, PAYMENT_METHOD, PAYMENT_DATE, AMOUNT)
               VALUES (:id, :oid, :pm, :pdate, :amount)'
                     using l_row_id,v_order_id,v_payment_method,v_payment_date,v_amount;
            end;

         elsif l_operation = 'DELETE' then
            execute immediate 'DELETE FROM PAYMENTS WHERE PAYMENT_ID = :id'
               using l_row_id;
         end if;
      end loop;
   end restore_payments;

   procedure rollback_changes (
      p_target_date in date
   ) is
   begin
      g_disable_logging := true;
      execute immediate 'DELETE FROM PAYMENTS';
      execute immediate 'DELETE FROM ORDERS';
      execute immediate 'DELETE FROM CUSTOMERS';
      commit;
      restore_customers(p_target_date);
      restore_orders(p_target_date);
      restore_payments(p_target_date);
      commit;
      g_disable_logging := false;
      g_rollback_date := systimestamp;
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
  from dml_log
 order by change_date asc;

begin
   pkg_dml_rollback.rollback_changes(to_date('2025-05-05 19:26:24',
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