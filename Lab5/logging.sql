create table dml_log (
   log_id      number primary key,
   table_name  varchar2(50) not null,
   operation   varchar2(10) not null,
   row_id      varchar2(100) not null,
   change_date timestamp with time zone default ( systimestamp at time zone 'UTC' ) not null,
   old_data    clob,
   new_data    clob
);


drop table dml_log;

create sequence dml_log_seq start with 1 increment by 1;

create or replace trigger trg_customers_audit after
   insert or update or delete on customers
   for each row
begin
   if pkg_dml_rollback.g_disable_logging then
      return;
   end if;
   if inserting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'CUSTOMERS',
                 'INSERT',
                 to_char(:new.customer_id),
                 'Name='
                 || :new.customer_name
                 || ', RegistrationDate='
                 || to_char(
                    :new.registration_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 ),
                 systimestamp at time zone 'UTC' );

   elsif updating then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'CUSTOMERS',
                 'UPDATE',
                 to_char(:old.customer_id),
                 'Name='
                 || :old.customer_name
                 || ', RegistrationDate='
                 || to_char(
                    :old.registration_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 ),
                 'Name='
                 || :new.customer_name
                 || ', RegistrationDate='
                 || to_char(
                    :new.registration_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 ),
                 systimestamp at time zone 'UTC' );

   elsif deleting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'CUSTOMERS',
                 'DELETE',
                 to_char(:old.customer_id),
                 'Name='
                 || :old.customer_name
                 || ', RegistrationDate='
                 || to_char(
                    :old.registration_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 ),
                 systimestamp at time zone 'UTC' );
   end if;
end;
/

create or replace trigger trg_payments_audit after
   insert or update or delete on payments
   for each row
begin
   if pkg_dml_rollback.g_disable_logging then
      return;
   end if;
   if inserting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'PAYMENTS',
                 'INSERT',
                 to_char(:new.payment_id),
                 'OrderID='
                 || :new.order_id
                 || ', PaymentMethod='
                 || :new.payment_method
                 || ', PaymentDate='
                 || to_char(
                    :new.payment_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Amount='
                 || to_char(:new.amount),
                 systimestamp at time zone 'UTC' );

   elsif updating then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'PAYMENTS',
                 'UPDATE',
                 to_char(:old.payment_id),
                 'OrderID='
                 || :old.order_id
                 || ', PaymentMethod='
                 || :old.payment_method
                 || ', PaymentDate='
                 || to_char(
                    :old.payment_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Amount='
                 || to_char(:old.amount),
                 'OrderID='
                 || :new.order_id
                 || ', PaymentMethod='
                 || :new.payment_method
                 || ', PaymentDate='
                 || to_char(
                    :new.payment_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Amount='
                 || to_char(:new.amount),
                 systimestamp at time zone 'UTC' );

   elsif deleting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'PAYMENTS',
                 'DELETE',
                 to_char(:old.payment_id),
                 'OrderID='
                 || :old.order_id
                 || ', PaymentMethod='
                 || :old.payment_method
                 || ', PaymentDate='
                 || to_char(
                    :old.payment_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Amount='
                 || to_char(:old.amount),
                 systimestamp at time zone 'UTC' );
   end if;
end;
/


create or replace trigger trg_orders_audit after
   insert or update or delete on orders
   for each row
begin
   if pkg_dml_rollback.g_disable_logging then
      return;
   end if;
   if inserting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'ORDERS',
                 'INSERT',
                 to_char(:new.order_id),
                 'CustomerID='
                 || :new.customer_id
                 || ', Status='
                 || :new.order_status
                 || ', OrderDate='
                 || to_char(
                    :new.order_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Total='
                 || to_char(:new.order_total),
                 systimestamp at time zone 'UTC' );

   elsif updating then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         new_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'ORDERS',
                 'UPDATE',
                 to_char(:old.order_id),
                 'CustomerID='
                 || :old.customer_id
                 || ', Status='
                 || :old.order_status
                 || ', OrderDate='
                 || to_char(
                    :old.order_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Total='
                 || to_char(:old.order_total),
                 'CustomerID='
                 || :new.customer_id
                 || ', Status='
                 || :new.order_status
                 || ', OrderDate='
                 || to_char(
                    :new.order_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Total='
                 || to_char(:new.order_total),
                 systimestamp at time zone 'UTC' );

   elsif deleting then
      insert into dml_log (
         log_id,
         table_name,
         operation,
         row_id,
         old_data,
         change_date
      ) values ( dml_log_seq.nextval,
                 'ORDERS',
                 'DELETE',
                 to_char(:old.order_id),
                 'CustomerID='
                 || :old.customer_id
                 || ', Status='
                 || :old.order_status
                 || ', OrderDate='
                 || to_char(
                    :old.order_date,
                    'YYYY-MM-DD HH24:MI:SS'
                 )
                 || ', Total='
                 || to_char(:old.order_total),
                 systimestamp at time zone 'UTC' );
   end if;
end;
/