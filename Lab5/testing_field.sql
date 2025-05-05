insert into customers (
   customer_id,
   customer_name,
   registration_date
) values ( 1,
           'Иванов',
           sysdate );

insert into customers (
   customer_id,
   customer_name,
   registration_date
) values ( 2,
           'Петров',
           sysdate );

insert into orders (
   order_id,
   customer_id,
   order_status,
   order_date,
   order_total
) values ( 100,
           1,
           'NEW',
           sysdate,
           200.50 );

insert into orders (
   order_id,
   customer_id,
   order_status,
   order_date,
   order_total
) values ( 101,
           2,
           'NEW',
           sysdate,
           500.75 );

insert into payments (
   payment_id,
   order_id,
   payment_method,
   payment_date,
   amount
) values ( 1000,
           100,
           'Credit Card',
           sysdate,
           200.50 );

insert into payments (
   payment_id,
   order_id,
   payment_method,
   payment_date,
   amount
) values ( 1001,
           101,
           'Cash',
           sysdate,
           500.75 );

commit;

update customers
   set
   customer_name = 'Иван'
 where customer_id = 1;

update orders
   set order_status = 'SHIPPED',
       order_total = 210.00
 where order_id = 100;


update payments
   set payment_method = 'Debit Card',
       amount = 210.00
 where payment_id = 1000;

commit;


delete from payments
 where payment_id = 1000;


delete from orders
 where order_id = 100;


delete from customers
 where customer_id = 1;

commit;


select *
  from customers;
select *
  from payments;
select *
  from orders;

delete from payments where payment_id = 1001;