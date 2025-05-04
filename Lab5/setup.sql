create table customers (
   customer_id       number primary key,
   customer_name     varchar2(100) not null,
   registration_date date not null
);

create table orders (
   order_id     number primary key,
   customer_id  number not null,
   order_status varchar2(20) not null,
   order_date   date not null,
   order_total  number(10,2) not null,
   constraint fk_customer foreign key ( customer_id )
      references customers ( customer_id )
);

create table payments (
   payment_id     number primary key,
   order_id       number not null,
   payment_method varchar2(50) not null,
   payment_date   date not null,
   amount         number(10,2) not null,
   constraint fk_order foreign key ( order_id )
      references orders ( order_id )
);

drop table customers;
drop table orders;
drop table payments;

select table_name
  from user_tables
 where table_name in ( 'CUSTOMERS',
                       'ORDERS',
                       'PAYMENTS' );


