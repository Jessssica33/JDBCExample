create table mycustomer (
    cid int primary key not null unique,
    first_name varchar(30),
    last_name varchar(30),
    login varchar(30) not null,
    password varchar(30) not null,
    rental_plan int
);

insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (1, 'Shan', 'Alexander', 'shan', '123', 3);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (2, 'Jessica', 'Wu', 'jessica', '123', 1);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (3, 'Kai', 'Zhao', 'kai', '123', 3);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (4, 'Flora', 'Zhao', 'flora', '123', 2);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (5, 'Summer', 'John', 'summer', '456', 1);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (6, 'Will', 'John', 'will', '456', 2);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (7, 'Vick', 'Li', 'vick', '456', 1);
insert into mycustomer (cid, first_name, last_name, login, password, rental_plan) values (8, 'Dan', 'Will', 'dan', '456', 1);


create table myplan(
    pid int primary key not null unique,
    name varchar(30) not null,
    max_num int,
    monthly_fee int
);

insert into myplan (pid, name, max_num, monthly_fee) values (1, 'Basic', 3, 0);
insert into myplan (pid, name, max_num, monthly_fee) values (2, 'Rental Plus', 5, 10);
insert into myplan (pid, name, max_num, monthly_fee) values (3, 'Super Access', 8, 15);



create table myrental(
    cid int not null,
    mid varchar(30) not null,
    status varchar(10),
    rental_time int,
    primary key(mid, cid, status, rental_time),
    foreign key(cid) references mycustomer(cid)
);

insert into myrental (cid, mid, status, rental_time) values (1, 'M_166', 'closed', 1);
insert into myrental (cid, mid, status, rental_time) values (1, 'M_166', 'closed', 2);
insert into myrental (cid, mid, status, rental_time) values (6, 'M_166', 'open', 1);
insert into myrental (cid, mid, status, rental_time) values (6, 'M_1001', 'open', 1);
insert into myrental (cid, mid, status, rental_time) values (6, 'M_1006', 'open', 1);
insert into myrental (cid, mid, status, rental_time) values (6, 'M_1005', 'open', 1);
insert into myrental (cid, mid, status, rental_time) values (6, 'M_101', 'open', 1);
