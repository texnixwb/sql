--посмотреть гранты:
select * from system.grants

select * from system.users;

create user user_name IDENTIFIED WITH plaintext_password BY 'user_pass';
grant select on *.* to user_name;
