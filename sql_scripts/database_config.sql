CREATE LOGIN external_user1 WITH password='passw0rd!';
CREATE USER external_user1 FOR LOGIN external_user1;
GRANT SELECT to external_user1;