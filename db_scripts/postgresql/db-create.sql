
DROP DATABASE IF EXISTS ehdtd_dev ;
DROP USER IF EXISTS ehdtd_dev ;

CREATE USER ehdtd_dev WITH ENCRYPTED password 'ehdtd_9898' NOCREATEDB NOCREATEROLE ;

CREATE DATABASE ehdtd_dev WITH OWNER ehdtd_dev ENCODING = 'utf-8' TEMPLATE template0 ;

ALTER DATABASE ehdtd_dev SET DateStyle = 'ISO, YMD';
ALTER DATABASE ehdtd_dev SET client_encoding = 'utf-8' ;

