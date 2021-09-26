#!/bin/sh

export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export ORACLE_SID=XE
export TNS_ADMIN=$ORACLE_HOME/network/admin
export SQLPLUS=$ORACLE_HOME/bin/sqlplus

su - oracle -c 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe;export ORACLE_SID=XE;export TNS_ADMIN=$ORACLE_HOME/network/admin;export SQLPLUS=$ORACLE_HOME/bin/sqlplus;/u01/app/oracle/product/11.2.0/xe/bin/sqlplus sys/oracle as sysdba << EOF

alter system set processes = 300 scope = spfile;
alter system set sessions = 335 scope = spfile;
alter system set transactions = 369 scope = spfile;

create user test_user identified by 1234;
GRANT create session to test_user;
GRANT CREATE TABLE to test_user;
ALTER USER test_user quota 50m on system;
CREATE TABLE test_user.testTable(
    id       NUMBER PRIMARY KEY,
    textFied VARCHAR(100));

shutdown immediate;

startup;

EOF
'