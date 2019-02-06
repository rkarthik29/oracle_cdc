### Setup XStreams on Oracle Database


*Tested on Oracle 12.2 in both docker and IaaS deployments. Amazon RDS was also tested but we were unable to get it started. 
It is recommended that XStreams ONLY be utilized on self deployed databases, where the user has root access to the database.*

Prerequisites:
- Ensure that Goldengate Replication is enabled
- Ensure that AutoArchiving is enabled.
- A user with sysdba privileges


#### XStreams Overview

XStreams was released with Oracle 11g and was engineered to support data replication between Oracle and non-Oracle databases. 
With XStreams, transactions that are collected in redo logs are replicated to a memory queue (in order) where they are 
available to be read by an external XStreams application. This memory queue is built off of the dat SGA (System Global Area) 
memory. The size of the memory query can be modified by adjust the Oracle Streams Pool. 

The memory queue is exposed by the creation of a XStreams Output Server. The XStreams application connects to the server and 
ingests the records.

Below are instructions for setting up the proper users and permissions to deploy a simple XStreams application. 

#### _Create xstream_admin user and tablespace_ 

*CREATE TABLESPACE xstream_tbs DATAFILE 'C:\oracledb\database\xstream_table_space\xstream_tbs.dbf' 
SIZE 50M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;*

*CREATE USER xstream_admin IDENTIFIED BY welcome1
DEFAULT TABLESPACE xstream_tbs
QUOTA UNLIMITED ON xstream_tbs;
COMMIT;*

#### _Assign Permissions to xstream_admin_

*GRANT DBA TO xstream_admin;*     
*GRANT CONNECT, CREATE TABLE TO xstream_admin;*     
*GRANT RESOURCE TO xstream_admin;*     
*GRANT CREATE TABLESPACE TO xstream_admin;*   
*GRANT UNLIMITED TABLESPACE TO xstream_admin;*   
*GRANT SELECT_CATALOG_ROLE TO xstream_admin;*   
*GRANT EXECUTE_CATALOG_ROLE TO xstream_admin;*  
*GRANT CREATE SEQUENCE TO xstream_admin;*      
*GRANT CREATE SESSION TO xstream_admin;*   
*GRANT CREATE ANY VIEW TO xstream_admin;*   
*GRANT CREATE ANY TABLE TO xstream_admin;*   
*GRANT SELECT ANY TABLE TO xstream_admin;* 
*GRANT COMMENT ANY TABLE TO xstream_admin;*   
*GRANT LOCK ANY TABLE TO xstream_admin;*   
*GRANT SELECT ANY DICTIONARY TO xstream_admin;*   
*GRANT EXECUTE ON SYS.DBMS_CDC_PUBLISH to xstream_admin;*  
*ALTER USER xstream_admin QUOTA UNLIMITED ON USERS;*  
*GRANT CREATE ANY TRIGGER TO xstream_admin;*  
*GRANT ALTER ANY TRIGGER TO xstream_admin;*  
*GRANT DROP ANY TRIGGER TO xstream_admin;*  
*COMMIT;  *

#### _Assign xstream_admin Admin Privileges to XStreams_

*BEGIN*  
   *DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(*    
      *grantee => 'xstream_admin',* 
      *privilege_type => 'CAPTURE',*  
      *grant_select_privileges => TRUE);*  
*END;*

*BEGIN*  
   *DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(*    
      *grantee => 'xstream_admin',*  
      *privilege_type => 'APPLY',*  
      *grant_select_privileges => TRUE);*
*END;*    
*COMMIT;*    


#### _Create Test User and Grant Permissions_

Create a schema called xstream_test_user. This schema will be used to hold the test table 
XStreams will be collecting CDC from.

*CREATE USER xstream_test_user IDENTIFIED BY welcome1   
DEFAULT TABLESPACE users  
QUOTA UNLIMITED ON users;*

*GRANT connect,resource,create table TO xstream_test_user;*  
*COMMIT;*  

#### _CREATE TEST TABLE_

*CREATE TABLE xstream_test_user.t1
(id int primary key,
department varchar2(100),
salary varchar2(100));*   
*COMMIT;*   

#### _CREATE XSTREAMS OUTBOUND SERVER_
This procedure creates the server that an the external XStreams client will connect to in order to harvest CDC 
from our test table. 

*DECLARE*  
  *tables  DBMS_UTILITY.UNCL_ARRAY;*  
  *schemas DBMS_UTILITY.UNCL_ARRAY;*  
*BEGIN*  
    *tables(1)  := 'T1';*  
    *schemas(1) := 'XSTREAM_TEST_USER';*  
  *DBMS_XSTREAM_ADM.CREATE_OUTBOUND(*  
    *server_name     =>  'xout',*  
    *table_names     =>  tables,*  
    *schema_names    =>  schemas,*  
    *capture_user => 'xstream_admin',*  
    *connect_user =>  'xstream_admin');*  
*END;*     
*COMMIT;*  

To see if things are running well, execute teh following query:

Inline-style: 
![alt text](https://github.com/jster1357/oracle_cdc/edit/master/vstream-capture.png "vstreams capture")


*COLUMN CAPTURE_NAME HEADING 'Capture|Name' FORMAT A15*  
*COLUMN STATE HEADING 'State' FORMAT A15*  
*COLUMN CREATE_MESSAGE HEADING 'Last LCR|Create Time'*  
*COLUMN ENQUEUE_MESSAGE HEADING 'Last|Enqueue Time'*  
*SELECT CAPTURE_NAME, STATE,*  
       *TO_CHAR(CAPTURE_MESSAGE_CREATE_TIME, 'HH24:MI:SS MM/DD/YY') CREATE_MESSAGE,*  
       *TO_CHAR(ENQUEUE_MESSAGE_CREATE_TIME, 'HH24:MI:SS MM/DD/YY') ENQUEUE_MESSAGE*  
  *FROM V$XSTREAM_CAPTURE;*  
  
If everything is setup properly, you should see a “waiting for transaction” in the STATE field.

To drop a output server run the following command, replacing xout with your xstream server name

*exec DBMS_XSTREAM_ADM.DROP_OUTBOUND('xout');* 

#### _GENERATE TEST DATA_

The SQL below will generate 5000 records into test table created previously. These inserts will be captured by the 
XStreams process.

*INSERT INTO xstream_test_user.t1(id, department, salary)*  
*SELECT rownum, 'Employee ' || to_char(rownum), dbms_random.value(2, 9) * 1000*  
*FROM dual*  
*CONNECT BY level <= 5000;*  
*COMMIT;*  

When running the XStreams java app, you should see records come through on the console after the records have been committed. 


https://github.com/jster1357/oracle_cdc/edit/master/

Inline-style: 
![alt text](https://github.com/jster1357/oracle_cdc/edit/master/vstream-capture.png "vstreams capture")

