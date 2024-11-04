CREATE OR REPLACE package Pkg_AnlzeTwoStkCSVs_LogResults

--Usage notes:
--The procedures are to be executed in the ordered steps as metioned in the name
--The following script are required to be executed as only a one and first time step in the beginning, and must be completed before creating the package body of this package.
--Step1: Create the Oracle directories and the folders in the OS, with the CSV files as per the path mentioned or as required:
-- The file exchange_data.CSV is required to be stored in the folder d:\exchange_data 
-- The file depository_data.CSV is required to be stored in the folder d:\depository_data
--create or replace directory EXCHANGEDATACSVPATH as 'd:\exchange_data\';
--create or replace directory DEPOSITORYDATACSVPATH as 'D:\depository_data\';

--Step2: 
--The following four tables created are required to be created in this schema, two for the logs and ytwo for the stock data.
--Note: Changes may be required to implement, to handle any required changes in data structure or format
    /*
    -- The two log tables are as follws:
    
    create table TheMainComparisonLogOfTwoCSVs
    (
        MatchedStkCnt_User_ID VARCHAR2(50),
        MatchedStkCnt_Stock_ID VARCHAR2(50),
        MatchedStkCnt_Stock_Name VARCHAR2(200),
        MatchedStkCnt_Stock_Count NUMBER(30),
        MsMatchedStkCnt_User_ID VARCHAR2(50),
        MsMatchedStkCnt_Stock_ID VARCHAR2(50),
        MsMatchedStkCnt_Stock_Name VARCHAR2(200),
        MsMatchedStkCnt_Stock_Count_e NUMBER(30),
        MsMatchedStkCnt_Stock_Count_d NUMBER(30)
    );

    create table TheLogOfEdgeCasesAndCSVFileIssues
        (
        LogIDOrderedStep_Serial NUMBER(10),
        LogIDOrderedStep_Desc VARCHAR2(400),
        MissngOrIvldCSV_exchangedata VARCHAR2(4000),
        MissngOrIvldCSV_depositorydata VARCHAR2(4000),
        Dup_User_ID_exchange_data VARCHAR2(50),
        Dup_Stock_ID_exchange_data VARCHAR2(50),
        Dup_Stock_Name_exchange_data VARCHAR2(200),
        Dup_Stock_Count_exchange_data VARCHAR2(50),
        Dup_User_ID_depository_data VARCHAR2(50),
        Dup_Stock_ID_depository_data VARCHAR2(50),
        Dup_Stock_Name_depository_data VARCHAR2(200),
        Dup_Stock_Count_dep_data VARCHAR2(50),
        Incst_Stock_ID_exchange_data VARCHAR2(50),
        Incst_Stock_Name_exchange_data VARCHAR2(50),
        Incst_Stock_ID_depstry_data VARCHAR2(50),
        Incst_Stock_Name_depstry_data VARCHAR2(50),
        TotalRows_exchange_data NUMBER(30),
        TotalRows_depository_data NUMBER(30),
        MissingRows_In_exchange_data NUMBER(30),
        MissingRows_In_depository_data NUMBER(30),
        TotalRows_Matched NUMBER(30),
        TotalRows_MtchdButDiffStkCntNm NUMBER(30)
        );
    
        
    -- The two stock tables are as follows. The "not null" constraints in the following two stock tables, check the - "Edge Cases - Incomplete rows in the CSV (e.g., missing stock data or user details", at the very root of the process
    create table exchange_data_extrnl_fromcsv
   (
     User_ID VARCHAR2(50) not null,
     Stock_ID VARCHAR2(50) not null,
     Stock_Name VARCHAR2(200) not null,
     Stock_Count VARCHAR2(50) not null
   )
   organization external
   (
     type ORACLE_LOADER
     default directory EXCHANGEDATACSVPATH
     access parameters
     (
       records delimited by newline
       skip 1
       fields terminated by ","
       --missing field values are null
       --reject rows with all null fields
     )
     location ('exchange_data.csv')
   )
   reject limit unlimited;
   
    create table depository_data_extrnl_fromcsv
   (
     User_ID VARCHAR2(50) not null,
     Stock_ID VARCHAR2(50) not null,
     Stock_Name VARCHAR2(200) not null,
     Stock_Count VARCHAR2(50) not null
   )
   organization external
   (
     type ORACLE_LOADER
     default directory DEPOSITORYDATACSVPATH
     access parameters
     (
       records delimited by newline
       skip 1
       fields terminated by ","
       --missing field values are null
       --reject rows with all null fields
     )
     location ('depository_data.csv')
   )
   reject limit unlimited;
   */
is

  procedure OrdredStp1_DirFor_Exchange (FilePathInV IN VARCHAR2);
  procedure OrdredStp2_DirFor_depository (FilePathInV IN VARCHAR2);
  procedure OrdredStp3_LogTheResults;
 
end;
/