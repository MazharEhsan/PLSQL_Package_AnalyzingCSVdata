CREATE OR REPLACE package body Pkg_AnlzeTwoStkCSVs_LogResults

--Usage notes:
--Refer to the notes in the specs of this package

IS

PROCEDURE OrdredStp1_DirFor_Exchange(FilePathInV IN VARCHAR2) IS

--Usage notes:
--Sample execution: exec Pkg_AnlzeTwoStkCSVs_LogResults.OrdredStp1_DirFor_Exchange('''d:\exchange_data\''');
--Any valid path and folder name can be used in the above execution command such as d:\exchange_data\ or d:\exchange_data_day1\ or d:\exchange_data_Nov1\
--Make sure to store the valid and required CSV data file named exchange_data.csv in that folder cited above
--The folder location of the exchange_data.csv is the input in this procedure
--This is required to be executed everyday to make available the stock data in the exchange_data.csv in Oracle db using the external table: exchange_data_extrnl_fromcsv
    
BEGIN
  EXECUTE IMMEDIATE 'create or replace directory EXCHANGEDATACSVPATH as '||FilePathInV;

  EXCEPTION
    WHEN OTHERS THEN
    RETURN;

END;

PROCEDURE OrdredStp2_DirFor_depository(FilePathInV IN VARCHAR2) IS

--Usage notes:
--Sample execution: exec Pkg_AnlzeTwoStkCSVs_LogResults.OrdredStp1_DirFor_Exchange('''d:\depository_data\''');
--Any valid path and folder name can be used in the above execution command such as d:\depository_data\ or d:\depository_data_day1\ or d:\depository_data_Nov1\
--Make sure to store the valid and required CSV data file named depository_data.csv in that folder cited above
--The folder location of the depository_data.csv is the input in this procedure
--This is required to be executed everyday to make available the stock data in the depository_data.csv in Oracle db using the external table: depository_data_extrnl_fromcsv
    
BEGIN

EXECUTE IMMEDIATE 'create or replace directory DEPOSITORYDATACSVPATH as '||FilePathInV;

  EXCEPTION
    WHEN OTHERS THEN
    RETURN;
END;

PROCEDURE OrdredStp3_LogTheResults IS

--Usage note: To execute this last program: exec Pkg_AnlzeTwoStkCSVs_LogResults.OrdredStp3_LogTheResults;

TotalRows_exchange_data NUMBER(30);
TotalRows_depository_data NUMBER(30);
MissingRows_In_exchange_data NUMBER(30);
MissingRows_In_depository_data NUMBER(30);
TotalRows_Matched NUMBER(30);
TotalRows_MtchdButDiffStkCntNm NUMBER(30);
The_SQLERRM VARCHAR2(4000);
TheLogDesc VARCHAR2(400);
TheDups NUMBER(30);
Inconsistent_Stock_ID_Name NUMBER(30);
TheRetrunFromLevel1_3EdgeCases NUMBER(1);
TheRetrunFromLevel2_3EdgeCases NUMBER(1);

BEGIN

EXECUTE IMMEDIATE 'TRUNCATE TABLE TheLogOfEdgeCasesAndCSVFileIssues'; --To clear the log tables before an execute
EXECUTE IMMEDIATE 'TRUNCATE TABLE TheMainComparisonLogOfTwoCSVs'; --To clear the log tables before an execute

BEGIN 
-- Error Handling - the Edge Cases Level 1- Missing or corrupt CSV files with Format errors or corrupt data
SELECT count(*) INTO TotalRows_exchange_data FROM exchange_data_extrnl_fromcsv;

IF TotalRows_exchange_data = 0 THEN
TheRetrunFromLevel1_3EdgeCases := 1;
      TheLogDesc := 'Error handling and edge Case-1: The status log of the exchange_data.CSV. Missing or corrupt file with format error. No data avaialble';
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc)
      VALUES 
      (1, TheLogDesc);
      COMMIT;
END IF;

EXCEPTION
      WHEN OTHERS THEN
      TheRetrunFromLevel1_3EdgeCases := 1;
      The_SQLERRM := SQLERRM;
      TheLogDesc := 'Error handling and edge Case-1: The status log of the exchange_data.CSV. Missing or corrupt file, folders or format errors (e.g., invalid CSV structure) and CSV data not available because of this';
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, MissngOrIvldCSV_exchangedata)
      VALUES 
      (1, TheLogDesc, The_SQLERRM);
      COMMIT;
END;

BEGIN
-- Error Handling - the Edge Cases - Missing or corrupt CSV files with Format errors or corrupt data
SELECT count(*) INTO TotalRows_depository_data FROM depository_data_extrnl_fromcsv;
IF TotalRows_depository_data = 0 THEN
      TheRetrunFromLevel1_3EdgeCases := 1;
      TheLogDesc := 'Error handling and edge Case-1: The status log of the depository_data.CSV. Missing or corrupt file with format error. No data avaialble';
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc)
      VALUES 
      (1, TheLogDesc);
      COMMIT;
END IF;
   EXCEPTION
      WHEN OTHERS THEN
      TheRetrunFromLevel1_3EdgeCases := 1;
      The_SQLERRM := null;
      The_SQLERRM := SQLERRM;
      TheLogDesc := 'Error handling and edge Case-1: The status log of the depository_data.CSV. Missing or corrupt files, folders or format errors (e.g., invalid CSV structure) and CSV data not available because of this';
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, MissngOrIvldCSV_depositorydata) 
      VALUES 
      (1, TheLogDesc, The_SQLERRM);
      COMMIT;
      RETURN;

END;

--Return from Level 1 Edge Cases check--
IF TheRetrunFromLevel1_3EdgeCases = 1 THEN
 RETURN;
END IF;
--Return from Level 1 Edge Cases check--

-- Error Handling - The next Edge Case, level 2 - Checking for duplicate data / rows - a potential possibility of inconsistent data

BEGIN
    TheLogDesc := 'Error handling the Edge Case-2: Duplicate rows or duplicate sets of User_ID, Stock_ID, Stock_Name - in the exchange_data.CSV';
    
    BEGIN
        SELECT COUNT(*) INTO TheDups FROM
        (
        SELECT User_ID, Stock_ID, Stock_Name, COUNT(*) FROM exchange_data_extrnl_fromcsv
        GROUP BY User_ID, Stock_ID, Stock_Name
        HAVING COUNT(*) > 1
        ) TheDups;
    EXCEPTION
        WHEN OTHERS THEN
        RETURN;
    END;

    IF TheDups > 0 THEN
      TheRetrunFromLevel2_3EdgeCases := 1;
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, Dup_User_ID_exchange_data, Dup_Stock_ID_exchange_data, Dup_Stock_Name_exchange_data, Dup_Stock_Count_exchange_data) 

      (
          SELECT 2, TheLogDesc, ex.User_ID, ex.Stock_ID, ex.Stock_Name, ex.Stock_Count FROM exchange_data_extrnl_fromcsv ex,
            (
            SELECT User_ID, Stock_ID, Stock_Name, COUNT(*) FROM exchange_data_extrnl_fromcsv
            GROUP BY User_ID, Stock_ID, Stock_Name
            HAVING COUNT(*) > 1
            ) cte
            WHERE cte.User_ID = ex.User_ID AND cte.Stock_ID = ex.Stock_ID AND cte.Stock_Name = ex.Stock_Name
      );
      
    END IF;
END;

BEGIN
    TheLogDesc := 'Error handling the Edge Case-2: Duplicate rows or duplicate sets of User_ID, Stock_ID, Stock_Name - in the depository_data.CSV';
    
    BEGIN
        SELECT COUNT(*) INTO TheDups FROM
        (
        SELECT User_ID, Stock_ID, Stock_Name, COUNT(*) FROM depository_data_extrnl_fromcsv
        GROUP BY User_ID, Stock_ID, Stock_Name
        HAVING COUNT(*) > 1
        ) TheDups;
    EXCEPTION
        WHEN OTHERS THEN
        RETURN;
    END;

    IF TheDups > 0 THEN

      TheRetrunFromLevel2_3EdgeCases := 1;
      INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, Dup_User_ID_depository_data, Dup_Stock_ID_depository_data, Dup_Stock_Name_depository_data,  Dup_Stock_Count_dep_data) 

      (
          SELECT 2, TheLogDesc, de.User_ID, de.Stock_ID, de.Stock_Name, de.Stock_Count FROM depository_data_extrnl_fromcsv de,
            (
            SELECT User_ID, Stock_ID, Stock_Name, COUNT(*) FROM depository_data_extrnl_fromcsv
            GROUP BY User_ID, Stock_ID, Stock_Name
            HAVING COUNT(*) > 1
            ) cte
            WHERE cte.User_ID = de.User_ID AND cte.Stock_ID = de.Stock_ID AND cte.Stock_Name = de.Stock_Name
      );
      
    END IF;
END;

-- Error Handling - The next Edge Case, level 3 - Checking for possibility of Varying Stock_Name for a Stock_ID in the rows - - a case of corrupt data - a potential possibility of inconsistent data
TheLogDesc := 'Error handling the Edge Case-3: Varying Stock_Name for a Stock_ID in the rows - a case of corrupt data - in the exchange_data.CSV';
WITH CTE AS
(
SELECT e.STOCK_ID, e.STOCK_NAME FROM exchange_data_extrnl_fromcsv e,
(
    SELECT Stock_ID, CNT, (SELECT Stock_Name FROM exchange_data_extrnl_fromcsv WHERE Stock_ID = q1.Stock_ID AND ROWNUM = 1) Stock_Name_Occurance1 FROM
    (
    SELECT Stock_ID, COUNT(*) CNT FROM exchange_data_extrnl_fromcsv GROUP BY Stock_ID HAVING COUNT(*) > 1
    ) q1
) qm
WHERE qm.Stock_ID = e.Stock_ID AND qm.Stock_Name_Occurance1 <> e.Stock_Name
)
SELECT COUNT(*) INTO Inconsistent_Stock_ID_Name FROM CTE;

IF Inconsistent_Stock_ID_Name > 0 THEN
  TheRetrunFromLevel2_3EdgeCases := 1;
  INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, Incst_Stock_ID_exchange_data, Incst_Stock_Name_exchange_data)
  (
    SELECT 3, TheLogDesc, Stock_ID, Stock_Name FROM exchange_data_extrnl_fromcsv WHERE Stock_ID IN
    (
    SELECT e.Stock_ID FROM exchange_data_extrnl_fromcsv e,
    (
        SELECT Stock_ID, CNT, (SELECT Stock_Name FROM exchange_data_extrnl_fromcsv WHERE Stock_ID = q1.Stock_ID AND ROWNUM = 1) Stock_Name_Occurance1 FROM
        (
        SELECT Stock_ID, COUNT(*) CNT FROM exchange_data_extrnl_fromcsv GROUP BY Stock_ID HAVING COUNT(*) > 1
        ) q1
    ) qm
    WHERE qm.Stock_ID = e.Stock_ID AND qm.Stock_Name_Occurance1 <> e.Stock_Name
    )
  );
END IF;

TheLogDesc := 'Error handling the Edge Case-3: Varying Stock_Name for a Stock_ID in the rows - a case of corrupt data - in the depository_data.CSV';
WITH CTE AS
(
SELECT d.STOCK_ID, d.STOCK_NAME FROM depository_data_extrnl_fromcsv d,
(
    SELECT Stock_ID, CNT, (SELECT Stock_Name FROM depository_data_extrnl_fromcsv WHERE Stock_ID = q1.Stock_ID AND ROWNUM = 1) Stock_Name_Occurance1 FROM
    (
    SELECT Stock_ID, COUNT(*) CNT FROM depository_data_extrnl_fromcsv GROUP BY Stock_ID HAVING COUNT(*) > 1
    ) q1
) qm
WHERE qm.Stock_ID = d.Stock_ID AND qm.Stock_Name_Occurance1 <> d.Stock_Name
)
SELECT COUNT(*) INTO Inconsistent_Stock_ID_Name FROM CTE;

IF Inconsistent_Stock_ID_Name > 0 THEN
  TheRetrunFromLevel2_3EdgeCases := 1;
  INSERT INTO TheLogOfEdgeCasesAndCSVFileIssues (LogIDOrderedStep_Serial, LogIDOrderedStep_Desc, Incst_Stock_ID_depstry_data, Incst_Stock_Name_depstry_data)
  (

    SELECT 3, TheLogDesc, Stock_ID, Stock_Name FROM depository_data_extrnl_fromcsv WHERE Stock_ID IN
    (
    SELECT d.Stock_ID FROM depository_data_extrnl_fromcsv d,
    (
        SELECT Stock_ID, CNT, (SELECT Stock_Name FROM depository_data_extrnl_fromcsv WHERE Stock_ID = q1.Stock_ID AND ROWNUM = 1) Stock_Name_Occurance1 FROM
        (
        SELECT Stock_ID, COUNT(*) CNT FROM depository_data_extrnl_fromcsv GROUP BY Stock_ID HAVING COUNT(*) > 1
        ) q1
    ) qm
    WHERE qm.Stock_ID = d.Stock_ID AND qm.Stock_Name_Occurance1 <> d.Stock_Name
    )

  );
  
END IF;
---------------------------------------------------------The Edge Case Mismatch Stock_ID  Stock_Name end----------------------------------------------

--Return from Level 2 and 3 Edge Cases check--
IF TheRetrunFromLevel2_3EdgeCases = 1 THEN
 RETURN;
END IF;
--Return from Level 2 and 3 Edge Cases check--

WITH MissingRows_In_exchange_data as
(
SELECT User_ID, Stock_ID, Stock_Name FROM depository_data_extrnl_fromcsv
minus
SELECT a.User_ID, a.Stock_ID, a.Stock_Name FROM exchange_data_extrnl_fromcsv a, depository_data_extrnl_fromcsv b
where a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID
and a.Stock_Name = b.Stock_Name
)
select count(*) into MissingRows_In_exchange_data from MissingRows_In_exchange_data;

WITH MissingRows_In_depository_data as
(
SELECT User_ID, Stock_ID, Stock_Name FROM exchange_data_extrnl_fromcsv
minus
SELECT a.User_ID, a.Stock_ID, a.Stock_Name FROM exchange_data_extrnl_fromcsv a, depository_data_extrnl_fromcsv b
where a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID
and a.Stock_Name = b.Stock_Name
)
select count(*) into MissingRows_In_depository_data from MissingRows_In_depository_data;

SELECT count(*) into TotalRows_Matched FROM exchange_data_extrnl_fromcsv a, depository_data_extrnl_fromcsv b
where a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID
and a.Stock_Name = b.Stock_Name
and a.Stock_Count = b.Stock_Count;

SELECT count(*) into TotalRows_MtchdButDiffStkCntNm FROM exchange_data_extrnl_fromcsv a,
depository_data_extrnl_fromcsv b
where (a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID)
and (a.Stock_Name <> b.Stock_Name or a.Stock_Count <> b.Stock_Count);

BEGIN
insert into TheLogOfEdgeCasesAndCSVFileIssues
(
LogIDOrderedStep_Serial,
LogIDOrderedStep_Desc,
TotalRows_exchange_data,
TotalRows_depository_data,
MissingRows_In_exchange_data,
MissingRows_In_depository_data,
TotalRows_Matched,
TotalRows_MtchdButDiffStkCntNm
)
values
(
4,
'Total Rows Summary in the exchange_data.CSV and the depository_data.CSV. Reaching to the logging of this step means that the CSV data does not have issues of inconsistent, duplicate and corrupt data',
TotalRows_exchange_data,
TotalRows_depository_data,
MissingRows_In_exchange_data,
MissingRows_In_depository_data,
TotalRows_Matched,
TotalRows_MtchdButDiffStkCntNm
);
END;

INSERT INTO TheMainComparisonLogOfTwoCSVs
(
MsMatchedStkCnt_User_ID,
MsMatchedStkCnt_Stock_ID,
MsMatchedStkCnt_Stock_Name,
MsMatchedStkCnt_Stock_Count_e,
MsMatchedStkCnt_Stock_Count_d
)

SELECT a.User_ID, a.Stock_ID, a.Stock_Name, a.Stock_Count, b.Stock_Count FROM exchange_data_extrnl_fromcsv a,
depository_data_extrnl_fromcsv b
where (a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID)
and (a.Stock_Name <> b.Stock_Name or a.Stock_Count <> b.Stock_Count);


INSERT INTO TheMainComparisonLogOfTwoCSVs
(
MatchedStkCnt_User_ID,
MatchedStkCnt_Stock_ID,
MatchedStkCnt_Stock_Name,
MatchedStkCnt_Stock_Count
)

SELECT a.User_ID, a.Stock_ID, a.Stock_Name, a.Stock_Count FROM exchange_data_extrnl_fromcsv a,
depository_data_extrnl_fromcsv b
where a.User_ID = b.User_ID and a.Stock_ID = b.Stock_ID and a.Stock_Name = b.Stock_Name and a.Stock_Count = b.Stock_Count;

COMMIT;

END;

END Pkg_AnlzeTwoStkCSVs_LogResults;
/