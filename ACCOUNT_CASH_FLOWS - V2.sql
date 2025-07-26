
DROP TABLE IF EXISTS ACCOUNT_CASH_FLOWS;
CREATE TABLE ACCOUNT_CASH_FLOWS(
ACCOUNT_NUMBER VARCHAR(50)
,REPORTING_DATE DATE
,CASH_FLOW_SEQUENCE INT DEFAULT NULL
,CASH_FLOW_DATE DATE
,DATA_SOURCE VARCHAR(20) DEFAULT 'Salesforce_CL_Loan'
,CASH_FLOW_TYPE VARCHAR(20)
,CASH_FLOW_AMOUNT DECIMAL(22,3)
,CCY_CODE VARCHAR(3) DEFAULT 'SGD'
,CUSTOMER_ID VARCHAR(100)
,BANK_PRODUCT_CODE VARCHAR(20) DEFAULT 'SMEL'
,BOOK_TYPE CHAR(1) DEFAULT 'B'
,COUNTRY_CODE VARCHAR(2) DEFAULT 'SG'
,DRCR_IND CHAR(1) DEFAULT 'D'
,ENTITY_CODE VARCHAR(20) DEFAULT 'SG01'
,MonthOfImport VARCHAR(10) NOT NULL
);

CREATE INDEX IDX_ACCOUNT_NUMBER ON ACCOUNT_CASH_FLOWS (ACCOUNT_NUMBER);

DROP VIEW IF EXISTS cv_CashFlow_Bullet;
CREATE VIEW cv_CashFlow_Bullet AS
SELECT *,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport FROM (
SELECT 
LLA.Loan_App_ID__c AS ACCOUNT_NUMBER
,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
,1 AS CASH_FLOW_SEQUENCE
,LLA.loan__Last_Installment_Date__c  AS CASH_FLOW_DATE
,'PRINCIPAL' AS CASH_FLOW_TYPE
,LLA.loan__Principal_Remaining__c  AS CASH_FLOW_AMOUNT
,A.User_ID__c  AS CUSTOMER_ID
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  
	AND LLA.GXS_Book__c=TRUE
	AND LLA.loan__Loan_Status__c LIKE 'Active%'
LEFT JOIN NS_loan__Repayment_Schedule__c LRS ON LRS.loan__Loan_Account__c = LLA.Id 
LEFT JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c P ON GA.genesis__Parent_Application__c=P.Id
LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID
WHERE P.genesis__CL_Product_Name__c IN ('Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)','CVF High Doc','CVF Low Doc','CVF Medium Doc','NIP WC Bullet','Overdraft LN - Citi(Single)','Purchase Order Financing')
UNION 
SELECT 
LLA.Loan_App_ID__c AS ACCOUNT_NUMBER
,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
,1 AS CASH_FLOW_SEQUENCE
,LLA.loan__Last_Installment_Date__c  AS CASH_FLOW_DATE
,'INTEREST' AS CASH_FLOW_TYPE
,CASE WHEN LLA.Payment_days_remaining__c <= 0 THEN
(
  (YEAR(LLA.loan__Last_Installment_Date__c) - YEAR(CURDATE())) * 360
  + (MONTH(LLA.loan__Last_Installment_Date__c) - MONTH(CURDATE())) * 30
  + (LEAST(30, DAY(LLA.loan__Last_Installment_Date__c)) - LEAST(30, DAY(CURDATE())))
) * LLA.Interest_Accrued_Per_Day_Declining__c
  + LLA.Total_Interest_Accrued__c 
ELSE LLA.Total_Interest_Accrued__c END AS CASH_FLOW_AMOUNT
,A.User_ID__c  AS CUSTOMER_ID
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  
	AND LLA.GXS_Book__c=TRUE
	AND LLA.loan__Loan_Status__c LIKE 'Active%'
LEFT JOIN NS_loan__Repayment_Schedule__c LRS ON LRS.loan__Loan_Account__c = LLA.Id 
LEFT JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c P ON GA.genesis__Parent_Application__c=P.Id
LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID
WHERE P.genesis__CL_Product_Name__c IN ('Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)','CVF High Doc','CVF Low Doc','CVF Medium Doc','NIP WC Bullet','Overdraft LN - Citi(Single)','Purchase Order Financing')
) CTE;



DROP VIEW IF EXISTS cv_CashFlowEMI;
CREATE VIEW cv_CashFlowEMI AS
WITH CTE AS 
(
SELECT A.User_ID__c  AS CUSTOMER_ID,LLA.Loan_App_ID__c AS ACCOUNT_NUMBER, LLA.loan__Principal_Remaining__c,LLA.loan__Last_Installment_Date__c,LRS.loan__Due_Date__c AS CASH_FLOW_DATE,LRS.loan__Due_Principal__c AS CASH_FLOW_AMOUNT
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  
	AND LLA.GXS_Book__c=TRUE
	AND LLA.loan__Loan_Status__c LIKE 'Active%'
JOIN NS_loan__Repayment_Schedule__c  LRS ON LRS.loan__Loan_Account__c = LLA.Id 
	AND LRS.loan__Is_Archived__c = FALSE
	AND LRS.loan__isPaid__c = FALSE
LEFT JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c P ON GA.genesis__Parent_Application__c=P.Id
LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID
WHERE P.genesis__CL_Product_Name__c NOT IN ('Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)','CVF High Doc','CVF Low Doc','CVF Medium Doc','NIP WC Bullet','Overdraft LN - Citi(Single)','Purchase Order Financing')
AND LLA.loan__Last_Installment_Date__c <> LRS.loan__Due_Date__c 
UNION
SELECT CUSTOMER_ID,ACCOUNT_NUMBER,loan__Principal_Remaining__c,loan__Last_Installment_Date__c,CASH_FLOW_DATE,loan__Principal_Remaining__c - COALESCE(Total_AdjustmentPrincipal,0) AS CASH_FLOW_AMOUNT
FROM
(
SELECT CUSTOMER_ID,ACCOUNT_NUMBER,COALESCE(loan__Principal_Remaining__c,0) AS loan__Principal_Remaining__c,loan__Last_Installment_Date__c,MAX(loan__Due_Date__c) AS CASH_FLOW_DATE,SUM(AdjustmentPrincipal) AS Total_AdjustmentPrincipal
FROM
(
SELECT A.User_ID__c  AS CUSTOMER_ID,LLA.Loan_App_ID__c AS ACCOUNT_NUMBER, LLA.loan__Principal_Remaining__c,LRS.loan__Due_Principal__c,LLA.loan__Last_Installment_Date__c,LRS.loan__Due_Date__c 
,LAG(loan__Due_Principal__c,1,0) OVER(PARTITION BY LLA.Loan_App_ID__c ORDER BY LRS.loan__Due_Date__c) AS AdjustmentPrincipal
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  
	AND LLA.GXS_Book__c=TRUE
	AND LLA.loan__Loan_Status__c LIKE 'Active%'
JOIN NS_loan__Repayment_Schedule__c  LRS ON LRS.loan__Loan_Account__c = LLA.Id 
	AND LRS.loan__Is_Archived__c = FALSE
	AND LRS.loan__isPaid__c = FALSE
LEFT JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c P ON GA.genesis__Parent_Application__c=P.Id
LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID
WHERE P.genesis__CL_Product_Name__c NOT IN ('Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)','CVF High Doc','CVF Low Doc','CVF Medium Doc','NIP WC Bullet','Overdraft LN - Citi(Single)','Purchase Order Financing')
) CTE 
GROUP BY CUSTOMER_ID,ACCOUNT_NUMBER,loan__Principal_Remaining__c,loan__Last_Installment_Date__c
) X
) 
SELECT 
	ACCOUNT_NUMBER
	,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
	,ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY CASH_FLOW_DATE ) AS CASH_FLOW_SEQUENCE
	,CASH_FLOW_DATE
	,'PRINCIPAL' AS CASH_FLOW_TYPE
	,CASH_FLOW_AMOUNT
	,CUSTOMER_ID
	,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport 
FROM CTE 
UNION
SELECT LLA.Loan_App_ID__c AS ACCOUNT_NUMBER
	,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
	,ROW_NUMBER() OVER (PARTITION BY LLA.Loan_App_ID__c ORDER BY LRS.loan__Due_Date__c ) AS CASH_FLOW_SEQUENCE
	,LRS.loan__Due_Date__c AS CASH_FLOW_DATE
	,'INTEREST' AS CASH_FLOW_TYPE
	,LRS.loan__Due_Interest__c AS CASH_FLOW_AMOUNT
	,A.User_ID__c  AS CUSTOMER_ID
	,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport 
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  
	AND LLA.GXS_Book__c=TRUE
	AND LLA.loan__Loan_Status__c LIKE 'Active%'
JOIN NS_loan__Repayment_Schedule__c  LRS ON LRS.loan__Loan_Account__c = LLA.Id 
	AND LRS.loan__Is_Archived__c = FALSE
	AND LRS.loan__isPaid__c = FALSE
LEFT JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c P ON GA.genesis__Parent_Application__c=P.Id
LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID
WHERE P.genesis__CL_Product_Name__c NOT IN ('Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)','CVF High Doc','CVF Low Doc','CVF Medium Doc','NIP WC Bullet','Overdraft LN - Citi(Single)','Purchase Order Financing')
ORDER BY CUSTOMER_ID,ACCOUNT_NUMBER,CASH_FLOW_TYPE DESC,CASH_FLOW_SEQUENCE;

DROP VIEW IF EXISTS cv_ACCOUNT_CASH_FLOWS;
CREATE VIEW cv_ACCOUNT_CASH_FLOWS AS 
WITH CTE AS (
SELECT ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,MonthOfImport FROM cv_CashFlowEMI
UNION
SELECT ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,MonthOfImport FROM cv_CashFlow_Bullet
) 
SELECT DISTINCT * FROM CTE 
ORDER BY ACCOUNT_NUMBER,CASH_FLOW_TYPE DESC,CASH_FLOW_SEQUENCE ;




DROP VIEW IF EXISTS v_ACCOUNT_CASH_FLOWS;
CREATE VIEW v_ACCOUNT_CASH_FLOWS AS
SELECT ACCOUNT_NUMBER,DATE_FORMAT(REPORTING_DATE,'%Y-%m-%d') REPORTING_DATE,CASH_FLOW_SEQUENCE,DATE_FORMAT(CASH_FLOW_DATE,'%Y-%m-%d') CASH_FLOW_DATE,DATA_SOURCE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CCY_CODE,CUSTOMER_ID,BANK_PRODUCT_CODE,BOOK_TYPE,COUNTRY_CODE,DRCR_IND,ENTITY_CODE,MonthOfImport
FROM ACCOUNT_CASH_FLOWS
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y')
ORDER BY ACCOUNT_NUMBER,CASH_FLOW_TYPE DESC,CASH_FLOW_SEQUENCE ;



DROP PROCEDURE IF EXISTS proc_AccountCashFlows;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_AccountCashFlows`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_AccountCashFlows
** Name: ACCOUNT_CASH_FLOWS
** Desc: DM - AccountCashFlows
** Auth: Vivek Srinivasamorthy
** Date: 17-06-2025
*******************************************************
** Change History
*******************************************************
** PR   Date        Author  Description 
** --   --------   -------   -------------------
** 1    
*******************************************************/
    DECLARE LV_TABLE_NAME,LV_STATUS VARCHAR(255);
    DECLARE ErrorMessage TEXT DEFAULT '';
    DECLARE done BOOLEAN;
    DECLARE LSQL TEXT DEFAULT '';
    DECLARE l_code VARCHAR(5) DEFAULT '';
    DECLARE l_message TEXT DEFAULT '';
    DECLARE l_rowcount int default 0;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
        l_code = returned_sqlstate,
        l_message = message_text;
        CALL error_handler('Error',
        'An Exception Occured',
        l_code,
        CONCAT(l_message, " ---while running-", COALESCE(@LSQL, '')),
        'proc_AccountCashFlows'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'ACCOUNT_CASH_FLOWS';

    CALL sys.table_exists(DATABASE(), LV_TABLE_NAME,@exist); 
  
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DELETE FROM ",LV_TABLE_NAME," WHERE MonthOfImport = CONCAT(DATE_FORMAT(DATE(NOW() - INTERVAL 4 MONTH),'%b'),'-',DATE_FORMAT(DATE(NOW() - INTERVAL 4 MONTH),'%y') )");   
    SET @LSQL = LSQL;
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
    
    IF l_code=''
    THEN
        CALL error_handler(
        'Info',
        'Success',
        '',
        CONCAT("Table: ",LV_TABLE_NAME," - Deleted Month: ",DATE_FORMAT(DATE(NOW() - INTERVAL 4 MONTH),'%b-%y')," counts:",l_rowcount),
        'proc_AccountCashFlows'
        ,CONCAT(IN_JOB_ID));
    END IF; 

    SET LSQL='',l_code='',l_message='', l_rowcount=0, @LV_ACTIVE_MONTH=0;
    SET LSQL = CONCAT("SELECT COUNT(*) INTO @LV_ACTIVE_MONTH FROM ",LV_TABLE_NAME," WHERE MonthOfImport = CONCAT(DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b'),'-',DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%y') )");   
    SET @LSQL = LSQL;
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    DEALLOCATE PREPARE STMT2; 
    
    IF l_code='' AND @LV_ACTIVE_MONTH<>0
    THEN
        CALL error_handler(
        'Info',
        'Success',
        '',
        CONCAT("Table: ",LV_TABLE_NAME," - Already Exists: ",DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y')," counts:",@LV_ACTIVE_MONTH),
        'proc_AccountCashFlows'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,MonthOfImport)
                        SELECT DISTINCT ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,MonthOfImport
                        FROM  cv_",LV_TABLE_NAME,";");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
    
    COMMIT;
    
    IF l_code=''
    THEN
        SET LV_STATUS = 'Completed';
        CALL error_handler(
        'Info',
        'Success',
        '',
        CONCAT("Table: ",LV_TABLE_NAME," - Inserted Month: ",DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y')," counts:",l_rowcount),
        'proc_AccountCashFlows'
        ,CONCAT(IN_JOB_ID));
    ELSE
        SET LV_STATUS = 'Error';
    END IF; 

    SET LSQL='',l_code='',l_message='';
    SET LSQL = CONCAT("UPDATE NS_Job_Detail SET RowCount=",l_rowcount,",Status='",LV_STATUS,"',EndDateTime=NOW() WHERE JobID=",IN_JOB_ID," AND NS_Name='",LV_TABLE_NAME,"';");   
    SET @LSQL = LSQL;
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 

    IF l_code=''
    THEN
        CALL error_handler(
        'Info',
        'Success',
        '',
        CONCAT("Table: NS_Job_Detail - Updated RowCount Successfully"),
        'proc_AccountCashFlows'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;

