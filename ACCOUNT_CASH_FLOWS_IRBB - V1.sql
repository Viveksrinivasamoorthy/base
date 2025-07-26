
DROP TABLE IF EXISTS ACCOUNT_CASH_FLOWS_IRBB;
CREATE TABLE ACCOUNT_CASH_FLOWS_IRBB(
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
,DPD_no_grace_period__c INT
,MonthOfImport VARCHAR(10) NOT NULL
);

CREATE INDEX IDX_ACCOUNT_NUMBER ON ACCOUNT_CASH_FLOWS_IRBB (ACCOUNT_NUMBER);


DROP VIEW IF EXISTS cv_ACCOUNT_CASH_FLOWS_IRBB;
CREATE VIEW cv_ACCOUNT_CASH_FLOWS_IRBB AS 
WITH CTE AS (
SELECT ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,CAST(LLA.DPD_no_grace_period__c AS UNSIGNED) AS DPD_no_grace_period__c,MonthOfImport 
FROM ACCOUNT_CASH_FLOWS ACF
JOIN NS_loan__Loan_Account__c LLA ON LLA.Loan_App_ID__c =ACF.ACCOUNT_NUMBER AND LLA.DPD_no_grace_period__c < 30 
) 
SELECT DISTINCT * FROM CTE 
ORDER BY ACCOUNT_NUMBER,CASH_FLOW_TYPE DESC,CASH_FLOW_SEQUENCE;




DROP VIEW IF EXISTS v_ACCOUNT_CASH_FLOWS_IRBB;
CREATE VIEW v_ACCOUNT_CASH_FLOWS_IRBB AS
SELECT DISTINCT ACCOUNT_NUMBER,DATE_FORMAT(REPORTING_DATE,'%Y-%m-%d') REPORTING_DATE,CASH_FLOW_SEQUENCE,DATE_FORMAT(CASH_FLOW_DATE,'%Y-%m-%d') CASH_FLOW_DATE,DATA_SOURCE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CCY_CODE,CUSTOMER_ID,BANK_PRODUCT_CODE,BOOK_TYPE,COUNTRY_CODE,DRCR_IND,ENTITY_CODE,DPD_no_grace_period__c,MonthOfImport
FROM ACCOUNT_CASH_FLOWS_IRBB
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y')
ORDER BY ACCOUNT_NUMBER,CASH_FLOW_TYPE DESC,CASH_FLOW_SEQUENCE ;



DROP PROCEDURE IF EXISTS proc_AccountCashFlowsIRBB;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_AccountCashFlowsIRBB`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_AccountCashFlowsIRBB
** Name: ACCOUNT_CASH_FLOWS_IRBB
** Desc: DM - AccountCashFlowsIRBB
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
        'proc_AccountCashFlowsIRBB'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'ACCOUNT_CASH_FLOWS_IRBB';

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
        'proc_AccountCashFlowsIRBB'
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
        'proc_AccountCashFlowsIRBB'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,DPD_no_grace_period__c,MonthOfImport)
                        SELECT DISTINCT ACCOUNT_NUMBER,REPORTING_DATE,CASH_FLOW_SEQUENCE,CASH_FLOW_DATE,CASH_FLOW_TYPE,CASH_FLOW_AMOUNT,CUSTOMER_ID,DPD_no_grace_period__c,MonthOfImport
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
        'proc_AccountCashFlowsIRBB'
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
        'proc_AccountCashFlowsIRBB'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;

