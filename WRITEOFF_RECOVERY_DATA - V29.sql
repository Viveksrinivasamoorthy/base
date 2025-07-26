
DROP TABLE IF EXISTS WRITEOFF_RECOVERY_DATA;
CREATE TABLE WRITEOFF_RECOVERY_DATA(
ENTITY_CODE VARCHAR(20) DEFAULT 'SG01'
,REPORTING_DATE DATE DEFAULT NULL
,EXPOSURE_ID VARCHAR(50) DEFAULT ''
,BANK_PRODUCT_CODE VARCHAR(20) DEFAULT 'SMEL'
,CUSTOMER_ID VARCHAR(100) DEFAULT ''
,TXN_CCY_CODE VARCHAR(3) DEFAULT ''
,CONTRACTUAL_WRITEOFF_AMT DECIMAL(18,2) DEFAULT NULL
,DECEASED_WRITEOFF_AMT DECIMAL(18,2) DEFAULT NULL
,BANKRUPTCY_WRITEOFF_AMT DECIMAL(18,2) DEFAULT NULL
,OTHER_WRITEOFF_AMT DECIMAL(18,2) DEFAULT NULL
,RECOVERY_AMT DECIMAL(18,2) DEFAULT NULL
,WRITEOFF_DATE DATE DEFAULT NULL
,MonthOfImport VARCHAR(20) NOT NULL
);



DROP VIEW IF EXISTS cv_WRITEOFF_RECOVERY_DATA;
CREATE VIEW cv_WRITEOFF_RECOVERY_DATA AS
(
SELECT DISTINCT 
DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
,LLA.Loan_App_ID__c  AS EXPOSURE_ID
,A.User_ID__c AS CUSTOMER_ID
,LLA.Currency__c AS TXN_CCY_CODE
,LLA.loan__Charged_Off_Principal__c + LLA.loan__Charged_Off_Interest__c + LLA.loan__Charged_Off_Fees__c AS CONTRACTUAL_WRITEOFF_AMT
,SUM(LPT.loan__Principal__c) + SUM(LPT.loan__Interest__c) AS RECOVERY_AMT
,LLA.loan__Charged_Off_Date__c AS WRITEOFF_DATE
,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID
WHERE LLA.Loan__Loan_Status__c='Closed- Written Off' 
AND (LLA.loan__Charged_Off_Principal__c + LLA.loan__Charged_Off_Interest__c + LLA.loan__Charged_Off_Fees__c >0)
AND ( LLA.loan__Charged_Off_Date__c >= '2025-04-15' OR LPT.CreatedDate  >= '2025-04-15' )
AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
GROUP BY DATE(NOW() - INTERVAL 1 DAY),LLA.Loan_App_ID__c,A.User_ID__c,LLA.Currency__c,LLA.loan__Charged_Off_Principal__c ,LLA.loan__Charged_Off_Interest__c ,LLA.loan__Charged_Off_Fees__c,LLA.loan__Charged_Off_Date__c
);

DROP VIEW IF EXISTS v_WRITEOFF_RECOVERY_DATA;
CREATE VIEW v_WRITEOFF_RECOVERY_DATA AS
(
SELECT DISTINCT ENTITY_CODE, DATE_FORMAT(REPORTING_DATE,'%Y-%m-%d') AS REPORTING_DATE, EXPOSURE_ID, BANK_PRODUCT_CODE, CUSTOMER_ID, TXN_CCY_CODE, CONTRACTUAL_WRITEOFF_AMT, DECEASED_WRITEOFF_AMT, BANKRUPTCY_WRITEOFF_AMT, OTHER_WRITEOFF_AMT, RECOVERY_AMT,WRITEOFF_DATE
FROM  WRITEOFF_RECOVERY_DATA
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') 
ORDER BY CUSTOMER_ID
);

DROP PROCEDURE IF EXISTS proc_WriteoffRecoveryData;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_WriteoffRecoveryData`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_WriteoffRecoveryData
** Name: WRITEOFF_RECOVERY_DATA
** Desc: DM - WriteoffRecoveryData
** Auth: Vivek Srinivasamorthy
** Date: 20-01-2025
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
        'proc_WriteoffRecoveryData'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'WRITEOFF_RECOVERY_DATA';

    CALL sys.table_exists(DATABASE(), LV_TABLE_NAME,@exist); 

    SET SQL_SAFE_UPDATES = 0;
  
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
        'proc_WriteoffRecoveryData'
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
        'proc_WriteoffRecoveryData'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;
    
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (REPORTING_DATE,EXPOSURE_ID,CUSTOMER_ID,TXN_CCY_CODE,CONTRACTUAL_WRITEOFF_AMT,RECOVERY_AMT,WRITEOFF_DATE,MonthOfImport)
						SELECT DISTINCT REPORTING_DATE,EXPOSURE_ID,CUSTOMER_ID,TXN_CCY_CODE,CONTRACTUAL_WRITEOFF_AMT,RECOVERY_AMT,WRITEOFF_DATE,MonthOfImport
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
        'proc_WriteoffRecoveryData'
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
        'proc_WriteoffRecoveryData'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;

