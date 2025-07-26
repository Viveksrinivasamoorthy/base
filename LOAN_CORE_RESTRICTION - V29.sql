

DROP TABLE IF EXISTS LOAN_CORE_RESTRICTION;
CREATE TABLE LOAN_CORE_RESTRICTION
(
account_id VARCHAR(50) DEFAULT ''
,created_at DATE DEFAULT NULL
,created_by VARCHAR(50) DEFAULT 'OPS'
,reason_code VARCHAR(50) DEFAULT ''
,restriction VARCHAR(50) DEFAULT 'DRAWDOWN_BLOCKED'
,status VARCHAR(50) DEFAULT 'Active'
,updated_at DATE DEFAULT NULL
,updated_by VARCHAR(50) DEFAULT 'OPS'
,MonthOfImport VARCHAR(10) NOT NULL
);


DROP VIEW IF EXISTS cv_LOAN_CORE_RESTRICTION;
CREATE VIEW cv_LOAN_CORE_RESTRICTION AS
SELECT DISTINCT account_id,created_at,reason_code,updated_at,MonthOfImport
FROM (
SELECT DISTINCT 
GPA.NAME AS account_id
,A.UEN_Blacklist_Reason__c AS reason_code
,LCR.created_at AS created_at
,LCR.updated_at AS updated_at
,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')  
LEFT JOIN LOAN_CORE_RESTRICTION LCR ON LCR.account_id=GPA.Name 
WHERE A.UEN_Blacklist_Reason__c IS NOT NULL
AND A.UEN_Blacklist_Reason__c  IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up','AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation','Fraud - Internal')
AND EXISTS (SELECT 1 FROM LOAN_CORE_RESTRICTION B WHERE GPA.Name =B.account_id AND B.MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 2 MONTH),'%b-%y'))
AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
AND ( LLA.loan__Loan_Status__c like 'Active%'
OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)  
OR LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
)
UNION
SELECT 
GPA.NAME
,A.UEN_Blacklist_Reason__c
,LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
,LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
FROM NS_Account A
JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')
WHERE A.UEN_Blacklist_Reason__c IS NOT NULL
AND A.UEN_Blacklist_Reason__c  IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up','AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation','Fraud - Internal')
AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
AND ( LLA.loan__Loan_Status__c like 'Active%'
OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)  
OR LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) )
) CTE ; 


DROP VIEW IF EXISTS v_LOAN_CORE_RESTRICTION;
CREATE VIEW v_LOAN_CORE_RESTRICTION AS
SELECT account_id,created_at,created_by,reason_code,restriction,status,updated_at,updated_by
FROM LOAN_CORE_RESTRICTION
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') 
ORDER BY account_id;



DROP PROCEDURE IF EXISTS proc_LoanCoreRestriction;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_LoanCoreRestriction`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_LoanCoreRestriction
** Name: LOAN_CORE_RESTRICTION
** Desc: DM - LoanCoreRestriction
** Auth: Vivek Srinivasamorthy
** Date: 07-03-2025
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
        'proc_LoanCoreRestriction'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'LOAN_CORE_RESTRICTION';

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
        'proc_LoanCoreRestriction'
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
        'proc_LoanCoreRestriction'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (account_id,created_at,reason_code,updated_at, MonthOfImport)
                        SELECT DISTINCT account_id,created_at,reason_code,updated_at, MonthOfImport
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
        'proc_LoanCoreRestriction'
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
        'proc_LoanCoreRestriction'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;
