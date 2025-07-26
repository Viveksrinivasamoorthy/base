DROP TABLE IF EXISTS CREDIT_FACILITY_MASTER;
CREATE TABLE CREDIT_FACILITY_MASTER 
(
`SYNDICATED_IND` CHAR(1) DEFAULT '',
`DATA_SOURCE` VARCHAR(20) DEFAULT 'Salesforce_CL_Loan',
`Entity_Code` VARCHAR(20) DEFAULT 'SG01',
`FACILITY_ID` VARCHAR(25) DEFAULT '',
`REPORTING_DATE` DATE,
`CREDIT_LINE_DESC` VARCHAR(100) DEFAULT '',
`CREDIT_LINE_ORIG_DATE` DATE,
`CREDIT_LINE_TYPE` VARCHAR(20) DEFAULT '',
`CREDIT_LINE_PURPOSE_CODE` VARCHAR(100) DEFAULT '',
`CREDIT_LINE_PURPOSE_DESC` VARCHAR(100) DEFAULT '',
`PARENT_CREDIT_LINE_CODE` VARCHAR(20) DEFAULT '',
`DRAW_TILL_DATE` DATE,
`ORIG_CREDIT_LINE_CODE` VARCHAR(100) DEFAULT '',
`FACILITY_CLASSIFICATION_IND` CHAR(1) DEFAULT 'C',
`REVOCABLE_STATUS_CODE` VARCHAR(20) DEFAULT 'UNC',
`DRAW_DATE_LIMIT_AVAIL` DATE,
`FACILITY_COMMITTED_FLAG` CHAR(1) DEFAULT '',
`EXPIRY_DATE` DATE,
`END_DATE` DATE,
`START_DATE` DATE,
`MonthOfImport` VARCHAR(10) NOT NULL
);



DROP VIEW IF EXISTS cv_MasterChildApp;
CREATE VIEW cv_MasterChildApp AS
SELECT DISTINCT 
	CUSTOMER_ID
	,FACILITY_ID
	,CASE WHEN LIMIT_COMMITTED_AMT = LIMIT_UTILIZED_AMT THEN 'Y' ELSE '' END AS FACILITY_COMMITTED_FLAG
	,REPORTING_DATE
	,CREDIT_LINE_DESC
	,CREDIT_LINE_ORIG_DATE
	,CREDIT_LINE_TYPE
	,CREDIT_LINE_PURPOSE_CODE
	,CREDIT_LINE_PURPOSE_DESC
	,DRAW_TILL_DATE
	,ORIG_CREDIT_LINE_CODE
	,DRAW_DATE_LIMIT_AVAIL
	,EXPIRY_DATE
	,START_DATE
	,END_DATE
	,MonthOfImport
FROM (
	SELECT DISTINCT 
		CUSTOMER_ID
		,FACILITY_ID
		,CASE WHEN BANK_SUB_PRODUCT_CODE IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)') THEN SUM(LIMIT_COMMITTED_AMT)  ELSE MIN(LIMIT_COMMITTED_AMT)  END AS LIMIT_COMMITTED_AMT
		,SUM(LIMIT_UTILIZED_AMT) AS LIMIT_UTILIZED_AMT
		,REPORTING_DATE
		,CREDIT_LINE_DESC
		,CREDIT_LINE_ORIG_DATE
		,CREDIT_LINE_TYPE
		,CREDIT_LINE_PURPOSE_CODE
		,CREDIT_LINE_PURPOSE_DESC
		,CASE WHEN DRAW_TILL_DATE < REPORTING_DATE THEN DATE(REPORTING_DATE + INTERVAL 2 MONTH) ELSE DRAW_TILL_DATE END AS DRAW_TILL_DATE 
		,ORIG_CREDIT_LINE_CODE
		,DRAW_DATE_LIMIT_AVAIL
		,CASE WHEN EXPIRY_DATE < REPORTING_DATE THEN DATE(REPORTING_DATE + INTERVAL 2 MONTH) ELSE EXPIRY_DATE END AS EXPIRY_DATE
		,START_DATE
		,MAX(END_DATE) AS END_DATE
		,MonthOfImport
	FROM (
	SELECT DISTINCT 
	A.User_ID__c AS CUSTOMER_ID
	,P.Name AS FACILITY_ID
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc','Purchase Order Financing','Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)') THEN  P.genesis__Loan_Amount__c 
	  WHEN P.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)') THEN  LLA.loan__Principal_Remaining__c 
		ELSE NULL END AS LIMIT_COMMITTED_AMT
	,LLA.loan__Principal_Remaining__c AS LIMIT_UTILIZED_AMT 
	,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS CREDIT_LINE_DESC
	-- ,P.genesis__CL_Product_Name__c AS CREDIT_LINE_DESC
	,P.Application_Approved_Date__c AS CREDIT_LINE_ORIG_DATE
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)')  THEN 'TERM_LOAN' ELSE 'REVOLVING_CREDIT' END AS CREDIT_LINE_TYPE
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)')  THEN 'OTH' ELSE 'TF' END AS CREDIT_LINE_PURPOSE_CODE
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS CREDIT_LINE_PURPOSE_DESC
	,COALESCE(P.Temp_Maturity_Date__c,P.genesis__Maturity_Date__c) AS DRAW_TILL_DATE
	,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS  ORIG_CREDIT_LINE_CODE
	,P.Application_Approved_Date__c AS DRAW_DATE_LIMIT_AVAIL
	,COALESCE(P.Temp_Maturity_Date__c,P.genesis__Maturity_Date__c) AS EXPIRY_DATE
	,P.Application_Approved_Date__c AS START_DATE
	,LLA.loan__Last_Installment_Date__c AS END_DATE
	,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
	,LLA.Product_Code__c AS BANK_SUB_PRODUCT_CODE 
	FROM NS_Account A
	JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
	JOIN NS_genesis__Applications__c GA ON GA.Id = LLA.genesis_app_Id__c
	LEFT JOIN NS_genesis__Applications__c P ON P.Id = GA.genesis__Parent_Application__c 
	LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off') 
	WHERE 1=1 -- GPA.genesis__status__c='Approved' 
	AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
	OR (LLA.loan__Loan_Status__c like 'Active%') 
	OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)  
	) 
	AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
	) CTE -- WHERE FACILITY_ID='APP-0000070033'
	GROUP BY CUSTOMER_ID,FACILITY_ID, REPORTING_DATE, CREDIT_LINE_DESC, CREDIT_LINE_ORIG_DATE, CREDIT_LINE_TYPE, CREDIT_LINE_PURPOSE_CODE, CREDIT_LINE_PURPOSE_DESC, DRAW_TILL_DATE, ORIG_CREDIT_LINE_CODE, DRAW_DATE_LIMIT_AVAIL, EXPIRY_DATE, START_DATE, MonthOfImport
) X;
 
 
DROP VIEW IF EXISTS cv_MasterParentApp;
CREATE VIEW cv_MasterParentApp AS
SELECT DISTINCT 
	FACILITY_ID
	,'N' AS FACILITY_COMMITTED_FLAG
	,REPORTING_DATE
	,CREDIT_LINE_DESC
	,CREDIT_LINE_ORIG_DATE
	,CREDIT_LINE_TYPE
	,CREDIT_LINE_PURPOSE_CODE
	,CREDIT_LINE_PURPOSE_DESC
	,CASE WHEN DRAW_TILL_DATE < REPORTING_DATE THEN DATE(REPORTING_DATE + INTERVAL 2 MONTH) ELSE DRAW_TILL_DATE END AS DRAW_TILL_DATE 
	,ORIG_CREDIT_LINE_CODE
	,DRAW_DATE_LIMIT_AVAIL
	,CASE WHEN EXPIRY_DATE < REPORTING_DATE THEN DATE(REPORTING_DATE + INTERVAL 2 MONTH) ELSE EXPIRY_DATE END AS EXPIRY_DATE
	,START_DATE
	,END_DATE
	,MonthOfImport
FROM (
SELECT DISTINCT 
P.Name AS FACILITY_ID
,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS CREDIT_LINE_DESC
,P.Application_Approved_Date__c AS CREDIT_LINE_ORIG_DATE
,CASE WHEN P.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)')  THEN 'TERM_LOAN' ELSE 'REVOLVING_CREDIT' END AS CREDIT_LINE_TYPE
,CASE WHEN P.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)')  THEN 'OTH' ELSE 'TF' END AS CREDIT_LINE_PURPOSE_CODE
,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS CREDIT_LINE_PURPOSE_DESC
,COALESCE(P.Temp_Maturity_Date__c,P.genesis__Maturity_Date__c) AS DRAW_TILL_DATE
,CASE WHEN P.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE P.genesis__CL_Product_Name__c  END AS  ORIG_CREDIT_LINE_CODE
,P.Application_Approved_Date__c AS DRAW_DATE_LIMIT_AVAIL
,COALESCE(P.Temp_Maturity_Date__c,P.genesis__Maturity_Date__c) AS EXPIRY_DATE
,P.Application_Approved_Date__c AS START_DATE
,COALESCE(P.Temp_Maturity_Date__c,P.genesis__Maturity_Date__c) AS END_DATE
,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
,P.Product_Code__c AS BANK_SUB_PRODUCT_CODE 
FROM NS_Account A
JOIN NS_genesis__Applications__c P ON A.Id=P.genesis__Account__c AND P.Loan_ID__c IS NULL AND P.genesis__Loan_Amount__c IS NOT NULL 
LEFT JOIN NS_genesis__Applications__c GA ON GA.genesis__Parent_Application__c= P.id
WHERE 1=1 -- GPA.genesis__status__c='Approved' 
AND EXISTS (SELECT ParentID FROM GenesisParentNoChild PNC WHERE PNC.ParentID=P.Id) 
AND EXISTS (SELECT 1 FROM cv_MasterChildApp MCA WHERE MCA.CUSTOMER_ID=A.User_ID__c)
) CTE  
GROUP BY FACILITY_ID, REPORTING_DATE, CREDIT_LINE_DESC, CREDIT_LINE_ORIG_DATE, CREDIT_LINE_TYPE, CREDIT_LINE_PURPOSE_CODE, CREDIT_LINE_PURPOSE_DESC, DRAW_TILL_DATE, ORIG_CREDIT_LINE_CODE, DRAW_DATE_LIMIT_AVAIL, EXPIRY_DATE, START_DATE, END_DATE, MonthOfImport;


DROP VIEW IF EXISTS cv_CREDIT_FACILITY_MASTER;
CREATE VIEW cv_CREDIT_FACILITY_MASTER AS
SELECT DISTINCT 
	CFD.FACILITY_ID
	,CFM.FACILITY_COMMITTED_FLAG
	,CFM.REPORTING_DATE
	,CFM.CREDIT_LINE_DESC
	,CFM.CREDIT_LINE_ORIG_DATE
	,CFM.CREDIT_LINE_TYPE
	,CFM.CREDIT_LINE_PURPOSE_CODE
	,CFM.CREDIT_LINE_PURPOSE_DESC
	,CFM.DRAW_TILL_DATE
	,CFM.ORIG_CREDIT_LINE_CODE
	,CFM.DRAW_DATE_LIMIT_AVAIL
	,CFM.EXPIRY_DATE
	,CFM.START_DATE
	,CFM.END_DATE
	,CFM.MonthOfImport
FROM  (
SELECT DISTINCT 
	FACILITY_ID
	,FACILITY_COMMITTED_FLAG
	,REPORTING_DATE
	,CREDIT_LINE_DESC
	,CREDIT_LINE_ORIG_DATE
	,CREDIT_LINE_TYPE
	,CREDIT_LINE_PURPOSE_CODE
	,CREDIT_LINE_PURPOSE_DESC
	,DRAW_TILL_DATE
	,ORIG_CREDIT_LINE_CODE
	,DRAW_DATE_LIMIT_AVAIL
	,EXPIRY_DATE
	,START_DATE
	,END_DATE
	,MonthOfImport
FROM cv_MasterChildApp 
UNION
SELECT DISTINCT 
	FACILITY_ID
	,FACILITY_COMMITTED_FLAG
	,REPORTING_DATE
	,CREDIT_LINE_DESC
	,CREDIT_LINE_ORIG_DATE
	,CREDIT_LINE_TYPE
	,CREDIT_LINE_PURPOSE_CODE
	,CREDIT_LINE_PURPOSE_DESC
	,DRAW_TILL_DATE
	,ORIG_CREDIT_LINE_CODE
	,DRAW_DATE_LIMIT_AVAIL
	,EXPIRY_DATE
	,START_DATE
	,END_DATE
	,MonthOfImport
FROM cv_MasterParentApp
) CFM
JOIN CREDIT_FACILITY_DETAILS CFD ON CFM.FACILITY_ID =CFD.FACILITY_ID ;



DROP VIEW IF EXISTS v_CREDIT_FACILITY_MASTER;
CREATE VIEW v_CREDIT_FACILITY_MASTER AS 
(
SELECT DISTINCT SYNDICATED_IND,DATA_SOURCE,Entity_Code,FACILITY_ID,DATE_FORMAT(REPORTING_DATE,'%Y-%m-%d') AS REPORTING_DATE,CREDIT_LINE_DESC,
DATE_FORMAT(CREDIT_LINE_ORIG_DATE,'%Y-%m-%d') AS CREDIT_LINE_ORIG_DATE,CREDIT_LINE_TYPE,CREDIT_LINE_PURPOSE_CODE,CREDIT_LINE_PURPOSE_DESC,
PARENT_CREDIT_LINE_CODE,DATE_FORMAT(DRAW_TILL_DATE,'%Y-%m-%d') AS DRAW_TILL_DATE,ORIG_CREDIT_LINE_CODE,FACILITY_CLASSIFICATION_IND,
REVOCABLE_STATUS_CODE,DATE_FORMAT(DRAW_DATE_LIMIT_AVAIL,'%Y-%m-%d') AS DRAW_DATE_LIMIT_AVAIL,FACILITY_COMMITTED_FLAG,DATE_FORMAT(EXPIRY_DATE,'%Y-%m-%d') AS EXPIRY_DATE,
DATE_FORMAT(END_DATE,'%Y-%m-%d') AS END_DATE,DATE_FORMAT(START_DATE,'%Y-%m-%d') AS START_DATE,MonthOfImport
FROM CREDIT_FACILITY_MASTER 
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AND  CREDIT_LINE_PURPOSE_CODE <> 'Term LN - Variable Rate - Monthly'
) ;


DROP PROCEDURE IF EXISTS proc_CreditFacilityMaster;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_CreditFacilityMaster`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_CreditFacilityMaster
** Name: CREDIT_FACILITY_MASTER
** Desc: DM - CreditFacilityMaster
** Auth: Vivek Srinivasamorthy
** Date: 30-01-2025
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
        'proc_CreditFacilityMaster'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'CREDIT_FACILITY_MASTER';

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
        'proc_CreditFacilityMaster'
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
        'proc_CreditFacilityMaster'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (FACILITY_ID, FACILITY_COMMITTED_FLAG, REPORTING_DATE, CREDIT_LINE_DESC, CREDIT_LINE_ORIG_DATE, CREDIT_LINE_TYPE, CREDIT_LINE_PURPOSE_CODE, CREDIT_LINE_PURPOSE_DESC, 
                        DRAW_TILL_DATE, ORIG_CREDIT_LINE_CODE, DRAW_DATE_LIMIT_AVAIL, EXPIRY_DATE, START_DATE, END_DATE, MonthOfImport)
            SELECT DISTINCT FACILITY_ID, FACILITY_COMMITTED_FLAG, REPORTING_DATE, CREDIT_LINE_DESC, CREDIT_LINE_ORIG_DATE, CREDIT_LINE_TYPE, CREDIT_LINE_PURPOSE_CODE, CREDIT_LINE_PURPOSE_DESC, 
                        DRAW_TILL_DATE, ORIG_CREDIT_LINE_CODE, DRAW_DATE_LIMIT_AVAIL, EXPIRY_DATE, START_DATE, END_DATE, MonthOfImport
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
        'proc_CreditFacilityMaster'
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
        'proc_CreditFacilityMaster'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;