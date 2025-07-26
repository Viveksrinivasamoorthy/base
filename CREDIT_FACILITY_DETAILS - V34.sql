
DROP TABLE IF EXISTS CREDIT_FACILITY_DETAILS;
CREATE TABLE CREDIT_FACILITY_DETAILS(
BRANCH_ID VARCHAR(20) DEFAULT ''
,ENTITY_CODE VARCHAR(20) DEFAULT 'SG01'
,UNCONDITIONAL_CANCEL_IND CHAR(1) DEFAULT 'Y'
,DATA_SOURCE VARCHAR(20) DEFAULT 'Salesforce_CL_Loan'
,MATERIALITY_THRESHOLD DECIMAL(18,2) DEFAULT 5000000.00
,NOMINAL_THRESHOLD DECIMAL(18,2) DEFAULT 0.00
,A_SCORE_SEGMENT VARCHAR(50) DEFAULT ''
,A_SCORE DECIMAL(18,2) DEFAULT NULL
,BUREAU_GRADE VARCHAR(20) DEFAULT ''
,DPD_COUNT_EXCEEDED BOOLEAN DEFAULT NULL
,HAS_ACCELERATED_LOANS CHAR(1) DEFAULT 'N'
,INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED BOOLEAN DEFAULT NULL
,SUSPENDED_BTI_FLAG CHAR(1) DEFAULT 'N'
,CBS_CUO DECIMAL(18,2) DEFAULT NULL
,FACILITY_ID  VARCHAR(20) DEFAULT ''
,REPORTING_DATE DATE DEFAULT NULL
,LIMIT_COMMITTED_AMT DECIMAL(18,2) DEFAULT NULL
,LIMIT_UTILIZED_AMT DECIMAL(18,2) DEFAULT NULL
,LIMIT_UNUTILIZED_AMT DECIMAL(18,2) DEFAULT NULL
,NEW_CREDIT_FAC_AMT DECIMAL(18,2) DEFAULT NULL
,CUSTOMER_ID VARCHAR(100) DEFAULT ''
,PROMINENT_MITIGANT_CODE VARCHAR(20) DEFAULT ''
,INTEREST_RATE DECIMAL(18,2) DEFAULT NULL
,TXN_CCY_CODE VARCHAR(3) DEFAULT ''
,CREDIT_LINE_ORIG_DATE DATE DEFAULT NULL
,RESTRUCTURED_DATE DATE DEFAULT NULL
,EXTENDED_CREDIT_FAC_DATE DATE DEFAULT NULL
,CREDIT_REVIEW_DATE DATE DEFAULT NULL
,LAST_CREDIT_REVIEW_DATE DATE DEFAULT NULL
,OVERDUE_REVIEW_DATE DATE DEFAULT NULL
,REVOLVE_IND CHAR(1) DEFAULT ''
,UNDRAWN_AMT DECIMAL(18,2) DEFAULT NULL
,COLLATERAL_VALUE DECIMAL(18,2) DEFAULT 0.00
,COLLATERAL_TYPE DECIMAL(18,2) DEFAULT NULL
,COLLATERAL_HAIRCUT DECIMAL(18,2) DEFAULT NULL
,COLLATERAL_FORCESALE_VALUE DECIMAL(18,2) DEFAULT NULL
,DISBURSMENT_TYPE VARCHAR(20) DEFAULT 'LUMPSUM'
,BANK_PRODUCT_CODE VARCHAR(50) DEFAULT 'SMEL'
,CUSTOMER_MONTH_ON_BOOK BIGINT DEFAULT NULL
,PREFERRED_CUSTOMER_FLAG BOOLEAN DEFAULT NULL
,DEFAULTED_IND CHAR(1) DEFAULT ''
,SUSPENSION_IND CHAR(1) DEFAULT 'N'
,COUNTRY_CODE VARCHAR(50) DEFAULT 'SG'
,ACCT_BLOCK_CODE CHAR(3) DEFAULT ''
,ACCT_STATUS_CODE VARCHAR(100) DEFAULT 'APPROVED'
,TRANSACTOR_IND CHAR(1) DEFAULT 'N'
,PARENT_GROUP_ID VARCHAR(20) DEFAULT ''
,PARENT_GROUP_NAME VARCHAR(100) DEFAULT ''
,GUARANTOR_TYPE VARCHAR(20) DEFAULT ''
,BANK_SUB_PRODUCT_CODE VARCHAR(100) DEFAULT ''
,MonthOfImport VARCHAR(10) NOT NULL
);

CREATE INDEX IDX_PARENT_GROUP_ID ON CREDIT_FACILITY_DETAILS (PARENT_GROUP_ID);
CREATE INDEX IDX_CUSTOMER_ID ON CREDIT_FACILITY_DETAILS (CUSTOMER_ID);
CREATE INDEX IDX_FACILITY_ID ON CREDIT_FACILITY_DETAILS(FACILITY_ID);
CREATE INDEX IDX_BANK_SUB_PRODUCT_CODE ON CREDIT_FACILITY_DETAILS(BANK_SUB_PRODUCT_CODE);
CREATE INDEX IDX_MonthOfImport ON CREDIT_FACILITY_DETAILS(MonthOfImport);  


DROP PROCEDURE IF EXISTS proc_CreditFacilityDetails;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_CreditFacilityDetails`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_CreditFacilityDetails
** Name: CREDIT_FACILITY_DETAILS
** Desc: DM - CreditFacilityDetails
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
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'CREDIT_FACILITY_DETAILS';

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
        'proc_CreditFacilityDetails'
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
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS GenesisParentNoChild;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
   	SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS CustomerLookup;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS WithoutChildApp;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS BaseFacilityDetails;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS RevolvingFacilityDetails_1;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS RevolvingFacilityDetails_2;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS RevolvingFacilityDetails;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS NonRevolvingFacilityDetails;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS BothRevNonRev;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS TotalLimit;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;   
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS NIPLimit;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS AllProduct;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS UnUtlizedApp;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS Step1;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS Step2_cte;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS Step2;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2;    
    START TRANSACTION;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("DROP TABLE IF EXISTS cv_CREDIT_FACILITY_DETAILS;");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    GET DIAGNOSTICS l_rowcount = ROW_COUNT;
    DEALLOCATE PREPARE STMT2; 
   
    START TRANSACTION;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE GenesisParentNoChild AS
						SELECT P.ID ParentID,P.Name As ParentName
						FROM NS_genesis__Applications__c P 
						LEFT JOIN NS_genesis__Applications__c CH ON P.ID=CH.genesis__Parent_Application__c 
						WHERE P.Loan_ID__c IS NULL AND P.genesis__Loan_Amount__c IS NOT NULL 
						AND P.genesis__status__c='Approved' AND P.Sub_Stage__c='APPROVED' AND CH.ID IS NULL;");   
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
        CONCAT("Table: GenesisParentNoChild - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF; 
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE GenesisParentNoChild ADD INDEX IDX_PARENTID(ParentID);");   
    SET @LSQL = LSQL; 
    PREPARE STMT2 FROM @LSQL;
    EXECUTE STMT2;
    DEALLOCATE PREPARE STMT2;
   
   	IF l_code=''
    THEN
        CALL error_handler(
        'Info',
        'Success',
        '',
        CONCAT("Table: GenesisParentNoChild - Index Created Successfully: IDX_PARENTID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;    
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE CustomerLookup AS
						WITH CTE AS (
						SELECT DISTINCT A.User_ID__c ,L.SSIC_Code ,L.GL_Customer_Type_Code,ROW_NUMBER() OVER (PARTITION BY A.User_ID__c ORDER BY P.CreatedDate DESC) RNK
						FROM NS_Account A
						JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c AND LLA.GXS_Book__c=TRUE
						LEFT JOIN NS_genesis__Applications__c P ON P.genesis__Account__c=A.Id AND P.Loan_ID__c IS NULL AND P.genesis__Loan_Amount__c IS NOT NULL AND P.genesis__Status__c='Approved' AND P.SSIC_Code__c IS NOT NULL
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN Lookup L ON L.SSIC_Code=P.SSIC_Code__c
						LEFT JOIN (SELECT DISTINCT  SSIC__c,Description__c FROM NS_Principal_Activity_Activities__c ) PAA ON PAA.SSIC__c=P.SSIC_Code__c
						LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')
						WHERE LEFT(A.User_ID__c,1)='2'
						AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						OR (LLA.loan__Loan_Status__c like 'Active%')  
						OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)  
						 ) 
						AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
						) SELECT * FROM CTE WHERE RNK=1 and SSIC_Code IS NOT NULL;");   
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
        CONCAT("Table: CustomerLookup - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;    	
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE CustomerLookup ADD INDEX IDX_USERID(User_ID__c);");   
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
        CONCAT("Table: CustomerLookup - Index Created Successfully: IDX_USERID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE WithoutChildApp AS
						SELECT DISTINCT 
						GPA.Name AS FACILITY_ID
						,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
						,CASE   WHEN COALESCE(GPA.Temp_Maturity_Date__c,GPA.genesis__Maturity_Date__c) < DATE(NOW() - INTERVAL 1 DAY) THEN 0 ELSE GPA.genesis__Loan_Amount__c END AS LIMIT_COMMITTED_AMT
						,0 AS LIMIT_UTILIZED_AMT 
						,GPA.genesis__Interest_Rate__c AS INTEREST_RATE
						,CASE WHEN GPA.Application_Approved_Date__c 
						  BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)   THEN GPA.genesis__Loan_Amount__c
						  ELSE 0 END AS NEW_CREDIT_FAC_AMT
						,A.User_ID__c AS CUSTOMER_ID
						,A.Currency__c AS TXN_CCY_CODE
						,GPA.Application_Approved_Date__c AS CREDIT_LINE_ORIG_DATE
						,GPA.Application_Approved_Date__c AS LAST_CREDIT_REVIEW_DATE
						,MONTH(A.CreatedDate) AS CUSTOMER_MONTH_ON_BOOK
						,CASE WHEN A.Credit_Grade__c IN ('SS','DFL','L') THEN 'Y' ELSE 'N' END AS DEFAULTED_IND
						,COALESCE(GPA.Temp_Maturity_Date__c, GPA.genesis__Maturity_Date__c) AS CREDIT_REVIEW_DATE
						,CASE WHEN A.UEN_Blacklist_Reason__c IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up') THEN 'A'
						      WHEN A.UEN_Blacklist_Reason__c IN ('AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation') THEN 'B'
						      ELSE '' END AS ACCT_BLOCK_CODE
						,CASE WHEN A.UEN_Blacklist_Reason__c IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up','AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation') THEN TRUE ELSE FALSE END AS INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED
						,COALESCE(PA.User_ID__c,A.User_ID__c) AS PARENT_GROUP_ID    
						,COALESCE(PA.Name,A.Name) AS PARENT_GROUP_NAME
						,CASE WHEN CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') THEN 'Personal'
						      WHEN CP.clcommon__type__c='a7W2y000000KyjLEAS' THEN 'Corporate'
						      ELSE '' END AS GUARANTOR_TYPE
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE GPA.genesis__CL_Product_Name__c  END AS BANK_SUB_PRODUCT_CODE
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)') THEN 'N' ELSE 'Y' END AS REVOLVE_IND
						,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
						FROM NS_Account A
						JOIN NS_genesis__Applications__c GPA ON A.Id=GPA.genesis__Account__c AND GPA.Loan_ID__c IS NULL AND GPA.genesis__Loan_Amount__c IS NOT NULL 
						JOIN CustomerLookup CL ON CL.User_ID__c=A.User_ID__c
						LEFT JOIN NS_genesis__Applications__c GA ON GA.genesis__Parent_Application__c= GPA.id
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN NS_clcommon__Party__c CP ON CP.genesis__Application__c=GPA.Id 
						  AND CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjLEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') AND CP.Active__c=1
						WHERE EXISTS (SELECT ParentID FROM GenesisParentNoChild PNC WHERE PNC.ParentID=GPA.Id) 
						;");   
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
        CONCAT("Table: WithoutChildApp - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;    	   
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE BaseFacilityDetails AS
						SELECT DISTINCT FACILITY_ID
						,REPORTING_DATE
						,NEW_CREDIT_FAC_AMT
						,CUSTOMER_ID
						,INTEREST_RATE
						,TXN_CCY_CODE
						,CREDIT_LINE_ORIG_DATE
						,LAST_CREDIT_REVIEW_DATE
						,CUSTOMER_MONTH_ON_BOOK
						,DEFAULTED_IND
						,CASE WHEN CREDIT_REVIEW_DATE < REPORTING_DATE THEN DATE(REPORTING_DATE + INTERVAL 2 MONTH) ELSE CREDIT_REVIEW_DATE END AS CREDIT_REVIEW_DATE
						,ACCT_BLOCK_CODE
						,INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED
						,PARENT_GROUP_ID
						,PARENT_GROUP_NAME
						,GUARANTOR_TYPE
						,BANK_SUB_PRODUCT_CODE
						,REVOLVE_IND
						,MonthOfImport
						FROM (
						SELECT DISTINCT 
						GPA.Name AS FACILITY_ID
						,DATE(NOW() - INTERVAL 1 DAY) AS REPORTING_DATE
						,CASE WHEN GPA.Application_Approved_Date__c 
						  BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)   THEN GPA.genesis__Loan_Amount__c
						  ELSE 0 END AS NEW_CREDIT_FAC_AMT
						,A.User_ID__c AS CUSTOMER_ID
						,LLA.loan__Interest_Rate__c  AS INTEREST_RATE
						,A.Currency__c AS TXN_CCY_CODE
						,GPA.Application_Approved_Date__c AS CREDIT_LINE_ORIG_DATE
						,GPA.Application_Approved_Date__c AS LAST_CREDIT_REVIEW_DATE
						,MONTH(A.CreatedDate) AS CUSTOMER_MONTH_ON_BOOK
						,CASE WHEN A.Credit_Grade__c IN ('SS','DFL','L') THEN 'Y' ELSE 'N' END AS DEFAULTED_IND
						,COALESCE(GPA.Temp_Maturity_Date__c, GPA.genesis__Maturity_Date__c) AS CREDIT_REVIEW_DATE
						,CASE WHEN A.UEN_Blacklist_Reason__c IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up') THEN 'A'
						      WHEN A.UEN_Blacklist_Reason__c IN ('AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation') THEN 'B'
						      ELSE '' END AS ACCT_BLOCK_CODE
						,CASE WHEN A.UEN_Blacklist_Reason__c IN ('Cease Operations','Financial Crime','Bankrupted','Wound-up','AML - Internal','Winding-up Suit','Bankruptcy Petition','Credit Litigation') THEN TRUE ELSE FALSE END AS INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED
						,COALESCE(PA.User_ID__c,A.User_ID__c) AS PARENT_GROUP_ID    
						,COALESCE(PA.Name,A.Name) AS PARENT_GROUP_NAME
						,CASE WHEN CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') THEN 'Personal'
						      WHEN CP.clcommon__type__c='a7W2y000000KyjLEAS' THEN 'Corporate'
						      ELSE '' END AS GUARANTOR_TYPE
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE GPA.genesis__CL_Product_Name__c  END AS BANK_SUB_PRODUCT_CODE  
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)') THEN 'N' ELSE 'Y' END AS REVOLVE_IND
						,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
						FROM NS_Account A
						JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
						JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
						LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN NS_clcommon__Party__c CP ON CP.genesis__Application__c=GPA.Id 
						  AND CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjLEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') AND CP.Active__c=1
						LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')  
						WHERE 1=1 -- GPA.genesis__status__c='Approved' 
						AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						OR (LLA.loan__Loan_Status__c like 'Active%') 
						OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						) 
						AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
						UNION
						SELECT DISTINCT FACILITY_ID,REPORTING_DATE,NEW_CREDIT_FAC_AMT,CUSTOMER_ID,INTEREST_RATE,TXN_CCY_CODE,CREDIT_LINE_ORIG_DATE,LAST_CREDIT_REVIEW_DATE,CUSTOMER_MONTH_ON_BOOK,DEFAULTED_IND,CREDIT_REVIEW_DATE,ACCT_BLOCK_CODE,INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED,PARENT_GROUP_ID,PARENT_GROUP_NAME,GUARANTOR_TYPE,BANK_SUB_PRODUCT_CODE,REVOLVE_IND,MonthOfImport
						FROM WithoutChildApp
						) CTE;");   
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
        CONCAT("Table: BaseFacilityDetails - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BaseFacilityDetails ADD INDEX IDX_FACILITY_ID(FACILITY_ID);");   
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
        CONCAT("Table: BaseFacilityDetails - Index Created Successfully: IDX_FACILITY_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BaseFacilityDetails ADD INDEX IDX_CUSTOMER_ID(CUSTOMER_ID);");   
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
        CONCAT("Table: BaseFacilityDetails - Index Created Successfully: IDX_CUSTOMER_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;  
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BaseFacilityDetails ADD INDEX IDX_BANK_SUB_PRODUCT_CODE(BANK_SUB_PRODUCT_CODE(100));");   
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
        CONCAT("Table: BaseFacilityDetails - Index Created Successfully: IDX_BANK_SUB_PRODUCT_CODE "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE RevolvingFacilityDetails_1 AS
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c,MAX(INTEREST_RATE) AS INTEREST_RATE,SUM(LIMIT_COMMITTED_AMT) AS LIMIT_COMMITTED_AMT,SUM(LIMIT_UTILIZED_AMT) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT 
						COALESCE(PA.User_ID__c,A.User_ID__c) AS PARENT_GROUP_ID
						,A.User_ID__c  AS CUSTOMER_ID
						,GPA.Name AS FACILITY_ID
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE GPA.genesis__CL_Product_Name__c  END AS Product_Code__c
						,LLA.loan__Interest_Rate__c  AS INTEREST_RATE
						,LLA.Loan_App_ID__c
						,CASE WHEN COALESCE(GPA.Temp_Maturity_Date__c,GPA.genesis__Maturity_Date__c) < DATE(NOW() - INTERVAL 1 DAY)  THEN LLA.loan__Principal_Remaining__c
						    ELSE NULL END AS LIMIT_COMMITTED_AMT    
						,LLA.loan__Principal_Remaining__c AS LIMIT_UTILIZED_AMT 
						FROM NS_Account A
						JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
						JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
						LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN NS_clcommon__Party__c CP ON CP.genesis__Application__c=GPA.Id 
						  AND CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjLEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') AND CP.Active__c=1
						LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')  
						WHERE 1=1 -- GPA.genesis__status__c='Approved' 
						AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						OR (LLA.loan__Loan_Status__c like 'Active%') 
						OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						) 
						AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
						AND GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc','Purchase Order Financing','Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)')
						AND COALESCE(GPA.Temp_Maturity_Date__c,GPA.genesis__Maturity_Date__c) < DATE(NOW() - INTERVAL 1 DAY) 
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c;");   
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
        CONCAT("Table: RevolvingFacilityDetails_1 - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;        
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE RevolvingFacilityDetails_2 AS
						SELECT DISTINCT  PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c,COALESCE(MAX(INTEREST_RATE),0) AS INTEREST_RATE,COALESCE(MAX(LIMIT_COMMITTED_AMT),0) AS LIMIT_COMMITTED_AMT,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT 
						COALESCE(PA.User_ID__c,A.User_ID__c) AS PARENT_GROUP_ID
						,A.User_ID__c  AS CUSTOMER_ID
						,GPA.Name AS FACILITY_ID
						,CASE WHEN GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc') THEN 'Corporate Vendor Financing' ELSE GPA.genesis__CL_Product_Name__c  END AS Product_Code__c
						,LLA.loan__Interest_Rate__c  AS INTEREST_RATE
						,LLA.Loan_App_ID__c
						,CASE WHEN COALESCE(GPA.Temp_Maturity_Date__c,GPA.genesis__Maturity_Date__c) >= DATE(NOW() - INTERVAL 1 DAY)  THEN GPA.genesis__Loan_Amount__c 
						    ELSE NULL END AS LIMIT_COMMITTED_AMT    
						,LLA.loan__Principal_Remaining__c AS LIMIT_UTILIZED_AMT 
						FROM NS_Account A
						JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
						JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
						LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN NS_clcommon__Party__c CP ON CP.genesis__Application__c=GPA.Id 
						  AND CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjLEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') AND CP.Active__c=1
						LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')  
						WHERE 1=1 -- GPA.genesis__status__c='Approved' 
						AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						OR (LLA.loan__Loan_Status__c like 'Active%') 
						OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						) 
						AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
						AND GPA.genesis__CL_Product_Name__c IN ('CVF Low Doc','CVF High Doc','CVF Medium Doc','Purchase Order Financing','Accounts Receivable Financing (Disclosed)','Accounts Receivable Financing (Non-Disclosed)')
						AND COALESCE(GPA.Temp_Maturity_Date__c,GPA.genesis__Maturity_Date__c) >= DATE(NOW() - INTERVAL 1 DAY) 
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c;");   
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
        CONCAT("Table: RevolvingFacilityDetails_2 - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE RevolvingFacilityDetails AS
						SELECT DISTINCT * FROM (
						SELECT * FROM RevolvingFacilityDetails_1
						UNION
						SELECT * FROM RevolvingFacilityDetails_2 ) CTE;				
						");   
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
        CONCAT("Table: RevolvingFacilityDetails - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;        
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE NonRevolvingFacilityDetails AS
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c,COALESCE(MAX(INTEREST_RATE),0) AS INTEREST_RATE,COALESCE(SUM(LIMIT_COMMITTED_AMT),0) AS LIMIT_COMMITTED_AMT,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT 
						COALESCE(PA.User_ID__c,A.User_ID__c) AS PARENT_GROUP_ID
						,A.User_ID__c  AS CUSTOMER_ID
						,GPA.Name AS FACILITY_ID
						,LLA.Loan_App_ID__c
						,GPA.genesis__CL_Product_Name__c AS Product_Code__c
						,(LLA.loan__Interest_Rate__c)  AS INTEREST_RATE
						,(LLA.loan__Principal_Remaining__c)  AS LIMIT_COMMITTED_AMT    
						,(LLA.loan__Principal_Remaining__c) AS LIMIT_UTILIZED_AMT 
						FROM NS_Account A
						JOIN NS_loan__Loan_Account__c LLA ON A.Id=LLA.loan__Account__c  AND LLA.GXS_Book__c=TRUE
						JOIN NS_genesis__Applications__c GA ON LLA.genesis_app_Id__c=GA.Id
						LEFT JOIN NS_genesis__Applications__c GPA ON GPA.id= GA.genesis__Parent_Application__c
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN NS_clcommon__Party__c CP ON CP.genesis__Application__c=GPA.Id 
						  AND CP.clcommon__type__c IN ('a7W2y000000KyjIEAS','a7W2y000000KyjLEAS','a7W2y000000KyjMEAS', 'a7W2y000000KyjJEAS') AND CP.Active__c=1
						LEFT JOIN NS_loan__Loan_Payment_Transaction__c LPT ON LPT.loan__Loan_Account__c = LLA.ID AND LPT.CL_Contract_Status__c IN ('Closed- Written Off')  
						WHERE 1=1 -- GPA.genesis__status__c='Approved' 
						AND ( LLA.Loan_Terminated_Date__c BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						OR (LLA.loan__Loan_Status__c like 'Active%') 
						OR LPT.CreatedDate BETWEEN  DATE(LAST_DAY(CURDATE() - INTERVAL 2 MONTH) + INTERVAL 1 DAY) AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH) 
						) 
						AND LLA.Genesis_Product__c NOT IN ('Term LN - Variable Rate - Monthly')
						AND GPA.genesis__CL_Product_Name__c IN ('Working Capital Financing','NIP WC Bullet','Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)')
						) CTE GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,Product_Code__c;		
						");   
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
        CONCAT("Table: NonRevolvingFacilityDetails - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE BothRevNonRev AS
						select* from RevolvingFacilityDetails
						UNION
						select * from NonRevolvingFacilityDetails 
						UNION
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,FACILITY_ID,BANK_SUB_PRODUCT_CODE,INTEREST_RATE,LIMIT_COMMITTED_AMT,LIMIT_UTILIZED_AMT  FROM WithoutChildApp;				
						");   
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
        CONCAT("Table: BothRevNonRev - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BothRevNonRev ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: BothRevNonRev - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BothRevNonRev ADD INDEX IDX_FACILITY_ID(FACILITY_ID);");   
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
        CONCAT("Table: BothRevNonRev - Index Created Successfully: IDX_FACILITY_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE BothRevNonRev ADD INDEX IDX_Product_Code__c(Product_Code__c(100));");   
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
        CONCAT("Table: BothRevNonRev - Index Created Successfully: IDX_Product_Code__c "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE TotalLimit AS
						SELECT  DISTINCT REV.PARENT_GROUP_ID,COALESCE(PA.Total_Limit__c,A.Total_Limit__c) AS TotalLimit
						FROM NS_Account A 
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN BothRevNonRev REV ON REV.PARENT_GROUP_ID  =COALESCE(PA.User_ID__c,A.User_ID__c ) 
						WHERE REV.PARENT_GROUP_ID IS NOT NULL  
						GROUP BY REV.PARENT_GROUP_ID;			
						");   
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
        CONCAT("Table: TotalLimit - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE TotalLimit ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: TotalLimit - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE NIPLimit AS
						SELECT PARENT_GROUP_ID,TotalNIPLimit,COALESCE(Total_Limit__c,0)-COALESCE(TotalNIPLimit,0) AS LimitApportioned
						FROM (
						SELECT  PARENT_GROUP_ID,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS TotalNIPLimit,COALESCE(Total_Limit__c,0)  AS Total_Limit__c -- - COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LimitApportioned
						FROM (
						SELECT DISTINCT REV.PARENT_GROUP_ID, LIMIT_UTILIZED_AMT,COALESCE(PA.Total_Limit__c,A.Total_Limit__c) AS Total_Limit__c
						FROM NS_Account A 
						LEFT JOIN NS_Account PA ON PA.ID=A.PARENTID 
						LEFT JOIN BothRevNonRev REV ON REV.PARENT_GROUP_ID  =COALESCE(PA.User_ID__c,A.User_ID__c )   AND REV.Product_Code__c IN ('Working Capital Financing','NIP WC Bullet')
						) CTE WHERE PARENT_GROUP_ID IS NOT NULL
						GROUP BY PARENT_GROUP_ID,Total_Limit__c
						) X;		
						");   
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
        CONCAT("Table: NIPLimit - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;        
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE NIPLimit ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: NIPLimit - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;          
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE AllProduct AS
						SELECT  DISTINCT PARENT_GROUP_ID,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS ToalUtlized ,COALESCE(MAX(TotalNIPLimit),0) AS TotalNIPUtilized ,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) - COALESCE(TotalNIPLimit,0) AS UtilizedApportioned
						FROM (
						SELECT REV.PARENT_GROUP_ID,REV.LIMIT_UTILIZED_AMT,NIP.TotalNIPLimit
						FROM BothRevNonRev REV 
						LEFT JOIN NIPLimit NIP ON NIP.PARENT_GROUP_ID = REV.PARENT_GROUP_ID -- AND REV.Product_Code__c =NIP.Product_Code__c
						) CTE WHERE PARENT_GROUP_ID IS NOT NULL 
						GROUP BY PARENT_GROUP_ID;	
						");   
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
        CONCAT("Table: AllProduct - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE AllProduct ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: AllProduct - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE UnUtlizedApp AS
						SELECT DISTINCT CV.PARENT_GROUP_ID,COALESCE(NIP.LimitApportioned,0) - COALESCE(AL.UtilizedApportioned,0) AS Unutilized
						FROM BothRevNonRev CV
						LEFT JOIN NIPLimit NIP ON CV.PARENT_GROUP_ID =NIP.PARENT_GROUP_ID 
						LEFT JOIN AllProduct  AL ON CV.PARENT_GROUP_ID =AL.PARENT_GROUP_ID 
						GROUP BY CV.PARENT_GROUP_ID;
						");   
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
        CONCAT("Table: UnUtlizedApp - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;        
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE UnUtlizedApp ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: UnUtlizedApp - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE Step1 AS
						SELECT DISTINCT PARENT_GROUP_ID,TotalLimit,TotalNIPLimit,LimitApportioned,TotalUtlized,TotalNIPUtilized,UtilizedApportioned,
						          CASE WHEN COALESCE(LimitApportioned,0)-COALESCE(UtilizedApportioned,0) <0 THEN 0 ELSE COALESCE(LimitApportioned,0)-COALESCE(UtilizedApportioned,0) END AS Unutilized
						FROM (          
						SELECT DISTINCT PARENT_GROUP_ID,COALESCE(MAX(TotalLimit),0) AS TotalLimit,COALESCE(MAX(TotalNIPLimit),0) AS TotalNIPLimit,COALESCE(MAX(TotalLimit),0)-COALESCE(MAX(TotalNIPLimit),0) AS LimitApportioned,
						        COALESCE(MAX(ToalUtlized),0) AS TotalUtlized,COALESCE(MAX(TotalNIPUtilized),0) AS TotalNIPUtilized,COALESCE(MAX(ToalUtlized),0) - COALESCE(MAX(TotalNIPUtilized),0) AS UtilizedApportioned,
						        COALESCE(MAX(Unutilized),0) AS Unutilized
						FROM (
						SELECT DISTINCT R.PARENT_GROUP_ID,TL.TotalLimit,NIP.TotalNIPLimit,LimitApportioned,ALLP.ToalUtlized,ALLP.TotalNIPUtilized,ALLP.UtilizedApportioned,UN.Unutilized 
						FROM BothRevNonRev R -- ON R.FACILITY_ID =CV.FACILITY_ID 
						LEFT JOIN TotalLimit TL ON TL.PARENT_GROUP_ID =R.PARENT_GROUP_ID -- AND TL.Product_Code__c=R.Product_Code__c
						LEFT JOIN NIPLimit NIP ON R.PARENT_GROUP_ID =NIP.PARENT_GROUP_ID -- AND NIP.Product_Code__c =R.Product_Code__c  
						LEFT JOIN AllProduct ALLP ON ALLP.PARENT_GROUP_ID =R.PARENT_GROUP_ID 
						LEFT JOIN UnUtlizedApp UN ON UN.PARENT_GROUP_ID =R.PARENT_GROUP_ID 
						) CTE WHERE PARENT_GROUP_ID IS NOT NULL
						GROUP BY PARENT_GROUP_ID
						) X;
						");   
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
        CONCAT("Table: Step1 - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE Step1 ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: Step1 - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE Step2_cte AS
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT,CASE WHEN Balance <0 THEN 0 ELSE Balance END AS Balance 
						FROM 
						(
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,Product_Limit,Product_Limit AS LIMIT_UTILIZED_AMT,COALESCE(Product_Limit,0)-COALESCE(Product_Limit,0) AS Balance
						FROM (
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS Product_Limit  
						FROM (
						SELECT DISTINCT CV.PARENT_GROUP_ID,CV.CUSTOMER_ID,CV.REPORTING_DATE,CV.BANK_SUB_PRODUCT_CODE,R.LIMIT_UTILIZED_AMT AS LIMIT_UTILIZED_AMT 
						FROM BaseFacilityDetails CV 
						JOIN BothRevNonRev R ON R.FACILITY_ID =CV.FACILITY_ID 
						JOIN NS_Account A ON CV.CUSTOMER_ID  =A.User_ID__c AND CV.BANK_SUB_PRODUCT_CODE IN ('Term LN - Citi(Monthly)','Overdraft LN - Citi(Single)','Working Capital Financing','NIP WC Bullet')
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE
						) X
						UNION
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,PO_Limit__c,LIMIT_UTILIZED_AMT,COALESCE(PO_Limit__c,0)  - COALESCE(LIMIT_UTILIZED_AMT,0)
						FROM (
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,PO_Limit__c, COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT  
						FROM (
						SELECT DISTINCT CV.PARENT_GROUP_ID,CV.CUSTOMER_ID,CV.REPORTING_DATE,CV.BANK_SUB_PRODUCT_CODE,A.PO_Limit__c ,R.LIMIT_UTILIZED_AMT AS LIMIT_UTILIZED_AMT 
						FROM BaseFacilityDetails CV 
						JOIN BothRevNonRev R ON R.FACILITY_ID =CV.FACILITY_ID 
						JOIN NS_Account A ON CV.CUSTOMER_ID  =A.User_ID__c AND CV.BANK_SUB_PRODUCT_CODE ='Purchase Order Financing'
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,PO_Limit__c
						) X
						UNION
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARND_Limit__c,LIMIT_UTILIZED_AMT,COALESCE(ARND_Limit__c,0)  - COALESCE(LIMIT_UTILIZED_AMT,0)
						FROM (
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARND_Limit__c, COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT CV.PARENT_GROUP_ID,CV.CUSTOMER_ID,CV.REPORTING_DATE,CV.BANK_SUB_PRODUCT_CODE,A.ARND_Limit__c ,R.LIMIT_UTILIZED_AMT AS LIMIT_UTILIZED_AMT 
						FROM BaseFacilityDetails CV 
						JOIN BothRevNonRev R ON R.FACILITY_ID =CV.FACILITY_ID 
						JOIN NS_Account A ON CV.CUSTOMER_ID  =A.User_ID__c AND CV.BANK_SUB_PRODUCT_CODE ='Accounts Receivable Financing (Non-Disclosed)' 
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARND_Limit__c
						) X
						UNION
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARD_Limit__c,  LIMIT_UTILIZED_AMT, COALESCE(ARD_Limit__c,0)  - COALESCE(LIMIT_UTILIZED_AMT,0)
						FROM (
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARD_Limit__c, COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT CV.PARENT_GROUP_ID,CV.CUSTOMER_ID,CV.REPORTING_DATE,CV.BANK_SUB_PRODUCT_CODE,A.ARD_Limit__c ,R.LIMIT_UTILIZED_AMT AS LIMIT_UTILIZED_AMT 
						FROM BaseFacilityDetails CV 
						JOIN BothRevNonRev R ON R.FACILITY_ID =CV.FACILITY_ID 
						JOIN NS_Account A ON CV.CUSTOMER_ID  =A.User_ID__c AND CV.BANK_SUB_PRODUCT_CODE ='Accounts Receivable Financing (Disclosed)'
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,ARD_Limit__c
						) X
						UNION
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,CVF_Limit__c, LIMIT_UTILIZED_AMT,COALESCE(CVF_Limit__c,0)  - COALESCE(LIMIT_UTILIZED_AMT,0)
						FROM (
						SELECT DISTINCT PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,CVF_Limit__c, COALESCE(SUM(LIMIT_UTILIZED_AMT),0) AS LIMIT_UTILIZED_AMT
						FROM (
						SELECT DISTINCT CV.PARENT_GROUP_ID,CV.CUSTOMER_ID,CV.REPORTING_DATE,CV.BANK_SUB_PRODUCT_CODE,A.CVF_Limit__c ,R.LIMIT_UTILIZED_AMT AS LIMIT_UTILIZED_AMT 
						FROM BaseFacilityDetails CV 
						JOIN BothRevNonRev R ON R.FACILITY_ID =CV.FACILITY_ID 
						JOIN NS_Account A ON CV.CUSTOMER_ID  =A.User_ID__c AND CV.BANK_SUB_PRODUCT_CODE ='Corporate Vendor Financing'
						) CTE
						GROUP BY PARENT_GROUP_ID,CUSTOMER_ID,REPORTING_DATE,BANK_SUB_PRODUCT_CODE,CVF_Limit__c
						) X
						) CTE ;
						");   
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
        CONCAT("Table: Step2_cte - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE Step2_cte ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: Step2_cte - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE Step2 AS
						SELECT DISTINCT REPORTING_DATE,PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT ,Balance,TotalBalance,COALESCE(LIMIT_COMMITTED_AMT,0) AS LIMIT_COMMITTED_AMT,COALESCE(LIMIT_COMMITTED_AMT,0)-COALESCE(LIMIT_UTILIZED_AMT,0) AS LIMIT_UNUTILIZED_AMT
						FROM (
						SELECT DISTINCT REPORTING_DATE,PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT ,Balance,TotalBalance,COALESCE(ApportionedBalance,0)+COALESCE(LIMIT_UTILIZED_AMT,0) AS LIMIT_COMMITTED_AMT
						FROM 
						(
						SELECT DISTINCT CT.REPORTING_DATE,CT.PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT ,Balance,SG.TotalBalance,CAST(COALESCE(CT.Balance,0)/COALESCE(SG.TotalBalance,0)*COALESCE(ST1.Unutilized,0) AS DECIMAL(18,2)) AS ApportionedBalance
						FROM Step2_cte CT
						LEFT JOIN (SELECT PARENT_GROUP_ID,SUM(Balance) AS TotalBalance FROM Step2_cte GROUP BY PARENT_GROUP_ID ) SG ON SG.PARENT_GROUP_ID =CT.PARENT_GROUP_ID 
						LEFT JOIN Step1 ST1 ON ST1.PARENT_GROUP_ID =CT.PARENT_GROUP_ID 
						GROUP BY CT.REPORTING_DATE,CT.PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT,Balance,SG.TotalBalance
						ORDER BY CT.REPORTING_DATE,CT.PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE,Product_Limit,LIMIT_UTILIZED_AMT,Balance,SG.TotalBalance
						) CTE ) X;
						");   
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
        CONCAT("Table: Step2 - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;    
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE Step2 ADD INDEX IDX_PARENT_GROUP_ID(PARENT_GROUP_ID);");   
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
        CONCAT("Table: Step2 - Index Created Successfully: IDX_PARENT_GROUP_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE Step2 ADD INDEX IDX_CUSTOMER_ID(CUSTOMER_ID);");   
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
        CONCAT("Table: Step2 - Index Created Successfully: IDX_CUSTOMER_ID "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;       
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE Step2 ADD INDEX IDX_BANK_SUB_PRODUCT_CODE(BANK_SUB_PRODUCT_CODE(100));");   
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
        CONCAT("Table: Step2 - Index Created Successfully: IDX_BANK_SUB_PRODUCT_CODE "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;        
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("CREATE TABLE cv_CREDIT_FACILITY_DETAILS AS
						SELECT DISTINCT 
						  FACILITY_ID
						  ,REPORTING_DATE
						  ,NEW_CREDIT_FAC_AMT
						  ,CUSTOMER_ID
						  ,INTEREST_RATE
						  ,TXN_CCY_CODE
						  ,CREDIT_LINE_ORIG_DATE
						  ,LAST_CREDIT_REVIEW_DATE
						  ,CUSTOMER_MONTH_ON_BOOK
						  ,DEFAULTED_IND
						  ,CREDIT_REVIEW_DATE
						  ,ACCT_BLOCK_CODE
						  ,INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED
						  ,PARENT_GROUP_ID
						  ,PARENT_GROUP_NAME
						  ,GUARANTOR_TYPE
						  ,BANK_SUB_PRODUCT_CODE
						  ,REVOLVE_IND
						  ,MonthOfImport
						  ,LIMIT_UTILIZED_AMT
						  ,LIMIT_COMMITTED_AMT
						  ,LIMIT_UNUTILIZED_AMT
						  ,UNDRAWN_AMT   
						FROM (
						SELECT DISTINCT 
						  FACILITY_ID
						  ,CV.REPORTING_DATE
						  ,NEW_CREDIT_FAC_AMT
						  ,CV.CUSTOMER_ID
						  ,INTEREST_RATE
						  ,TXN_CCY_CODE
						  ,CREDIT_LINE_ORIG_DATE
						  ,LAST_CREDIT_REVIEW_DATE
						  ,CUSTOMER_MONTH_ON_BOOK
						  ,DEFAULTED_IND
						  ,CREDIT_REVIEW_DATE
						  ,ACCT_BLOCK_CODE
						  ,INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED
						  ,CV.PARENT_GROUP_ID
						  ,PARENT_GROUP_NAME
						  ,GUARANTOR_TYPE
						  ,CV.BANK_SUB_PRODUCT_CODE
						  ,REVOLVE_IND
						  ,MonthOfImport 
						  ,ST2.LIMIT_UTILIZED_AMT 
						  ,ST2.LIMIT_COMMITTED_AMT 
						  ,ST2.LIMIT_UNUTILIZED_AMT 
						  ,ST2.LIMIT_UNUTILIZED_AMT AS UNDRAWN_AMT
						  ,ROW_NUMBER() OVER (PARTITION BY PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE ORDER BY CREDIT_LINE_ORIG_DATE DESC ,GUARANTOR_TYPE DESC) AS RNK
						FROM BaseFacilityDetails  CV
						LEFT JOIN Step2 ST2 ON ST2.PARENT_GROUP_ID = CV.PARENT_GROUP_ID AND ST2.CUSTOMER_ID =CV.CUSTOMER_ID AND ST2.BANK_SUB_PRODUCT_CODE =CV.BANK_SUB_PRODUCT_CODE 
						) CTE WHERE CTE.RNK=1
						ORDER BY PARENT_GROUP_ID,CUSTOMER_ID,BANK_SUB_PRODUCT_CODE;				
						");   
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
        CONCAT("Table: cv_CREDIT_FACILITY_DETAILS - Created Successfully - table counts:",l_rowcount),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;     
   
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("ALTER TABLE cv_CREDIT_FACILITY_DETAILS ADD INDEX IDX_MonthOfImport(MonthOfImport);");   
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
        CONCAT("Table: cv_CREDIT_FACILITY_DETAILS - Index Created Successfully: IDX_MonthOfImport "),
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF;      

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (FACILITY_ID,REPORTING_DATE,LIMIT_COMMITTED_AMT,LIMIT_UTILIZED_AMT,LIMIT_UNUTILIZED_AMT,NEW_CREDIT_FAC_AMT,CUSTOMER_ID,INTEREST_RATE,TXN_CCY_CODE,
              CREDIT_LINE_ORIG_DATE,CREDIT_REVIEW_DATE,CUSTOMER_MONTH_ON_BOOK,DEFAULTED_IND,LAST_CREDIT_REVIEW_DATE,ACCT_BLOCK_CODE,UNDRAWN_AMT,
              INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED,PARENT_GROUP_ID,PARENT_GROUP_NAME,GUARANTOR_TYPE,BANK_SUB_PRODUCT_CODE,REVOLVE_IND,MonthOfImport)
            SELECT DISTINCT FACILITY_ID,REPORTING_DATE,LIMIT_COMMITTED_AMT,LIMIT_UTILIZED_AMT,LIMIT_UNUTILIZED_AMT,NEW_CREDIT_FAC_AMT,CUSTOMER_ID,INTEREST_RATE,TXN_CCY_CODE,
              CREDIT_LINE_ORIG_DATE,CREDIT_REVIEW_DATE,CUSTOMER_MONTH_ON_BOOK,DEFAULTED_IND,LAST_CREDIT_REVIEW_DATE,ACCT_BLOCK_CODE,UNDRAWN_AMT,
              INDEFINITE_BLOCK_DPD_COUNT_EXCEEDED,PARENT_GROUP_ID,PARENT_GROUP_NAME,GUARANTOR_TYPE,BANK_SUB_PRODUCT_CODE,REVOLVE_IND,MonthOfImport
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
        'proc_CreditFacilityDetails'
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
        'proc_CreditFacilityDetails'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;


 