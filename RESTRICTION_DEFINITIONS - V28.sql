
DROP TABLE IF EXISTS RESTRICTION_DEFINITIONS;
CREATE TABLE RESTRICTION_DEFINITIONS
(
Reason_Code VARCHAR(10),
Restriction VARCHAR(255),
Minimum_MAS612_Grade VARCHAR(10),
Reason_Description VARCHAR(1000)
);


DROP VIEW IF EXISTS v_RESTRICTION_DEFINITIONS;
CREATE VIEW v_RESTRICTION_DEFINITIONS AS
SELECT * FROM RESTRICTION_DEFINITIONS;


DROP PROCEDURE IF EXISTS proc_RestrictionDefinitions;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_RestrictionDefinitions`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_RestrictionDefinitions
** Name: RESTRICTION_DEFINITIONS
** Desc: DM - AccountCashFlows
** Auth: Vivek Srinivasamorthy
** Date: 10-01-2025
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
        'proc_RestrictionDefinitions'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'RESTRICTION_DEFINITIONS';

    CALL sys.table_exists(DATABASE(), LV_TABLE_NAME,@exist); 

    SET SQL_SAFE_UPDATES = 0;
  
    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("TRUNCATE TABLE ",LV_TABLE_NAME,";");   
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
        CONCAT("Table: ",LV_TABLE_NAME," - Truncated"),
        'proc_RestrictionDefinitions'
        ,CONCAT(IN_JOB_ID));
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME," VALUES 
                        ('A01','DRAWDOWN_BLOCKED','L ','Cease Operations'),
                        ('A02','DRAWDOWN_BLOCKED','L','Financial Crime'),
                        ('A03','DRAWDOWN_BLOCKED','L','Bankrupted'),
                        ('A04','DRAWDOWN_BLOCKED','L','Wound-up'),
                        ('B01','DRAWDOWN_BLOCKED','SS','AML - Internal'),
                        ('B02','DRAWDOWN_BLOCKED','SS','Winding-up Suit'),
                        ('B03','DRAWDOWN_BLOCKED','SS','Bankruptcy Petition'),
                        ('B04','DRAWDOWN_BLOCKED','SS','Credit Litigation');");   
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
        CONCAT("Table: ",LV_TABLE_NAME," - Inserted counts:",l_rowcount),
        'proc_RestrictionDefinitions'
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
        'proc_RestrictionDefinitions'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;