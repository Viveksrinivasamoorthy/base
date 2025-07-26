DROP TABLE IF EXISTS LENDING_SUMMARY;
CREATE TABLE LENDING_SUMMARY
(
ORIGINATING_ENTITY VARCHAR(50) DEFAULT 'VCPL'
,_REPORTING_DATE DATE DEFAULT NULL
,PRODUCT_TYPE VARCHAR(1500) DEFAULT NULL
,BALANCESHEET_IND VARCHAR(1) DEFAULT NULL
,EXPOSURE_CURRENCY VARCHAR(3) DEFAULT 'SGD'
,EXPOSURE_AMT DECIMAL(22,3) DEFAULT NULL
,MonthOfImport VARCHAR(10) NOT NULL
);


DROP VIEW IF EXISTS cv_LENDING_SUMMARY;
CREATE VIEW cv_LENDING_SUMMARY AS
SELECT PRODUCT_TYPE,DATE(NOW() - INTERVAL 1 DAY) AS _REPORTING_DATE,Balancesheet_ind,SUM(EXPOSURE_AMT) AS EXPOSURE_AMT,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') AS MonthOfImport
FROM ECL_INPUT 
GROUP BY PRODUCT_TYPE,Balancesheet_ind
ORDER BY Balancesheet_ind DESC,PRODUCT_TYPE;

DROP VIEW IF EXISTS v_LENDING_SUMMARY;
CREATE VIEW v_LENDING_SUMMARY AS
SELECT DISTINCT ORIGINATING_ENTITY,DATE_FORMAT(_REPORTING_DATE,'%Y-%m-%d') _REPORTING_DATE,PRODUCT_TYPE,BALANCESHEET_IND,EXPOSURE_CURRENCY,EXPOSURE_AMT,MonthOfImport
FROM LENDING_SUMMARY
WHERE MonthOfImport=DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%b-%y') ;


DROP PROCEDURE IF EXISTS proc_LendingSummary;
DELIMITER $$
CREATE DEFINER=`sg_automation`@`%` PROCEDURE `proc_LendingSummary`(IN IN_JOB_ID INT)
mainBlock:BEGIN
/*******************************************************
** File: proc_LendingSummary
** Name: LENDING_SUMMARY
** Desc: DM - LENDING_SUMMARY
** Auth: Vivek Srinivasamorthy
** Date: 08-05-2025
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
        'proc_LendingSummary'
        ,CONCAT(IN_JOB_ID));
    END;
    
    SET LV_TABLE_NAME= 'LENDING_SUMMARY';

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
        'proc_LendingSummary'
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
        'proc_LendingSummary'
        ,CONCAT(IN_JOB_ID));
        SET LV_STATUS = 'Warning';
        LEAVE mainBlock;
    END IF; 

    START TRANSACTION;

    SET LSQL='',l_code='',l_message='', l_rowcount=0;
    SET LSQL = CONCAT("INSERT INTO ",LV_TABLE_NAME,"
                        (`_REPORTING_DATE`,PRODUCT_TYPE,BALANCESHEET_IND,EXPOSURE_AMT,MonthOfImport)
                        SELECT DISTINCT `_REPORTING_DATE`,PRODUCT_TYPE,BALANCESHEET_IND,EXPOSURE_AMT,MonthOfImport
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
        'proc_LendingSummary'
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
        'proc_LendingSummary'
        ,CONCAT(IN_JOB_ID));
    END IF; 
END$$
DELIMITER ;