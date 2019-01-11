# -*- coding: utf-8 -*-
"""
This module manages DB access, SQL queries to manage training source from database.
Uses Redshift as primary db source but also incorporates data from CDS (Hive) by merging files afterwards.
"""

__author__ = "Gil"
__status__ = "production"
__version__ = "0.1"
__date__ = "April 2018"

from common import const, db
import pandas as pd
import sys
import os
import re
from sqlalchemy import create_engine, text
import logging
import datetime
from dateutil.relativedelta import relativedelta

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DB_SANDBOX_URL = db.DB_SANDBOX


def step1_int_category_mapping(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING;
        """)
        q = ("""
        SELECT CASE WHEN D7.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE
                WHEN D6.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE
                WHEN D5.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE
                WHEN D4.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYCODE
                WHEN D3.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYCODE
                WHEN D2.NAME IS NOT NULL THEN D2.DISPLAYITEMCATEGORYCODE
                ELSE D1.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE
           , CASE WHEN D1.NAME IS NOT NULL THEN D1.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D2.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D3.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D4.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D5.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NULL AND D7.NAME IS NOT NULL THEN D7.NAME 
           END AS CATE1 
           , CASE WHEN D1.NAME IS NOT NULL THEN D1.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D2.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NULL AND D7.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE1 
           , CASE WHEN D1.NAME IS NOT NULL THEN D1.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D2.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NULL AND D7.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID 
           END AS CATEID1 
           , CASE WHEN D1.NAME IS NOT NULL THEN D2.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D3.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D4.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D5.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D7.NAME 
           END AS CATE2
           , CASE WHEN D1.NAME IS NOT NULL THEN D2.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE2
           , CASE WHEN D1.NAME IS NOT NULL THEN D2.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NULL AND D6.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID 
           END AS CATEID2
           , CASE WHEN D1.NAME IS NOT NULL THEN D3.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D4.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D5.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D7.NAME 
           END AS CATE3
           , CASE WHEN D1.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE3
           , CASE WHEN D1.NAME IS NOT NULL THEN D3.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NULL AND D5.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID 
           END AS CATEID3
           , CASE WHEN D1.NAME IS NOT NULL THEN D4.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D5.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D7.NAME 
           END AS CATE4
           , CASE WHEN D1.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE4
           , CASE WHEN D1.NAME IS NOT NULL THEN D4.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NULL AND D4.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID 
           END AS CATEID4
           , CASE WHEN D1.NAME IS NOT NULL THEN D5.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D7.NAME 
           END AS CATE5
           , CASE WHEN D1.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE 
           END AS CATECODE5
           , CASE WHEN D1.NAME IS NOT NULL THEN D5.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NULL AND D3.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID 
           END AS CATEID5
           , CASE WHEN D1.NAME IS NOT NULL THEN D6.NAME 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D7.NAME
           END AS CATE6
           , CASE WHEN D1.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYCODE 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYCODE
           END AS CATECODE6
           , CASE WHEN D1.NAME IS NOT NULL THEN D6.DISPLAYITEMCATEGORYID 
                WHEN D1.NAME IS NULL AND D2.NAME IS NOT NULL THEN D7.DISPLAYITEMCATEGORYID
           END AS CATEID6
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING
        FROM ODS.DISPLAY_ITEM_CATEGORIES D7
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D6 ON D6.DISPLAYITEMCATEGORYID=D7.PARENTID AND D6.STATUS = 'ACTIVE' AND D6.USAGEYN = 1
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D5 ON D5.DISPLAYITEMCATEGORYID=D6.PARENTID AND D5.STATUS = 'ACTIVE' AND D5.USAGEYN = 1
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D4 ON D4.DISPLAYITEMCATEGORYID=D5.PARENTID AND D4.STATUS = 'ACTIVE' AND D4.USAGEYN = 1
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D3 ON D3.DISPLAYITEMCATEGORYID=D4.PARENTID AND D3.STATUS = 'ACTIVE' AND D3.USAGEYN = 1
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D2 ON D2.DISPLAYITEMCATEGORYID=D3.PARENTID AND D2.STATUS = 'ACTIVE' AND D2.USAGEYN = 1
        LEFT JOIN ODS.DISPLAY_ITEM_CATEGORIES D1 ON D1.DISPLAYITEMCATEGORYID=D2.PARENTID AND D1.STATUS = 'ACTIVE' AND D1.USAGEYN = 1
        WHERE D7.STATUS = 'ACTIVE' AND D7.USAGEYN = 1;
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_audit(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST;
        """)
        q = ("""
        SELECT edh.ORIGINID as PRODUCTID
         , MAX(CASE WHEN edh.DESCRIPTION LIKE '%관리%' THEN 1 ELSE 0 END) as MNG_CLEAN_YN
         , MAX(CASE WHEN edh.DESCRIPTION LIKE '%노출%' THEN 1 ELSE 0 END) as INT_CLEAN_YN
         , CURRENT_TIMESTAMP as CREATEDAT
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST
        FROM ods.EDIT_HISTORIES edh
        WHERE edh.HISTORYTYPE = 'PRODUCT'
        AND edh.DESCRIPTION like '%카테고리%'
        AND edh.CRUDTYPE in ('C','U')
        GROUP BY edh.ORIGINID;
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


# cleansing field removed...so cannot use this table
def step2_clean_product_list_product_clean(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_PRODUCTCLEAN_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_PRODUCTCLEAN
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_PRODUCTCLEAN_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_PRODUCTCLEAN;
        """)
        q = ("""
        SELECT p.PRODUCTID
         , 1 as MNG_CLEAN_YN
         , 1 as INT_CLEAN_YN
         , CURRENT_TIMESTAMP as CREATEDAT
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_PRODUCTCLEAN
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST c
        INNER JOIN ods.PRODUCTS p on p.productid = c.productid
        WHERE p.cleansing = 'CLEAN';
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_r21(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21;
        """)
        q = ("""
        SELECT DISTINCT PRODUCTID
            , 1 AS MNG_CLEAN_YN
            , 1 AS INT_CLEAN_YN
            , CURRENT_TIMESTAMP as CREATEDAT	
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21
        FROM ODS.RETAIL_ESTIMATION_ITEMS;
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_jikgu(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU;
        """)
        q = ("""
        SELECT DISTINCT PRODUCTID
            , 1 AS MNG_CLEAN_YN
            , 1 AS INT_CLEAN_YN
            , CURRENT_TIMESTAMP as CREATEDAT
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU
        FROM BIMART.DDD_PRODUCT_VENDOR_ITEM PVI 
        JOIN BIMART.MANAGEMENT_CATEGORY_HIER_CURR MCHC on MCHC.MNGCATEID = PVI.CATEGORYID 
        WHERE PVI.VENDOR_ID = 'C00051747'


        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_book_rocket(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET;
        """)
        q = ("""
        SELECT DISTINCT PRODUCTID
            , 1 AS MNG_CLEAN_YN
            , 1 AS INT_CLEAN_YN
            , CURRENT_TIMESTAMP as CREATEDAT
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET
        FROM BIMART.DDD_PRODUCT_VENDOR_ITEM PVI 
        JOIN BIMART.MANAGEMENT_CATEGORY_HIER_CURR MCHC on MCHC.MNGCATEID = PVI.CATEGORYID 
        WHERE PVI.VENDOR_ID = 'A00010028'
        AND MCHC.CATECODE2 = '2114'

        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_browsing(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING;
        """)
        q = ("""
        
        SELECT DISTINCT b.PRODUCTID
            , 1 AS MNG_CLEAN_YN
            , 1 AS INT_CLEAN_YN
            , CURRENT_TIMESTAMP as CREATEDAT
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING
        FROM sb_pbs.tmp_jyj_browsing_accumulation_mapping b
        JOIN ODS.DISPLAY_CATEGORY_DISPLAY_ITEM_REL ICM ON (ICM.DISPLAYITEMID=b.PRODUCTID AND ICM.DELETED=0 AND ICM.REPRESENTATIVE=1)
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = ICM.displaycategorycode)
        WHERE m.catecode1 NOT IN (102984,77834,62588) 
        /* Toy, Homedec, Elec where browsing data showed bad performance */
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step2_clean_product_list_merge(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL;
        """)
        q = ("""
        SELECT 
            PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL
        FROM (
            SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN  FROM (
                SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST
                UNION
                SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_R21
                UNION
                SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_JIKGU
                UNION
                SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BOOK_ROCKET
                UNION
                SELECT PRODUCTID, MNG_CLEAN_YN, INT_CLEAN_YN FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_BROWSING
            ) X
        )
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step3_product_list(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST;
        """)
        q = ("""
        SELECT Z.PRODUCTID
             , Z.CATEGORYID
             , Z.CATECODE
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST
        FROM (
            SELECT PRD.PRODUCTID
               , PRD.CATEGORYID
               , ICM.DISPLAYCATEGORYCODE as CATECODE
               , ICM.MODIFIEDAT
               , ROW_NUMBER() OVER(PARTITION BY PRD.PRODUCTID ORDER BY ICM.MODIFIEDAT DESC) AS MODIFIED_RANKING
            FROM ODS.PRODUCTS PRD
            JOIN ODS.DISPLAY_CATEGORY_DISPLAY_ITEM_REL ICM ON (ICM.DISPLAYITEMID=PRD.PRODUCTID AND ICM.DELETED=0 AND ICM.REPRESENTATIVE=1)
            JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING IAC  ON (ICM.DISPLAYCATEGORYCODE=IAC.CATECODE)

            WHERE PRD.DIVISIONTYPE = 'GOODS'

            AND IAC.CATECODE1 IN (69182,66679,77834,80285,63897,102984,62588,79648,103371,59258,65799,78647,56112,76844,79138)
        ) Z
        WHERE Z.MODIFIED_RANKING = 1
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step4_item_list(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST;
        """)
        q = ("""
        SELECT Z.PRODUCTID
             , Z.ITEMID
             , Z.VENDORITEMID
             , Z.VENDORID
             , Z.LOGISTICS
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST
        FROM (
            SELECT MOS.PRODUCTID
                 , VIT.ITEMID
                 , VIT.VENDORITEMID
                 , VIT.VENDORID
                 , VIT.MODIFIEDAT
                 , VIT.LOGISTICS
                 , ROW_NUMBER() OVER(PARTITION BY MOS.PRODUCTID ORDER BY VIT.LOGISTICS DESC, VIT.MODIFIEDAT DESC) AS MODIFIED_RANKING
            FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST MOS
            JOIN ODS.VENDOR_ITEMS VIT ON (MOS.PRODUCTID = VIT.PRODUCTID)
            WHERE VIT.DIVISIONTYPE = 'GOODS'
        ) Z
        WHERE Z.MODIFIED_RANKING = 1
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step5_product_name(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME;
        """)
        q = ("""
        SELECT MOS.PRODUCTID
            , REGEXP_REPLACE(CASE WHEN PLE.PRODUCTID IS NULL THEN PLC.NAME ELSE CONCAT(CONCAT(PLC.NAME, ' '), PLE.NAME) END,'\t|\n|\r|,',' ') AS PRODUCTNAME
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST MOS
        JOIN ODS.PRODUCT_LOCALES PLC ON (MOS.PRODUCTID = PLC.PRODUCTID AND PLC.LOCALE = 'ko_KR')
        LEFT JOIN ODS.PRODUCT_LOCALES PLE ON (MOS.PRODUCTID = PLE.PRODUCTID AND PLE.LOCALE = 'en_US')
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step6_brand_manufacture(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS;
        """)
        q = ("""
        SELECT MOS.PRODUCTID
            , PPT.ATTRIBUTEID
            , REGEXP_REPLACE(LISTAGG(PPT.PROPERTYVALUE, ' ') WITHIN GROUP (ORDER BY PPT.PROPERTYVALUE DESC),'\t|\n|\r|,',' ') AS PROPERTYVALUE
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST MOS
        JOIN ODS.PRODUCT_PROPERTIES PPT ON (MOS.PRODUCTID = PPT.PRODUCTID)
        WHERE PPT.PROPERTYVALUE <> ''
        AND PPT.ATTRIBUTEID IN ('2147483643','2147483644','2147483640')
        GROUP BY 
            MOS.PRODUCTID
            , PPT.ATTRIBUTEID
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step7_item_attribute(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR;
        """)
        q = ("""
        SELECT MOS.PRODUCTID
            , MOS.ITEMID
            , REGEXP_REPLACE(LISTAGG(AVL.NAME, ' ') WITHIN GROUP (ORDER BY ITA.ATTRIBUTETYPEID),'\t|\n|\r|,',' ') AS ATTRIBUTEVALUE
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST MOS
        JOIN ODS.ITEM_ATTRIBUTES ITA ON (MOS.ITEMID = ITA.ITEMID)
        JOIN ODS.ATTRIBUTE_VALUE_LOCALES AVL ON (ITA.ATTRIBUTEVALUEID = AVL.ATTRIBUTEVALUEID AND AVL.LOCALE = 'ko_KR')
        WHERE 1=1
        GROUP BY 
            MOS.PRODUCTID
            , MOS.ITEMID
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step8_item_name(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO;
        """)
        q = ("""
        SELECT MOS.PRODUCTID
            , MOS.ITEMID
            , REGEXP_REPLACE(CASE WHEN ILE.ITEMID IS NULL THEN ILK.NAME ELSE CONCAT(CONCAT(ILK.NAME,' '),ILE.NAME) END,'\t|\n|\r|,',' ') AS ITEMNAME
            , REGEXP_REPLACE(ITM.MODELNO,'\t|\n|\r|,',' ') AS MODELNO
            , REGEXP_REPLACE(ITM.BARCODE,'\t|\n|\r|,',' ') AS BARCODE
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST MOS
        JOIN ODS.ITEMS ITM ON (MOS.ITEMID = ITM.ITEMID)
        JOIN ODS.ITEM_LOCALES ILK ON (MOS.ITEMID = ILK.ITEMID AND ILK.LOCALE = 'ko_KR')
        LEFT JOIN ODS.ITEM_LOCALES ILE ON (MOS.ITEMID = ILE.ITEMID AND ILE.LOCALE = 'en_US')
        WHERE ITM.VALID = 1
        AND ITM.DIVISIONTYPE = 'GOODS'
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step9_product_data(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA;
        """)
        q = ("""
        SELECT MOS.PRODUCTID
            , MOS.CATEGORYID
            , MOS.CATECODE
            , REPLACE(
            PDN.PRODUCTNAME || ' ' ||
            ITI.ITEMNAME || ' ' || 
            CASE WHEN BRD.PROPERTYVALUE IS NULL THEN ' ' ELSE BRD.PROPERTYVALUE || ' ' END ||
            CASE WHEN MUF.PROPERTYVALUE IS NULL THEN ' ' ELSE MUF.PROPERTYVALUE || ' ' END || 
            CASE WHEN ITI.BARCODE IS NULL THEN ' ' ELSE ITI.BARCODE || ' ' END ||
            CASE WHEN ITI.MODELNO IS NULL THEN ' ' ELSE ITI.MODELNO || ' ' END ||
            CASE WHEN ITA.ATTRIBUTEVALUE IS NULL THEN ' ' ELSE ITA.ATTRIBUTEVALUE || ' ' END ||
            CASE WHEN SHT.PROPERTYVALUE IS NULL THEN ' ' ELSE SHT.PROPERTYVALUE || ' ' END
            ,'|',' ') AS INPUT_STRING
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP03_INT_PRODUCT_LIST MOS
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST ITM ON (MOS.PRODUCTID = ITM.PRODUCTID)
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP05_INT_PRODUCTNAME PDN ON (MOS.PRODUCTID = PDN.PRODUCTID)
        LEFT JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS BRD ON (MOS.PRODUCTID = BRD.PRODUCTID AND BRD.ATTRIBUTEID = '2147483643')
        LEFT JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS MUF ON (MOS.PRODUCTID = MUF.PRODUCTID AND MUF.ATTRIBUTEID = '2147483644')
        LEFT JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP06_INT_BMS SHT ON (MOS.PRODUCTID = SHT.PRODUCTID AND SHT.ATTRIBUTEID = '2147483640')
        LEFT JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP07_INT_ITEM_ATTR ITA ON (MOS.PRODUCTID = ITA.PRODUCTID)
        LEFT JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP08_INT_ITEM_INFO ITI ON (MOS.PRODUCTID = ITI.PRODUCTID)
        """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step12_train_and_validate_data(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN;
        """)
        q = ("""
        SET SEED TO 1;
        SELECT mos.PRODUCTID, RANK() OVER(ORDER BY RANDOM()) % 10 as DATA_TYPE
            INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN
            FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
            JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP02_CLEAN_PRODUCT_LIST_ALL cln ON (mos.PRODUCTID = cln.PRODUCTID)
            JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP04_INT_ITEM_LIST itm ON (mos.PRODUCTID = itm.PRODUCTID)
         WHERE cln.INT_CLEAN_YN = 1 
         or itm.LOGISTICS = 1
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def get_insert_sql_string(file_name):
    total_query_lines = list()

    cnt = 0
    query_lines = list()

    with open(file_name, 'r') as f:
        for line in f:
            if cnt == 0:
                query_lines = list()
            elif cnt % 1000 == 0:
                total_query_lines.append(query_lines)
                query_lines = list()

            sliced_line = line.split('\t')

            query_line_str = ''
            if len(sliced_line) == 2:
                (catecode, product_name) = line.split("\t")
                query_line_str = "(" + catecode + ",'" + \
                                 product_name.replace(":", "").replace("'", "").strip('\n') + "')"

            elif len(sliced_line) == 3:
                (product_id, catecode, product_name) = line.split("\t")

                query_line_str = "(" + product_id + "," + catecode + ",'" + \
                                 product_name.replace(":", "").replace("'", "").strip('\n') + "')"

            query_lines.append(query_line_str)

            cnt = cnt + 1

        total_query_lines.append(query_lines)

    return total_query_lines


def step14_feedback_cate_name(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME;
        """)
        q = ("""
        CREATE TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME 
        (catecode bigint, input_string varchar(2000) ) ;
         """)

        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)

        insert_list = get_insert_sql_string(const.get_feedback_cate_name_file_name())

        for i in insert_list:
            insert_q = ("""
            INSERT into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME 
            values $replace_value ;
            """)

            if i is not None:
                sql = text(insert_q.replace("$replace_value", ','.join(i)))
                engine.execute(sql)

        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step13_train_augment_data_old(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT;
        """)
        q = ("""
        SET SEED TO 1;
        SELECT *
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT
        FROM (
        (SELECT catecode, input_string FROM (
            SELECT
            mos.catecode,
            m.catecode2,
            m.catecode3,
            m.catecode4,
            regexp_replace(CASE WHEN m.catecode6 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4), ' '), m.cate5), ' '), m.cate6)
                WHEN m.catecode5 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4), ' '), m.cate5)
                WHEN m.catecode4 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4)
                ELSE CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3) END, ' 외', '') AS input_string
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = mos.catecode)
        LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln on (cln.PRODUCTID = mos.PRODUCTID)
        WHERE cln.PRODUCTID IS NULL
        ) WHERE ((regexp_instr(input_string, '기타') = 0) or catecode2 = 85636 or catecode3 = 77700 or catecode4 in (77656,72587))
        ORDER BY RANDOM()
        LIMIT 2500000)
        UNION ALL
        (SELECT catecode, input_string FROM (
            SELECT
            mos.catecode,
            m.catecode2,
            m.catecode3,
            m.catecode4,
            regexp_replace(CASE WHEN m.catecode6 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate2, ' '), m.cate3), ' '), m.cate4), ' '), m.cate5), ' '), m.cate6)
                WHEN m.catecode5 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate2, ' '), m.cate3), ' '), m.cate4), ' '), m.cate5)
                WHEN m.catecode4 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(m.cate2, ' '), m.cate3), ' '), m.cate4)
                ELSE CONCAT(CONCAT(m.cate2, ' '), m.cate3) END, ' 외', '') AS input_string
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = mos.catecode)
        LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln on (cln.PRODUCTID = mos.PRODUCTID)
        WHERE cln.PRODUCTID IS NULL
        ) WHERE ((regexp_instr(input_string, '기타') = 0) or catecode2 = 85636 or catecode3 = 77700 or catecode4 in (77656,72587))
        ORDER BY RANDOM()
        LIMIT 2500000)
        UNION ALL
        (SELECT catecode, input_string FROM (
            SELECT
            mos.catecode,
            m.catecode2,
            m.catecode3,
            m.catecode4,
            regexp_replace(CASE WHEN m.catecode6 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4), ' '), m.cate5), ' '), m.cate6)
                WHEN m.catecode5 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4), ' '), m.cate5)
                WHEN m.catecode4 is not null THEN CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3), ' '), m.cate4)
                ELSE CONCAT(CONCAT(CONCAT(CONCAT(m.cate1, ' '), m.cate2), ' '), m.cate3) END, ' 외', '') AS input_string
        FROM (SELECT DISTINCT catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA) mos
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = mos.catecode)
        CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5)
        ) WHERE ((regexp_instr(input_string, '기타') = 0) or catecode2 = 85636 or catecode3 = 77700 or catecode4 in (77656,72587)))
        );
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step14_feedback_catalog(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG;
        """)
        q = ("""
        CREATE TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG 
        (productid bigint, catecode bigint, input_string varchar(2000) ) ;
         """)

        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)

        insert_list = get_insert_sql_string(const.get_feedback_catalog_file_name())

        for i in insert_list:
            insert_q = ("""
            INSERT into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG 
            values $replace_value ;
            """)

            if i is not None:
                sql = text(insert_q.replace("$replace_value", ','.join(i)))
                engine.execute(sql)

        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step14_feedback_seller(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER;
        """)
        q = ("""
        CREATE TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER 
        (catecode bigint, input_string varchar(2000) ) ;
         """)

        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)

        insert_list = get_insert_sql_string(const.get_feedback_seller_file_name())

        for i in insert_list:
            insert_q = ("""
            INSERT into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER 
            values $replace_value ;
            """)

            if i is not None:
                sql = text(insert_q.replace("$replace_value", ','.join(i)))
                engine.execute(sql)

        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step14_feedback_manual(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL;
        """)
        q = ("""
        CREATE TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL 
        (productid bigint, catecode bigint, input_string varchar(2000) ) ;
         """)

        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)

        insert_list = get_insert_sql_string(const.get_feedback_manual_file_name())

        for i in insert_list:
            insert_q = ("""
            INSERT into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL 
            values $replace_value ;
            """)

            if i is not None:
                sql = text(insert_q.replace("$replace_value", ','.join(i)))
                engine.execute(sql)

        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step95_cate_no_map(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP;
        """)
        q = ("""
        SELECT 2 as cate_level, 104121 as nomap_catecode, 'event category' as reason
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP;
        insert into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP values
        (2, 106524, 'event category'),
        (2, 106369, 'event category'),
        (2, 106023, 'event category'),
        (2, 103873, 'event category'),
        (2, 102321, 'event category'),
        (2, 84214, 'event category'),
        (2, 82711, 'event category'),
        (2, 83172, 'event category'),
        (2, 76446, 'event category'),
        (2, 104330, 'event category');
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step97_cate_remap(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP;
        """)
        q = ("""
        SELECT 99999 as original_catecode, 99999 as dest_catecode, 'this is test record' as reason
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP;
        ;
        insert into SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP values
        (105777,63691, 'installation'),
        (105778,63694, 'installation'),
        (105779,63696, 'installation'),
        (105780,63695, 'installation'),
        (105781,63698, 'installation'),
        (105782,63702, 'installation'),
        (105783,63706, 'installation'),
        (105784,63713, 'installation'),
        (105785,63715, 'installation'),
        (105793,63784, 'installation');
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step98_train_data_create(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN;
        """)
        q = ("""
        SET SEED TO 1;
        SELECT
            b.productid,
            b.catecode,
            m.catecode1 int_catecode1, 
            m.cate1 int_cate1, 
            m.catecode2 int_catecode2,
            m.cate2 int_cate2,
            m.catecode3 int_catecode3,
            m.cate3 int_cate3,
            m.catecode4 int_catecode4,
            m.cate4 int_cate4,
            m.catecode5 int_catecode5,
            m.cate5 int_cate5,
            m.catecode6 int_catecode6,
            m.cate6 int_cate6,
            REGEXP_REPLACE(b.input_string, '\t', ' ') input_string
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        FROM (
            SELECT a.productid, case when r.dest_catecode is not null then r.dest_catecode else a.catecode end catecode, a.input_string 
            FROM (
                (
                    SELECT 
                        mos.productid,
                        mos.catecode,
                        mos.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln ON (mos.PRODUCTID = cln.PRODUCTID)
                    WHERE cln.DATA_TYPE > 0  
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        a.catecode,
                        a.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME a
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = a.catecode)
                    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER
                )
                UNION ALL
                (
                    SELECT 
                        y.productid,
                        y.catecode,
                        y.input_string
                    FROM (
                        SELECT
                            a.productid,
                            a.catecode,
                            mos.input_string
                        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_TRAINDATA_AUGMENT_CATE_IN_PRODUCT a
                        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos on (mos.productid = a.productid)
                        UNION
                        SELECT
                            a.productid,
                            a.catecode,
                            mos.input_string
                        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_TRAINDATA_AUGMENT_GUIDELINE_IN_PRODUCT a
                        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos on (mos.productid = a.productid)
                    ) y
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = y.catecode)
                    WHERE m.catecode1 IN (56112, 102984, 78647, 66679, 103371) 
                    ORDER BY RANDOM()
                    LIMIT 500000
                )
            ) a
            JOIN ( SELECT distinct catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA ) d ON (d.catecode = a.catecode)
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP r ON (r.original_catecode = a.catecode)
        ) b 
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = b.catecode)
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 1) n1 on n1.nomap_catecode = m.catecode1
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 2) n2 on n2.nomap_catecode = m.catecode2
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 3) n3 on n3.nomap_catecode = m.catecode3
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 4) n4 on n4.nomap_catecode = m.catecode4
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 5) n5 on n5.nomap_catecode = m.catecode5
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 6) n6 on n6.nomap_catecode = m.catecode6
        WHERE n1.nomap_catecode is null
        AND n2.nomap_catecode is null
        AND n3.nomap_catecode is null
        AND n4.nomap_catecode is null
        AND n5.nomap_catecode is null
        AND n6.nomap_catecode is null;
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step98_train_data_create_old(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN;
        """)
        q = ("""
        SET SEED TO 1;
        SELECT
            b.productid,
            b.catecode,
            m.catecode1 int_catecode1, 
            m.cate1 int_cate1, 
            m.catecode2 int_catecode2,
            m.cate2 int_cate2,
            m.catecode3 int_catecode3,
            m.cate3 int_cate3,
            m.catecode4 int_catecode4,
            m.cate4 int_cate4,
            m.catecode5 int_catecode5,
            m.cate5 int_cate5,
            m.catecode6 int_catecode6,
            m.cate6 int_cate6,
            REGEXP_REPLACE(b.input_string, '\t', ' ') input_string
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        FROM (
            SELECT a.productid, case when r.dest_catecode is not null then r.dest_catecode else a.catecode end catecode, a.input_string 
            FROM (
                (
                    SELECT 
                        mos.productid,
                        mos.catecode,
                        mos.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln ON (mos.PRODUCTID = cln.PRODUCTID)
                    WHERE cln.DATA_TYPE > 0  
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        a.catecode,
                        a.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP13_INT_TRAINDATA_AUGMENT a
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = a.catecode)
                    WHERE m.catecode1 NOT IN (76844)
                )
                UNION ALL
                (
                    SELECT
                        productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL
                )
                UNION ALL
                (
                    SELECT 
                        y.productid,
                        y.catecode,
                        y.input_string
                    FROM (
                        SELECT
                            a.productid,
                            a.catecode,
                            mos.input_string
                        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_TRAINDATA_AUGMENT_CATE_IN_PRODUCT a
                        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos on (mos.productid = a.productid)
                        UNION
                        SELECT
                            a.productid,
                            a.catecode,
                            mos.input_string
                        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_TRAINDATA_AUGMENT_GUIDELINE_IN_PRODUCT a
                        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos on (mos.productid = a.productid)
                    ) y
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = y.catecode)
                    WHERE m.catecode1 IN (56112, 102984, 78647, 66679, 103371) 
                    ORDER BY RANDOM()
                    LIMIT 500000
                )
            ) a
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP r ON (r.original_catecode = a.catecode)
        ) b 
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = b.catecode)
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 1) n1 on n1.nomap_catecode = m.catecode1
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 2) n2 on n2.nomap_catecode = m.catecode2
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 3) n3 on n3.nomap_catecode = m.catecode3
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 4) n4 on n4.nomap_catecode = m.catecode4
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 5) n5 on n5.nomap_catecode = m.catecode5
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 6) n6 on n6.nomap_catecode = m.catecode6
        WHERE n1.nomap_catecode is null
        AND n2.nomap_catecode is null
        AND n3.nomap_catecode is null
        AND n4.nomap_catecode is null
        AND n5.nomap_catecode is null
        AND n6.nomap_catecode is null;
         """)
        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def step99_validate_data_create(backup=True):
    logger.info('start process:' + sys._getframe().f_code.co_name)
    try:
        engine = create_engine(DB_SANDBOX_URL)
        backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE_OLD;
        ALTER TABLE SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE
        RENAME TO CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE_OLD;
        """)
        no_backup_q = ("""
        DROP TABLE IF EXISTS SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE;
        """)
        q = ("""
        SELECT 
            a.productid,
            a.catecode,
            m.catecode1 int_catecode1, 
            m.cate1 int_cate1, 
            m.catecode2 int_catecode2,
            m.cate2 int_cate2,
            m.catecode3 int_catecode3,
            m.cate3 int_cate3,
            m.catecode4 int_catecode4,
            m.cate4 int_cate4,
            m.catecode5 int_catecode5,
            m.cate5 int_cate5,
            m.catecode6 int_catecode6,
            m.cate6 int_cate6,
            REGEXP_REPLACE(a.input_string, '\t', ' ') input_string
        INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE
        FROM (
            SELECT 
                mos.productid,
                case when r.dest_catecode is not null then r.dest_catecode else mos.catecode end catecode,
                mos.input_string
            FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
            JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln ON (mos.PRODUCTID = cln.PRODUCTID)
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN T ON T.PRODUCTID = mos.PRODUCTID
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP r ON (r.original_catecode = mos.catecode)
            WHERE cln.DATA_TYPE = 0 
            AND T.PRODUCTID IS NULL
        ) a 
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = a.catecode)
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 1) n1 on n1.nomap_catecode = m.catecode1
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 2) n2 on n2.nomap_catecode = m.catecode2
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 3) n3 on n3.nomap_catecode = m.catecode3
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 4) n4 on n4.nomap_catecode = m.catecode4
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 5) n5 on n5.nomap_catecode = m.catecode5
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 6) n6 on n6.nomap_catecode = m.catecode6
        WHERE n1.nomap_catecode is null
        AND n2.nomap_catecode is null
        AND n3.nomap_catecode is null
        AND n4.nomap_catecode is null
        AND n5.nomap_catecode is null
        AND n6.nomap_catecode is null;
        """)

        if backup:
            sql = text(backup_q + '\n' + q)
        else:
            sql = text(no_backup_q + '\n' + q)

        engine.execute(sql)
        logger.info('completed process:' + sys._getframe().f_code.co_name)

        return True
    except Exception as e:
        logger.error(e)
        return False


def get_vitamin_train_data():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """ set seed to 1; 
        SELECT 
            productid,
            catecode::varchar,
            int_catecode1::varchar, 
            int_cate1, 
            int_catecode2::varchar,
            int_cate2,
            int_catecode3::varchar,
            int_cate3,
            int_catecode4::varchar,
            int_cate4,
            int_catecode5::varchar,
            int_cate5,
            int_catecode6::varchar,
            int_cate6,
            input_string
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        WHERE int_catecode3 is not null
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_train_vitamin_file_name(), sep='\t', encoding='utf-8', header=False, index=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_augment_temp():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """ 
        SELECT
            b.productid,
            b.catecode::varchar,
            m.catecode1::varchar int_catecode1, 
            m.cate1 int_cate1, 
            m.catecode2::varchar int_catecode2,
            m.cate2 int_cate2,
            m.catecode3::varchar int_catecode3,
            m.cate3 int_cate3,
            m.catecode4::varchar int_catecode4,
            m.cate4 int_cate4,
            m.catecode5::varchar int_catecode5,
            m.cate5 int_cate5,
            m.catecode6::varchar int_catecode6,
            m.cate6 int_cate6,
            REGEXP_REPLACE(b.input_string, '\t', ' ') input_string
        FROM (
            SELECT a.productid, case when r.dest_catecode is not null then r.dest_catecode else a.catecode end catecode, a.input_string 
            FROM (
                (
                    SELECT
                        0 as productid,
                        a.catecode,
                        a.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATE_NAME a
                    JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = a.catecode)
                    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_MANUAL
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_CATALOG
                )
                UNION ALL
                (
                    SELECT
                        0 as productid,
                        catecode,
                        input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_FEEDBACK_SELLER
                )
            ) a
            JOIN ( SELECT distinct catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA ) d ON (d.catecode = a.catecode)
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP97_INT_REMAP r ON (r.original_catecode = a.catecode)
        ) b 
        JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING m ON (m.catecode = b.catecode)
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 1) n1 on n1.nomap_catecode = m.catecode1
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 2) n2 on n2.nomap_catecode = m.catecode2
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 3) n3 on n3.nomap_catecode = m.catecode3
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 4) n4 on n4.nomap_catecode = m.catecode4
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 5) n5 on n5.nomap_catecode = m.catecode5
        LEFT JOIN (SELECT DISTINCT nomap_catecode FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP95_INT_NOMAP WHERE cate_level = 6) n6 on n6.nomap_catecode = m.catecode6
        WHERE n1.nomap_catecode is null
        AND n2.nomap_catecode is null
        AND n3.nomap_catecode is null
        AND n4.nomap_catecode is null
        AND n5.nomap_catecode is null
        AND n6.nomap_catecode is null;
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_augment_temp(), sep='\t', encoding='utf-8', header=False, index=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_vitamin_validate_data():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """ 
        SELECT 
            productid,
            catecode::varchar,
            int_catecode1::varchar, 
            int_cate1, 
            int_catecode2::varchar,
            int_cate2,
            int_catecode3::varchar,
            int_cate3,
            int_catecode4::varchar,
            int_cate4,
            int_catecode5::varchar,
            int_cate5,
            int_catecode6::varchar,
            int_cate6,
            input_string
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE
        WHERE int_catecode3 is not null
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_test_vitamin_file_name(), sep='\t', encoding='utf-8', header=False, index=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_train_product_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """
        SELECT distinct
            productid::bigint as product_id
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        WHERE productid <> 0
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_train_product_id_file_name(),
              sep='\t', encoding='utf-8', header=True, index=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_train_product_id_with_catecode():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """
        SELECT distinct
            productid::bigint as product_id,
            catecode::bigint as catecode
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP98_INT_TRAIN
        WHERE productid <> 0
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_train_product_id_with_catecode_file_name(),
              sep='\t', encoding='utf-8', header=False, index=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_validate_product_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """
        SELECT DISTINCT
            productid::bigint  as product_id
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE
        WHERE productid <> 0
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_validate_product_id_file_name(),
              sep='\t', encoding='utf-8', header=True, index=False)
    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_validate_product_id_with_catecode():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """
        SELECT DISTINCT
            productid::bigint  as product_id,
            catecode::bigint as catecode
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP99_INT_VALIDATE
        WHERE productid <> 0
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_validate_product_id_with_catecode_file_name(),
              sep='\t', encoding='utf-8', header=False, index=False)
    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_category_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """  
        SELECT DISTINCT 
        catecode::varchar
        , catecode1::varchar
        , cate1
        , catecode2::varchar
        , cate2
        , catecode3::varchar
        , cate3
        , catecode4::varchar
        , cate4
        , catecode5::varchar
        , cate5
        , catecode6::varchar
        , cate6
        FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP01_INT_MAPPING
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_category_file_name(), sep='\t', encoding='utf-8', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_jikgu_product_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """  
        SELECT DISTINCT productid, 1
        FROM bimart.ddd_product_vendor_item 
        WHERE vendor_id = 'C00051747' 
        AND valid_products = 1 
        AND valid_items = 1
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_jikgu_prod_dictionary_file_name(), sep='\t', encoding='utf-8', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_cleansed_product_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """  
        SELECT DISTINCT productid, 1 from sb_pbs.tmp_jyj_browsing_accumulation_mapping 
        UNION 
        SELECT DISTINCT productid, 1 from ODS.RETAIL_ESTIMATION_ITEMS where productid is not null
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_cleansed_prod_dictionary_file_name(), sep='\t', encoding='utf-8', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_event_category_id():
    logger.info('start process:' + sys._getframe().f_code.co_name)

    engine = create_engine(DB_SANDBOX_URL)

    q = """  
        SELECT DISTINCT 
        displayitemcategorycode categorycode
        , assetKey
        from ods.display_item_category_assets 
        where assetkey = 'EVENT_CATEGORY'
        """

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(const.get_raw_event_category_file_name(), sep='\t', encoding='utf-8', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def get_garbage_product_names(year_month):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    year = year_month[:4]
    if len(year) != 4:
        raise Exception("Error in Year_month (e.g. input should be like 201808) ")

    month = year_month[4:]
    if len(month) != 2:
        raise Exception("Error in Year_month (e.g. input should be like 201808) ")

    base_month = datetime.date(int(year), int(month), 1)
    next_month = base_month + relativedelta(months=+1)

    engine = create_engine(DB_SANDBOX_URL)

    q = """  
        select productid, vendorid, dt, productname
        from (
        select productid 
        , vendorid
        , TO_CHAR(vi.createdat, 'YYYYMMDD') dt
        , productname 
        from ods.vendor_inventories vi
        where status = 'APPROVED' 
        and vi.createdat >= trunc('$base_month 00:00:00'::timestamp) AND vi.createdat < trunc('$next_month 00:00:00'::timestamp)
        ) a
        """

    q = q.replace('$base_month', str(base_month))
    q = q.replace('$next_month', str(next_month))

    # noinspection PyUnresolvedReferences
    df = pd.read_sql_query(q, engine)

    df.to_csv(
        const.get_garbage_product_names_file_name(year_month), sep='\t', encoding='utf-8', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def merge_cds(file_path, file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    os.system("cat " + file_path + "part* > " + file_name)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def create_cds_file(input_file_name, product_cate_file_name, output_file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    # noinspection PyUnresolvedReferences
    cds_df = pd.read_csv(
        input_file_name,
        names=['itemId',
               'productId',
               'categoryCode',
               'repImgLink',
               'originalAttr',
               'normalizedAttr'],
        sep='\t',
        dtype=[('itemId', 'long'),
               ('productId', 'long'),
               ('categoryCode', 'str'),
               ('repImgLink', 'str'),
               ('originalAttr', 'str'),
               ('normalizedAttr', 'str')])

    # noinspection PyUnresolvedReferences
    prod_cate_df = pd.read_csv(
        product_cate_file_name,
        names=['productId',
               'categoryCode'],
        sep='\t',
        dtype=[('productId', 'long'),
               ('categoryCode', 'str')])

    # noinspection PyUnresolvedReferences
    cate_df = pd.read_csv(
        const.get_raw_category_file_name(),
        sep='\t',
        names=['categoryCode',
               'categoryCode1',
               'cate1',
               'categoryCode2',
               'cate2',
               'categoryCode3',
               'cate3',
               'categoryCode4',
               'cate4',
               'categoryCode5',
               'cate5',
               'categoryCode6',
               'cate6'],
        dtype=[('categoryCode', 'str'),
               ('categoryCode1', 'str'),
               ('cate1', 'str'),
               ('categoryCode2', 'str'),
               ('cate2', 'str'),
               ('categoryCode3', 'str'),
               ('cate3', 'str'),
               ('categoryCode4', 'str'),
               ('cate4', 'str'),
               ('categoryCode5', 'str'),
               ('cate5', 'str'),
               ('categoryCode6', 'str'),
               ('cate6', 'str')])

    catecode1_list = ['69182', '66679', '77834', '80285', '63897', '102984', '62588', '79648', '103371', '59258', '65799', '78647', '56112', '76844', '79138']

    cate_df_filtered = cate_df[cate_df.categoryCode1.isin(catecode1_list)]
    print(cds_df.shape)
    # noinspection PyUnresolvedReferences
    product_cate_merged = pd.merge(cds_df.drop('categoryCode', 1), prod_cate_df, on='productId')
    print(product_cate_merged.shape)
    # noinspection PyUnresolvedReferences
    combined_1 = pd.merge(product_cate_merged, cate_df_filtered, on='categoryCode')

    cate2_df_filtered = cate_df_filtered[['categoryCode2']].drop_duplicates()
    # noinspection PyUnresolvedReferences
    combined_2 = pd.merge(combined_1, cate2_df_filtered, on='categoryCode2')

    cate3_df_filtered = cate_df_filtered[['categoryCode3']].drop_duplicates()
    # noinspection PyUnresolvedReferences
    combined_3 = pd.merge(combined_2, cate3_df_filtered, on='categoryCode3')

    train = combined_3[
        ['productId',
         'categoryCode',
         'categoryCode1',
         'cate1',
         'categoryCode2',
         'cate2',
         'categoryCode3',
         'cate3',
         'categoryCode4',
         'cate4',
         'categoryCode5',
         'cate5',
         'categoryCode6',
         'cate6',
         'originalAttr']]

    train[(train.categoryCode != 0)].to_csv(output_file_name, sep='\t', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def create_audit_file(input_file_name, output_file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    # noinspection PyUnresolvedReferences
    cds_df = pd.read_csv(
        input_file_name,
        names=['itemId',
               'productId',
               'categoryCode',
               'originalAttr',
               'normalizedAttr'],
        sep='\t',
        dtype=[('itemId', 'long'),
               ('productId', 'long'),
               ('categoryCode', 'str'),
               ('originalAttr', 'str'),
               ('normalizedAttr', 'str')])

    # noinspection PyUnresolvedReferences
    prod_cate_df = pd.read_csv(
        const.get_raw_category_file_name(),
        sep='\t',
        names=['categoryCode',
               'categoryCode1',
               'cate1',
               'categoryCode2',
               'cate2',
               'categoryCode3',
               'cate3',
               'categoryCode4',
               'cate4',
               'categoryCode5',
               'cate5',
               'categoryCode6',
               'cate6'],
        dtype=[('categoryCode', 'str'),
               ('categoryCode1', 'str'),
               ('cate1', 'str'),
               ('categoryCode2', 'str'),
               ('cate2', 'str'),
               ('categoryCode3', 'str'),
               ('cate3', 'str'),
               ('categoryCode4', 'str'),
               ('cate4', 'str'),
               ('categoryCode5', 'str'),
               ('cate5', 'str'),
               ('categoryCode6', 'str'),
               ('cate6', 'str')])

    # noinspection PyUnresolvedReferences
    combined = pd.merge(cds_df, prod_cate_df, on='categoryCode')

    train = combined[
        ['productId',
         'categoryCode1',
         'categoryCode2',
         'categoryCode3',
         'categoryCode4',
         'categoryCode5',
         'categoryCode6',
         'originalAttr']]

    train[(train.categoryCode1 != 0)].to_csv(output_file_name, sep='\t', index=False, header=False)

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def merge_cds_output(postfix, output_file):

    logger.info('start process:' + sys._getframe().f_code.co_name)

    os.system("rm -rf " + const.get_raw_train_cds_temp_file_name())
    os.system("rm -rf " + const.get_raw_validate_cds_temp_file_name())
    os.system("cat " + const.get_raw_temp_file_path() + "raw_" + postfix + "* > " + output_file)
    os.system("rm -rf " + const.get_raw_temp_file_path() + "cds/")

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def check_file(file_name, token_size):
    with open(file_name, "r") as ins:
        error_no = 0
        line_no = 0
        for line in ins:
            test = line.split('\t')

            if len(test) != int(token_size):
                error_no = error_no + 1
                print(str(len(test)) + str(line))
            line_no = line_no + 1

    print(error_no)
    print(line_no)


def cleanse_file(file_name, token_size):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    f = open(file_name, "r")
    lines = f.readlines()

    f.close()

    f = open(file_name, "w")
    for line in lines:
        splits = line.split('\t')
        if len(splits) == int(token_size):
            f.write(line)

    f.close()

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def delete_wrong_label_row(file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    installment_leaf_list = \
        ['105777',  # (방문설치)하이라이트
         '105778',  # (방문설치)인덕션
         '105779',  # (방문설치)하이브리드
         '105780',  # (방문설치)핫플레이트
         '105781',  # (방문설치)전자레인지
         '105782',  # (방문설치)전기오븐
         '105783',  # (방문설치)오븐레인지
         '105784',  # (방문설치)정수기
         '105785',  # (방문설치)냉 / 온수기
         '105793']  # (방문설치)식기세척기

    event_lv2_list = \
        ['104121',  # 겨울전문관
         '106369',  # 게임전문
         '106023',  # 여름침구
         '103873',  # 침구
         '102321',  # 칼전문관
         '84214',  # 수입주
         '82711',  # 수입식품
         '83172',  # 대용량
         '76446',  # 로드샵
         '104330']  # 배송티켓

    f = open(file_name, "r")
    lines = f.readlines()

    f.close()

    f = open(file_name, "w")
    for line in lines:
        splits = line.split('\t')
        if len(splits) >= 5:
            if splits[1] not in installment_leaf_list and splits[4] not in event_lv2_list:
                f.write(line)

    f.close()

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def cleanse_augmentation_file(input_file_name, output_file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    f = open(input_file_name, "r")
    lines = f.readlines()

    f.close()

    f = open(output_file_name, "w")
    for line in lines:
        splits = line.split('\t')
        if splits[0] != '0':
            f.write(line)

    f.close()

    logger.info('completed process:' + sys._getframe().f_code.co_name)


def add_new_train(current_file_name, input_file_name, output_file_name):
    logger.info('start process:' + sys._getframe().f_code.co_name)

    # noinspection PyUnresolvedReferences
    current_train_data = \
        pd.read_csv(const.get_raw_file_path() + current_file_name, sep='\t',
                    names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                           'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                           'categoryCode6', 'cate6', 'input_str'],
                    dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                           ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                           ('categoryCode4', 'str'), ('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                           ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    logger.info(current_train_data.shape + ': current_train_data shape')

    # noinspection PyUnresolvedReferences
    add_train_data = \
        pd.read_csv(const.get_raw_file_path() + input_file_name, sep='\t',
                    names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                           'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                           'categoryCode6', 'cate6', 'input_str'],
                    dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                           ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                           ('categoryCode4', 'str'), ('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                           ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    logger.info(add_train_data.shape + ': add_train_data shape')

    logger.info(add_train_data.shape[0] + ': rows before removing augmented')
    add_train_data_no_augmented = add_train_data[~(add_train_data.productId == '0')]
    logger.info(add_train_data_no_augmented.shape[0] + ': rows after removing augmented')

    common_train = current_train_data.merge(add_train_data_no_augmented, on=['productId'])
    logger.info(common_train.shape + ': common_train shape')

    no_overlapping_add_train_data = add_train_data_no_augmented[
        (~add_train_data_no_augmented.productId.isin(common_train.productId))]

    logger.info(no_overlapping_add_train_data.shape + ': no_overlapping_add_train_data shape')

    logger.info(current_train_data.shape[0] + ':rows before adding new data')
    final_train_data = current_train_data.append(no_overlapping_add_train_data, ignore_index=True)
    logger.info(final_train_data.shape[0] + ':rows after adding new data')

    final_train_data.to_csv(const.get_raw_file_path() + output_file_name, sep='\t', index=False, header=False)
    logger.info(final_train_data.shape[0] + ':rows on final data')

    logger.info('completed process:' + sys._getframe().f_code.co_name)

    return output_file_name


def add_new_validate(current_file_name, input_file_name, output_file_name, final_train_file_name):

    # noinspection PyUnresolvedReferences
    current_validate_data = \
        pd.read_csv(const.get_raw_file_path() + current_file_name, sep='\t',
                    names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                           'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                           'categoryCode6', 'cate6', 'input_str'],
                    dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                           ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                           ('categoryCode4', 'str'),('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                           ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    logger.info(current_validate_data.shape + ': current_validate_data shape')

    # noinspection PyUnresolvedReferences
    add_validate_data = \
        pd.read_csv(const.get_raw_file_path() + input_file_name, sep='\t',
                    names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                           'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                           'categoryCode6', 'cate6', 'input_str'],
                    dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                           ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                           ('categoryCode4', 'str'),('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                           ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    logger.info(add_validate_data.shape + ': add_validate_data shape')

    # noinspection PyUnresolvedReferences
    final_train_data = \
        pd.read_csv(const.get_raw_file_path() + final_train_file_name, sep='\t',
                    names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                           'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                           'categoryCode6', 'cate6', 'input_str'],
                    dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                           ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                           ('categoryCode4', 'str'),('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                           ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    logger.info(final_train_data.shape + ': final_train_data shape')

    common_validate = current_validate_data.merge(add_validate_data, on=['productId'])
    logger.info(common_validate.shape + ': common_validate shape')

    no_overlapping_add_validate_data = add_validate_data[(~add_validate_data.productId.isin(common_validate.productId))]
    logger.info(no_overlapping_add_validate_data.shape + ': no_overlapping_add_validate_data shape')

    logger.info(current_validate_data.shape[0] + ':rows before adding new data')
    final_validate_data = current_validate_data.append(no_overlapping_add_validate_data, ignore_index=True)
    logger.info(final_validate_data.shape[0] + ':rows after adding new data')

    common_train_validate = final_train_data.merge(final_validate_data, on=['productId'])
    logger.info(common_train_validate.shape + ': common_train_validate shape')

    final_validate_data = final_validate_data[(~final_validate_data.productId.isin(common_train_validate.productId))]

    final_validate_data.to_csv(const.get_raw_file_path() + output_file_name,  sep='\t', index=False, header=False)
    logger.info(final_validate_data.shape[0] + ':rows on final data')


def cleanse_final_output(final_train_file_name):

    # noinspection PyUnresolvedReferences
    df = pd.read_csv(const.get_raw_file_path() + final_train_file_name, sep='\t',
                     names=['productId', 'categoryCode', 'categoryCode1', 'cate1', 'categoryCode2', 'cate2',
                            'categoryCode3', 'cate3', 'categoryCode4', 'cate4', 'categoryCode5', 'cate5',
                            'categoryCode6', 'cate6', 'input_str'],
                     dtype=[('productId', 'str'), ('categoryCode', 'str'), ('categoryCode1', 'str'), ('cate1', 'str'),
                            ('categoryCode2', 'str'), ('cate2', 'str'), ('categoryCode3', 'str'), ('cate3', 'str'),
                            ('categoryCode4', 'str'), ('cate4', 'str'), ('categoryCode5', 'str'), ('cate5', 'str'),
                            ('categoryCode6', 'str'), ('cate6', 'str'), ('input_str', 'str')])

    not_good = df.query("categoryCode2=='69183' and input_str.str.contains('남성|남자')==True ")
    df = df[(~df.productId.isin(not_good.productId))]
    not_good = df.query("categoryCode2=='69512' and input_str.str.contains('여성|여자')==True ")
    df = df[(~df.productId.isin(not_good.productId))]
    not_good = df.query("categoryCode5=='79381' and input_str.str.contains('반팔|반소매')==True ")
    df = df[(~df.productId.isin(not_good.productId))]
    not_good = df.query("categoryCode5=='79382' and input_str.str.contains('긴팔|긴소매')==True ")
    df = df[(~df.productId.isin(not_good.productId))]
    not_good = df.query("categoryCode2=='93185' and productId=='0' and input_str.str.endswith('General')==True ")
    df = df[(~df.productId.isin(not_good.productId))]

    # remove event category temporarily
    df = df[(~df.categoryCode2.isin(['62477', '76062', '76757', '1024', '76446', '84241', '7014', '85115', '84248',
                                     '52225', '76758', '83908', '84214', '84271', '76152', '86182', '83329', '7287',
                                     '76125', '1022', '1026', '1027', '74296', '1023', '10581', '82711', '76294',
                                     '84270', '85971', '1000000', '76169', '86160', '76161', '72128', '29471',
                                     '26342', '83172', '8311']))]

    df.to_csv(const.get_raw_file_path() + final_train_file_name, sep='\t', index=False, header=False)


def build_augmented_data_cate_in_product_name(cate_file_name):

    engine = create_engine(DB_SANDBOX_URL)

    q = ''
    with open(cate_file_name, 'r') as input_f:
        idx = 0

        for line in input_f:

            if idx % 200 == 0:
                q = """ 
                    INSERT INTO SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP14_INT_TRAINDATA_AUGMENT_CATE_IN_PRODUCT
                    SELECT
                    mos.catecode,
                    mos.productid
                    FROM (
                    SELECT 
                    mos.catecode,
                    mos.productid,
                    mos.input_string
                    FROM SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP09_INT_PRODUCT_DATA mos
                    WHERE (
                """

            try:
                (cate1,
                 catecode,
                 keyword_rules) = line.split("\t")
            except Exception as e:
                print(e)
                continue

            if idx % 200 == 0:
                cate_query = " ( catecode = " + catecode + " and ( "
            else:
                cate_query = " OR ( catecode = " + catecode + " and ( "
            k = ""

            keyword_rules_formated = keyword_rules.lower().split('ㅁ')
            longest_keyword_rule = max(keyword_rules_formated, key=len)
            rules = [[str(x) for x in ss.split(',')] for ss in re.split('\(|\)', longest_keyword_rule.strip())]

            single_rule_list = []
            intersect_rule_list = []
            union_rule_list = []

            for rule in rules:
                for keyword in rule:
                    if "n" in keyword:
                        intersect_rule_keywords = [x.strip() for x in keyword.split('n')]
                        intersect_rule_list.append(intersect_rule_keywords)
                    elif "u" in keyword:
                        union_rule_keywords = [x.strip() for x in keyword.split('u')]
                        union_rule_list.append(union_rule_keywords)
                    else:
                        if keyword:
                            single_rule_keywords = [x.strip() for x in keyword.split(' ')]
                            single_rule_list.append(single_rule_keywords)

            rule_list = single_rule_list + intersect_rule_list + union_rule_list

            for rule in rule_list:
                if len(rule) == 1:
                    if k == "":
                        k = k + " lower(input_string) like '%" + str(rule[0]).strip() + "%' "
                    else:
                        k = k + " OR lower(input_string) like '%" + str(rule[0]).strip() + "%' "
                else:
                    if k == "":
                        rr = 0
                        for r in rule:
                            if rr == 0:
                                k = k + " ( lower(input_string) like '%" + str(r).strip() + "%' "
                            else:
                                k = k + " and lower(input_string) like '%" + str(r).strip() + "%' "
                            rr = rr + 1
                        k = k + ") "
                    else:
                        rr = 0
                        for r in rule:
                            k = k + " OR "
                            if rr == 0:
                                k = k + " (lower(input_string) like '%" + str(r).strip() + "%' "
                            else:
                                k = k + " and lower(input_string) like '%" + str(r).strip() + "%' "
                            rr = rr + 1
                        k = k + ") "

            if k == "":
                k = " 1 = 2"

            cate_query = cate_query + k + " )) "

            q = q + cate_query + '\n'

            if idx % 200 == 199:
                q = q + """) 
                    ) mos
                    LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln on (cln.PRODUCTID = mos.PRODUCTID)
                    WHERE cln.PRODUCTID IS NULL"""

                logger.info(idx)
                engine.execute(text(q))

            idx = idx + 1

    if q != "":
        q = q + """) 
            ) mos
            LEFT OUTER JOIN SB_PRODUCTCATALOG.CATALOG_SCIENCE_CATEGORY_PREDICTION_STEP12_INT_TRAINDATA_CLEAN cln on (cln.PRODUCTID = mos.PRODUCTID)
            WHERE cln.PRODUCTID IS NULL"""

        logger.info(idx)
        engine.execute(text(q))


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        arg = "all"
    else:
        arg = sys.argv[1]

    if arg == "refresh_all_source":

        is_backup = True

        step1_int_category_mapping(is_backup)
        step2_clean_product_list_audit(is_backup)
        # step2_clean_product_list_product_clean(is_backup)
        step2_clean_product_list_r21(is_backup)
        step2_clean_product_list_jikgu(is_backup)
        step2_clean_product_list_book_rocket(is_backup)
        step2_clean_product_list_browsing(is_backup)
        step2_clean_product_list_merge(is_backup)
        step3_product_list(is_backup)
        step4_item_list(is_backup)
        step5_product_name(is_backup)
        step6_brand_manufacture(is_backup)
        step7_item_attribute(is_backup)
        step8_item_name(is_backup)
        step9_product_data(is_backup)
        step12_train_and_validate_data(is_backup)

        step14_feedback_cate_name(is_backup)
        step14_feedback_manual(is_backup)
        step14_feedback_catalog(is_backup)
        step14_feedback_seller(is_backup)

        step95_cate_no_map(is_backup)
        step97_cate_remap(is_backup)

        step98_train_data_create(is_backup)
        step99_validate_data_create(is_backup)
        get_vitamin_train_data()
        get_vitamin_validate_data()

    elif arg == "add_new_train":

        output_train_file_name = add_new_train(
            current_file_name=sys.argv[2],
            input_file_name=sys.argv[3],
            output_file_name=sys.argv[4])

        add_new_validate(
            current_file_name=sys.argv[2],
            input_file_name=sys.argv[3],
            output_file_name=sys.argv[4],
            final_train_file_name=output_train_file_name)

        cleanse_final_output(output_train_file_name)

    elif arg == "refresh_train_validate":

        is_backup = False

        step14_feedback_cate_name(is_backup)
        step14_feedback_manual(is_backup)
        step14_feedback_catalog(is_backup)
        step14_feedback_seller(is_backup)

        step95_cate_no_map(is_backup)
        step97_cate_remap(is_backup)

        step98_train_data_create(is_backup)
        step99_validate_data_create(is_backup)
        get_vitamin_train_data()
        get_vitamin_validate_data()

    elif arg == "get_vitamin_data":
        get_vitamin_train_data()
        get_vitamin_validate_data()

    elif arg == "get_augment_temp":
        get_augment_temp()

    elif arg == "get_train_product_id":
        get_category_id()
        get_train_product_id()
        get_train_product_id_with_catecode()

    elif arg == "get_validate_product_id":
        get_category_id()
        get_validate_product_id()
        get_validate_product_id_with_catecode()

    elif arg == "get_jikgu_product_id":
        get_jikgu_product_id()

    elif arg == "get_cleansed_product_id":
        get_cleansed_product_id()

    elif arg == "download_merge_train_data":
        merge_cds(const.get_raw_temp_file_path() + "cds/train/", const.get_raw_train_cds_temp_file_name())
        cleanse_file(const.get_raw_train_cds_temp_file_name(), 6)
        create_cds_file(const.get_raw_train_cds_temp_file_name(),
                        const.get_raw_train_product_id_with_catecode_file_name(),
                        const.get_raw_train_cds_file_name())
        merge_cds_output("train", const.get_raw_train_file_name())
        cleanse_file(const.get_raw_train_file_name(), 16)
        # delete_wrong_label_row(const.get_raw_train_file_name())

    elif arg == "merge_train_data":
        merge_cds_output("train", const.get_raw_train_file_name())
        cleanse_file(const.get_raw_train_file_name(), 16)
        # delete_wrong_label_row(const.get_raw_train_file_name())

    elif arg == "download_merge_validate_data":
        merge_cds(const.get_raw_temp_file_path() + "cds/validate/", const.get_raw_validate_cds_temp_file_name())
        cleanse_file(const.get_raw_validate_cds_temp_file_name(), 6)
        create_cds_file(const.get_raw_validate_cds_temp_file_name(),
                        const.get_raw_validate_product_id_with_catecode_file_name(),
                        const.get_raw_validate_cds_file_name())
        merge_cds_output("validate", const.get_raw_validate_file_name())
        cleanse_file(const.get_raw_validate_file_name(), 16)
        # delete_wrong_label_row(const.get_raw_validate_file_name())

    elif arg == "merge_validate_data":
        merge_cds_output("validate", const.get_raw_validate_file_name())
        cleanse_file(const.get_raw_validate_file_name(), 16)
        # delete_wrong_label_row(const.get_raw_validate_file_name())

    elif arg == "merge_audit_data":
        merge_cds(const.get_audit_temp_file_path(), const.get_audit_temp_file_name())
        cleanse_file(const.get_audit_temp_file_name(), 5)
        create_audit_file(const.get_audit_temp_file_name(), sys.argv[2])

    elif arg == "check_file":
        check_file(sys.argv[2], sys.argv[3])

    elif arg == "cleanse_file":
        cleanse_file(sys.argv[2], sys.argv[3])

    elif arg == 'cleanse_augmentation':
        cleanse_augmentation_file(sys.argv[2], sys.argv[3])

    elif arg == 'get_garbage_product_names':
        get_garbage_product_names(sys.argv[2])

    else:
        sys.exit("Should run this script as: Python " + __file__ +
                 " [all/refresh_train_data/merge_train_data/get_train_product_id]")
