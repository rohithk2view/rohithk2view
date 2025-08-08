CREATE SCHEMA IF NOT EXISTS ${@schema};

--DROP TABLE IF EXISTS ${@schema}.ref_d2d_field_summary;

CREATE TABLE IF NOT EXISTS ${@schema}.ref_d2d_field_summary (
  EXECUTION_ID character varying(200), 
  IID character varying(200), 
  SOURCE_TABLE_NAME character varying(200), 
  TARGET_TABLE_NAME character varying(200), 
  CUSTOMIZED_KEY character varying(1000), 
  COLUMN_NAME character varying(200), 
  MATCH_RESULT character varying(50), 
  TARGET_VALUE_SECURED character varying(50), 
  SOURCE_COLUMN_VALUE character varying(1000), 
  TARGET_COLUMN_VALUE character varying(1000), 
  SOURCE_COLUMN_VALUE_TRANS character varying(1000), 
  TARGET_COLUMN_VALUE_TRANS character varying(1000), 
  BW_TRANSFORMATION_FLOW boolean, 
  NULL_COMPARE boolean, 
  LOOKUP boolean, 
  DEFAULT_VALUE boolean, 
  PRIMARY KEY(
    EXECUTION_ID, IID, SOURCE_TABLE_NAME, 
    TARGET_TABLE_NAME, CUSTOMIZED_KEY, 
    COLUMN_NAME
  )
);

CREATE INDEX IF NOT EXISTS IID_FIELD_SUMMARY_IDX ON ${@schema}.ref_d2d_field_summary (iid);

--DROP TABLE IF EXISTS ${@schema}.ref_d2d_entity_summary;

CREATE TABLE IF NOT EXISTS ${@schema}.ref_d2d_entity_summary (
  EXECUTION_ID character varying(200), 
  IID character varying(200), 
  NUMBER_OF_FIELDS_MATCH integer, 
  NUMBER_OF_FIELDS_MISMATCH integer, 
  NUMBER_OF_FIELDS_ONLY_IN_SOURCE integer, 
  NUMBER_OF_FIELDS_ONLY_IN_TARGET integer, 
  NUMBER_OF_FIELDS_UNSECURED_IN_TARGET integer, 
  NUMBER_OF_RECORDS_MATCH integer, 
  NUMBER_OF_RECORDS_MISMATCH integer, 
  NUMBER_OF_RECORDS_UNSECURED_IN_TARGET integer, 
  MATCH_RESULT character varying(200), 
UPDATE_TIME timestamp, 
  PRIMARY KEY(EXECUTION_ID, IID)
);

CREATE INDEX IF NOT EXISTS IID_ENTITY_SUMMARY_IDX ON ${@schema}.ref_d2d_entity_summary (iid, update_time);

--DROP TABLE IF EXISTS ${@schema}.ref_d2d_record_summary;

CREATE TABLE IF NOT EXISTS ${@schema}.ref_d2d_record_summary (
  EXECUTION_ID character varying(200), 
  IID character varying(200), 
  SOURCE_TABLE_NAME character varying(200), 
  TARGET_TABLE_NAME character varying(200), 
  CUSTOMIZED_KEY character varying(1000), 
  NUMBER_OF_FIELDS_MATCH integer, 
  NUMBER_OF_FIELDS_MISMATCH integer, 
  NUMBER_OF_FIELDS_ONLY_IN_SOURCE integer, 
  NUMBER_OF_FIELDS_ONLY_IN_TARGET integer, 
  NUMBER_OF_FIELDS_UNSECURED_IN_TARGET integer, 
  MATCH_RESULT character varying(200), 
  PRIMARY KEY(
    EXECUTION_ID, IID, SOURCE_TABLE_NAME, 
    TARGET_TABLE_NAME, CUSTOMIZED_KEY
  )
);

CREATE INDEX IF NOT EXISTS IID_RECORD_SUMMARY_IDX ON ${@schema}.ref_d2d_record_summary (iid);

--DROP TABLE IF EXISTS ${@schema}.ref_d2d_table_summary;

CREATE TABLE IF NOT EXISTS ${@schema}.ref_d2d_table_summary (
  EXECUTION_ID text, 
  IID text, 
  SOURCE_TABLE_NAME text, 
  TARGET_TABLE_NAME text, 
  NUMBER_OF_RECORDS_MATCH integer, 
  NUMBER_OF_RECORDS_MISMATCH integer, 
  NUMBER_OF_RECORDS_ONLY_IN_SOURCE integer, 
  NUMBER_OF_RECORDS_ONLY_IN_TARGET integer, 
  NUMBER_OF_RECORDS_UNSECURED_IN_TARGET integer, 
  MATCH_RESULT text, 
  PRIMARY KEY(
    EXECUTION_ID, IID, SOURCE_TABLE_NAME, 
    TARGET_TABLE_NAME
  )
);

CREATE INDEX IF NOT EXISTS IID_TABLE_SUMMARY_IDX ON ${@schema}.ref_d2d_table_summary (iid);

--DROP VIEW IF EXISTS ${@schema}.ref_d2d_execution_summary;

CREATE 
OR REPLACE VIEW ${@schema}.ref_d2d_execution_summary AS 
SELECT 
  es.EXECUTION_ID, 
  count(es.iid) AS TOTAL_NUMBER_OF_ENTITIES, 
  (
    SELECT 
      count(*) AS TOTAL_NUMBER_OF_MATCHES 
    FROM 
      ${@schema}.ref_d2d_entity_summary m 
    WHERE 
      m.EXECUTION_ID :: text = es.EXECUTION_ID :: text 
      AND m.MATCH_RESULT :: text = 'Match' :: text
  ) AS TOTAL_NUMBER_OF_MATCHES, 
  (
    SELECT 
      count(*) AS TOTAL_NUMBER_OF_MATCHES 
    FROM 
      ${@schema}.ref_d2d_entity_summary mm 
    WHERE 
      mm.EXECUTION_ID :: text = es.EXECUTION_ID :: text 
      AND mm.MATCH_RESULT :: text = 'Mismatch' :: text
  ) AS TOTAL_NUMBER_OF_MISMATCHES, 
  (
    (
      SELECT 
        count(*) AS TOTAL_NUMBER_OF_MATCHES 
      FROM 
        ${@schema}.ref_d2d_entity_summary m 
      WHERE 
        m.EXECUTION_ID :: text = es.EXECUTION_ID :: text 
        AND m.MATCH_RESULT :: text = 'Match' :: text
    )
  ):: numeric / (
    count(es.IID):: numeric + 0.0000001
  ) * 100 :: numeric AS MATCH_RATE 
FROM 
  ${@schema}.ref_d2d_entity_summary es 
GROUP BY 
  es.EXECUTION_ID;


--DROP VIEW IF EXISTS ${@schema}.ref_d2d_execution_tables_summary;

CREATE 
OR REPLACE VIEW ${@schema}.ref_d2d_execution_tables_summary AS 
SELECT 
  ${@schema}.ref_d2d_table_summary.EXECUTION_ID, 
  ${@schema}.ref_d2d_table_summary.SOURCE_TABLE_NAME, 
  sum(
    ${@schema}.ref_d2d_table_summary.NUMBER_OF_RECORDS_MATCH
  ) AS RECORDS_MATCH, 
  sum(
    ${@schema}.ref_d2d_table_summary.NUMBER_OF_RECORDS_MISMATCH
  ) AS RECORDS_MISMATCH, 
  sum(
    ${@schema}.ref_d2d_table_summary.NUMBER_OF_RECORDS_ONLY_IN_SOURCE
  ) AS RECORDS_IN_SOURCE, 
  sum(
    ${@schema}.ref_d2d_table_summary.NUMBER_OF_RECORDS_ONLY_IN_TARGET
  ) AS RECORDS_IN_TARGET, 
  max(${@schema}.ref_d2d_table_summary.MATCH_RESULT) AS MATCH_RESULT 
FROM 
  ${@schema}.ref_d2d_table_summary 
GROUP BY 
  ${@schema}.ref_d2d_table_summary.EXECUTION_ID, 
  ${@schema}.ref_d2d_table_summary.SOURCE_TABLE_NAME;