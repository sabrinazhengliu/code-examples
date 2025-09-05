-- https://docs.snowflake.com/en/sql-reference/functions/extract_semantic_categories
https://docs.snowflake.com/en/sql-reference/stored-procedures/associate_semantic_category_tags

EXTRACT_SEMANTIC_CATEGORIES( '<object_name>' [ , <max_rows_to_scan> ] );

-- automatic apply
CALL ASSOCIATE_SEMANTIC_CATEGORY_TAGS(
  $object_name
  EXTRACT_SEMANTIC_CATEGORIES($object_name)
);


-- manually update then apply

CREATE TABLE data_classficiation_results(
  UPDATE_TIMESTAMP     TIMESTAMP_LTZ
, OBJECT_NAME          VARCHAR
, SEMANTIC_CATEGORIES  VARIANT
  )

set object_name = '<your_table_name>';

INSERT INTO data_classification_results
SELECT
  CURRENT_TIMESTAMP
, $object_name
, EXTRACT_SEMANTIC_CATEGORIES($object_name,  1000)
;
USE ROLE data_engineer;

UPDATE data_classification_results 
SET semantic_categories =
    OBJECT_INSERT(
      V,'LNAME',OBJECT_INSERT(
        OBJECT_INSERT(V:LNAME,'semantic_category','NAME',TRUE),
        'privacy_category','IDENTIFIER',TRUE),
        TRUE
    );

CALL ASSOCIATE_SEMANTIC_CATEGORY_TAGS(
  $object_name
  (SELECT semantic_categories FROM data_classification_results WHERE OBJECT_NAME = $object_name)
);


