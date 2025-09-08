-- step 1: locate your source table containing the variant column

CREATE OR REPLACE TEMP TABLE T1 (VALUE VARIANT);

INSERT INTO T1
SELECT PARSE_JSON('
{
    "id": 1,
    "name": "Alice Smith",
    "email": "alice.smith@example.com",
    "age": 30,
    "isActive": true,
    "address": {
      "street": "123 Main St",
      "city": "Anytown",
      "zipCode": "12345"
    },
    "hobbies": ["reading", "hiking", "cooking"]
  }
')
;
SELECT * FROM T1;

-- step 2: create a flattened view

CREATE OR REPLACE TEMP VIEW V1
AS
SELECT
  VALUE:"name"::varchar as "name"
, VALUE:"id_number"::INTEGER as "id_number"
, VALUE:"email"::varchar as "email"
, VALUE:"address"."city"::varchar as "address.city"
, VALUE:"address"."street"::varchar as "address.street"
, VALUE:"address"."zipCode"::varchar as "address.zipCode"
FROM T1
;
SELECT * FROM V1;

SELECT EXTRACT_SEMANTIC_CATEGORIES('V1');

/*
{
  "address.city": {
    "alternates": []
  },
  "address.street": {
    "alternates": [],
    "recommendation": {
      "confidence": "HIGH",
      "coverage": 1,
      "details": [
        {
          "coverage": 1,
          "semantic_category": "US_STREET_ADDRESS"
        },
        {
          "coverage": 1,
          "semantic_category": "CA_STREET_ADDRESS"
        }
      ],
      "privacy_category": "IDENTIFIER",
      "semantic_category": "STREET_ADDRESS"
    },
    "valid_value_ratio": 1
  },
  "address.zipCode": {
    "alternates": [],
    "recommendation": {
      "confidence": "HIGH",
      "coverage": 1,
      "details": [
        {
          "coverage": 1,
          "semantic_category": "US_POSTAL_CODE"
        }
      ],
      "privacy_category": "QUASI_IDENTIFIER",
      "semantic_category": "POSTAL_CODE"
    },
    "valid_value_ratio": 1
  },
  "email": {
    "alternates": [],
    "recommendation": {
      "confidence": "HIGH",
      "coverage": 1,
      "details": [],
      "privacy_category": "IDENTIFIER",
      "semantic_category": "EMAIL"
    },
    "valid_value_ratio": 1
  },
  "id_number": {
    "alternates": []
  },
  "name": {
    "alternates": [],
    "recommendation": {
      "confidence": "HIGH",
      "coverage": 1,
      "details": [],
      "privacy_category": "IDENTIFIER",
      "semantic_category": "NAME"
    },
    "valid_value_ratio": 1
  }
}
*/

-- step 3: create a classification results record table
CREATE OR REPLACE TEMP TABLE DATA_CLASSIFICATION_RESULTS (
  OBJECT_NAME VARCHAR
, OBJECT_TYPE VARIANT
, UPDATE_TIMESTAMP TIMESTAMP_LTZ
);

-- step 4: load data classification results
SET object_name = 'V1';
INSERT INTO DATA_CLASSIFICATION_RESULTS
SELECT
  $object_name
, EXTRACT_SEMANTIC_CATEGORIES($OBJECT_NAME, 1000)
, CURRENT_TIMESTAMP
;
SELECT * FROM DATA_CLASSIFICATION_RESULTS;

-- step 5: create a record table for JSON paths that requires tagging
CREATE OR REPLACE TEMP TABLE SENSITIVE_DATA_COLUMNS
AS
WITH results AS (
    select EXTRACT_SEMANTIC_CATEGORIES('v1') AS json_data
)
SELECT
 $object_name as object_name
, CURRENT_TIMESTAMP as update_timestamp
, KEY AS field_name
, CAST(GET_PATH (value, 'alternates') AS ARRAY) AS alternates
, CAST(GET_PATH (value, 'valid_value_ratio') AS FLOAT) AS valid_value_ratio
, CAST(GET_PATH (value, 'recommendation.confidence') AS TEXT) AS confidence
, CAST(GET_PATH (value, 'recommendation.coverage') AS FLOAT) AS coverage
, CAST(GET_PATH (value, 'recommendation.privacy_category') AS TEXT) AS privacy_category
, CAST(GET_PATH (value, 'recommendation.semantic_category') AS TEXT) AS semantic_category
, GET_PATH (value, 'recommendation.details') AS details
FROM  results, LATERAL FLATTEN (input => json_data) AS f (SEQ, KEY, PATH, INDEX, VALUE, THIS)
WHERE NOT GET_PATH (value, 'recommendation') IS NULL;

SELECT * FROM SENSITIVE_DATA_COLUMNS;

SELECT ARRAY_AGG(FIELD_NAME)
FROM SENSITIVE_DATA_COLUMNS
WHERE OBJECT_NAME = $object_name
AND PRIVACY_CATEGORY = 'IDENTIFIER'
GROUP BY OBJECT_NAME, UPDATE_TIMESTAMP
QUALIFY ROW_NUMBER() OVER (PARTITION BY OBJECT_NAME ORDER BY UPDATE_TIMESTAMP DESC) = 1;

-- step 6: create a Python UDF to mask out variant column by keys
CREATE OR REPLACE FUNCTION udf_mask_variant_keys(
    variant_data VARIANT,
    keys_to_mask ARRAY
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9' -- Or your desired Python version
HANDLER = 'mask_data'
AS
$$
def mask_data(variant_data: dict, keys_to_mask: list):
    masked_data = variant_data.copy()
    for json_path in keys_to_mask:
        parts = json_path.split('.')
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                masked_data[part] = '***'
    return masked_data
$$;

SELECT VALUE, udf_mask_variant_keys(VALUE, ['address.street', 'email', 'name'])
FROM T1
;

/* value returned:
{
  "address": {
    "city": "Anytown",
    "street": "123 Main St",
    "zipCode": "12345"
  },
  "age": 30,
  "email": "***",
  "hobbies": [
    "reading",
    "hiking",
    "cooking"
  ],
  "id": 1,
  "isActive": true,
  "name": "***",
  "street": "***"
}
*/


-- step 7: create masking policy

CREATE MASKING POLICY mask_t1 AS (val VARIANT) RETURNS VARIANT ->
  CASE
    WHEN CURRENT_ROLE() NOT IN ('ADMIN') THEN udf_mask_variant_keys(val, (
      SELECT ARRAY_AGG(FIELD_NAME)
      FROM SENSITIVE_DATA_COLUMNS
      WHERE OBJECT_NAME = $object_name
      AND PRIVACY_CATEGORY = 'IDENTIFIER'
      GROUP BY OBJECT_NAME, UPDATE_TIMESTAMP
      QUALIFY ROW_NUMBER() OVER (PARTITION BY OBJECT_NAME ORDER BY UPDATE_TIMESTAMP DESC) = 1
    ))
    ELSE val
  END;

ALTER TABLE T1 MODIFY COLUMN VALUE SET MASKING POLICY mask_t1;
SELECT * FROM T1;

