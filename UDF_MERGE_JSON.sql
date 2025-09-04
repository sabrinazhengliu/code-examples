CREATE OR REPLACE FUNCTION UDF_MERGE_JSON_VALUES(json_array ARRAY)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'merge_json_list'
AS
$$
def merge_json_list(json_list: list) -> dict:
    """
    Merges a list of JSON objects into a single JSON object.
    If keys overlap, the last JSON object in the list takes precedence.
    """
    merged_json = {}

    for current_json in json_list:
        merged_json.update(current_json)

    return merged_json
$$
;

CREATE OR REPLACE FUNCTION UDF_FEATURE_PIPELINE_MERGE_JSON_VALUES(objects ARRAY)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
 return Object.assign({}, ...OBJECTS);
$$;


set partner_code = 'HAULMER_CL';
create or replace temp table feature_store
as
select 
 $partner_code || TO_CHAR(CURRENT_TIMESTAMP, '_YYYYMMDD_HHMI') AS SCORE_TAG
, '1234' as merchant_id
, UDF_MERGE_JSON_VALUES([{},
  PARSE_JSON('{"a": 1}')             -- integer
, PARSE_JSON('{"b": 2.34}')          -- float
, PARSE_JSON('{"c": "2025-09-02"}')  -- date
, PARSE_JSON('{"d": "xyz"}')         -- text
]) AS FEATURES_ALL;

select * from feature_store;
