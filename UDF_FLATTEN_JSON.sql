CREATE OR REPLACE FUNCTION UDF_FLATTEN_JSON(json_data VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'flatten_json'
AS
$$
from decimal import Decimal

def flatten_json(json_data):
    try:
        flattened_data = flatten_json_recursive(json_data)
        return flattened_data
    except Exception as e:
        return {"error": str(e)}

def flatten_json_recursive(obj, prefix=''):
    flat_dict = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flat_dict.update(flatten_json_recursive(value, new_key))
            elif isinstance(value, list):
                # Handle lists by flattening each element with an index
                for i, item in enumerate(value):
                    list_key = f"{new_key}[{i}]"
                    if isinstance(item, (dict, list)):
                        flat_dict.update(flatten_json_recursive(item, list_key))
                    else:
                        flat_dict[list_key] = item
            else:
                flat_dict[new_key] = value
    elif isinstance(obj, list):
        # If the top level is a list, treat each element as a separate item to flatten
        for i, item in enumerate(obj):
            list_key = f"{prefix}[{i}]" if prefix else str(i)
            if isinstance(item, (dict, list)):
                flat_dict.update(flatten_json_recursive(item, list_key))
            else:
                flat_dict[list_key] = item
    return flat_dict
$$;

SELECT UDF_FLATTEN_JSON(PARSE_JSON(
  '{
    "name": "Alice",
    "address": {
      "street": "Main St",
      "city": "Anytown"
    },
    "phones": [
      {
        "type": "home",
        "number": "123-456-7890"
      },
      {
        "type": "work", 
        "number": "987-654-3210"
      }
    ]
  }'
)) AS flattened_data;

-- important! Python JSONDecoder doesn't support Decimal data type
-- write a SQL UDF to covnert all floats into decimals

CREATE OR REPLACE FUNCTION UDF_CONVERT_JSON_FLOATS_TO_DECIMAL(json_input VARIANT)
RETURNS VARIANT
AS
$$
    SELECT OBJECT_AGG(
        key::STRING,
        CASE
            WHEN IS_REAL(value) THEN TRY_TO_DECIMAL(value::STRING, 38, 9)
            ELSE value
        END
    )::VARIANT
    FROM TABLE(FLATTEN(INPUT => json_input))
$$;

SELECT
  PARSE_JSON('{"float": 1.234,}') AS json_data
, UDF_FLATTEN_JSON(json_data) as flattened_json
, UDF_CONVERT_JSON_FLOATS_TO_DECIMAL(flattened_json) AS decimal_json
;
