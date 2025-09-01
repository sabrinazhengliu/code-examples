CREATE OR REPLACE FUNCTION UDF_FLATTEN_JSON(json_data VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'flatten_json'
AS
$$
import json

def flatten_json(obj, prefix=''):
    flat_dict = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, (dict, list)):
                flat_dict.update(flatten_json(value, new_key))
            else:
                flat_dict[new_key] = value
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            new_key = f"{prefix}_{i}" if prefix else str(i)
            if isinstance(item, (dict, list)):
                flat_dict.update(flatten_json(item, new_key))
            else:
                flat_dict[new_key] = item
    return flat_dict

def flatten_json(json_data):
    try:
        data = json_data
        flattened_data = flatten_json_recursive(data)
        return flattened_data
    except Exception as e:
        return {"error": str(e)}

def flatten_json_recursive(obj, prefix=''):
    flat_dict = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict):
                flat_dict.update(flatten_json_recursive(value, new_key))
            elif isinstance(value, list):
                # Handle lists by flattening each element with an index
                for i, item in enumerate(value):
                    list_key = f"{new_key}_{i}"
                    if isinstance(item, (dict, list)):
                        flat_dict.update(flatten_json_recursive(item, list_key))
                    else:
                        flat_dict[list_key] = item
            else:
                flat_dict[new_key] = value
    elif isinstance(obj, list):
        # If the top level is a list, treat each element as a separate item to flatten
        for i, item in enumerate(obj):
            list_key = f"{prefix}_{i}" if prefix else str(i)
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
