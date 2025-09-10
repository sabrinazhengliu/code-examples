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
                if isinstance(value, float):    # without this step, float values will be inferred as TEXT
                    value = Decimal(str(value))
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


CREATE OR REPLACE FUNCTION FLATTEN_JSON_UDF(OBJ VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
function flatten(obj, prefix = '') {
    let result = {};

    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            const newKey = prefix ? `${prefix}.${key}` : key;
            const value = obj[key];

            if (typeof value === 'object' && value !== null) {
                if (Array.isArray(value)) {
                    // Handle arrays by flattening each element
                    value.forEach((item, index) => {
                        if (typeof item === 'object' && item !== null) {
                            Object.assign(result, flatten(item, `${newKey}[${index}]`));
                        } else {
                            result[`${newKey}[${index}]`] = item;
                        }
                    });
                } else {
                    // Recursively flatten nested objects
                    Object.assign(result, flatten(value, newKey));
                }
            } else {
                // Add primitive values directly
                result[newKey] = value;
            }
        }
    }
    return result;
}

return flatten(OBJ);
$$;
