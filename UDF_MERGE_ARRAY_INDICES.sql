CREATE OR REPLACE FUNCTION UDF_MERGE_ARRAY_INDICES(event_schema ARRAY)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'merge_array_indices'
AS $$
import re
def merge_array_indices(event_schema: list) -> list:
    def replace_idx(schema_pair: dict):
        schema_pair['COLUMN_NAME'] = re.sub(r"\[\d+\]", "[ARRAY]", schema_pair['COLUMN_NAME'])
        return schema_pair
    return_list = [
        replace_idx(sch)
        if "[" in sch['COLUMN_NAME']
        else sch
        for sch in event_schema
    ]
    return [dict(t) for t in {tuple(d.items()) for d in return_list}]
$$
;

select UDF_MERGE_ARRAY_INDICES([
  PARSE_JSON('{"COLUMN_NAME": "c1[0].key", "TYPE": "TEXT"}')
, PARSE_JSON('{"COLUMN_NAME": "c1[1].key", "TYPE": "TEXT"}')
])
;

# result: [
  {
    "COLUMN_NAME": "c1[ARRAY].key",
    "TYPE": "TEXT"
  }
]
