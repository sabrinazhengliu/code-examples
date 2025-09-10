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


import re
def merge_json_keys(json_keys: list) -> list:
    return_list = set([
        re.sub(r"\[\d+\]", "[0]", key)
        if "[" in key
        else key
        for key in json_keys
    ])
    root_list = []
    for key in return_list:
        if "[0]" in key:
            root = key.split("[0]", 1)[0]
            root_list.append(root)
    for key in set(root_list):
        return_list.discard(key)
    return sorted(list(return_list))

keys = [
    "Level1",
    "Level1[0].A",
    "Level1[0].B",
    "Level1[0].C",
    "Level1[1].A",
    "Level1[1].B",
    "Level1[1].C",
    "Level1[2].A",
    "Level1[2].B",
    "Level1[2].C",
    "Other",
]

merge_json_keys(keys)

def convert_float_to_decimal(json_data: dict) -> str:
    keep_keys = merge_json_keys(json_data.keys())

    def mapper(key: str) -> str:
        data = f"EVENT_PAYLOAD:{key}"
        stmt = f"'{key}', CASE WHEN IS_REAL({data}) THEN TO_DECIMAL({data}, 38, 9) ELSE {data} END"
        return stmt

    query = "OBJECT_CONSTRUCT(\n"
    query += "\n,".join([mapper(key) for key in keep_keys])
    query += "\n) AS SAMPLE_PAYLOAD"
    return query

d ={
    "L1": 1,
    "L1[0].A": 1,
    "L1[0].B": 1,
    "L1[1].A": 1,
    "L1[1].B": 1,
    "L1[2].A": 1,
    "L1[2].B": 1,
    "OTHER": 0}

convert_float_to_decimal(d)
