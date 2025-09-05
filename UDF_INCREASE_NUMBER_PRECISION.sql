CREATE OR REPLACE FUNCTION UDF_INCREASE_NUMBER_PRECISION(event_schema ARRAY)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'increase_precision'
AS $$
def increase_precision(event_schema: list) -> list:

    def max_precision(schema_pair: dict):
        num_str = schema_pair['TYPE']
        p, s = num_str.strip("NUMBER()").split(", ")
        schema_pair['TYPE'] = f"NUMBER(38, {s})"
        return schema_pair

    return [
        max_precision(sch) 
        if sch['TYPE'].startswith('NUMBER') 
        else sch 
        for sch in event_schema
    ]
$$;

select UDF_INCREASE_NUMBER_PRECISION([
  PARSE_JSON('{"COLUMN_NAME": "c1", "TYPE": "NUMBER(8, 6)"}')
, PARSE_JSON('{"COLUMN_NAME": "c2", "TYPE": "TEXT"}')
])
