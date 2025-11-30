CREATE OR REPLACE FUNCTION UDF_EXTRACT_ARRAY_JSON_VALUES(
    input_list ARRAY, 
    target_key STRING
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'extract_handler'
AS
$$
def extract_handler(input_list, target_key):

    if input_list is None:
        return None
        
    extracted_data = []    
    for item in input_list:
        if isinstance(item, dict):
            extracted_data.append(item.get(target_key))
        else:
            extracted_data.append(None)
            
    return extracted_data
$$;
