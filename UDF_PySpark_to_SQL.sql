CREATE OR REPLACE FUNCTION UDF_PYSPARK_TO_SQL(INPUT_STR VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('sqlglot', 'snowflake-snowpark-python')
HANDLER = 'pyspark_to_snowflake_ddl'
AS 
$$    
import re
import ast
import sys
import sqlglot
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType

def extract_sql_from_string(pyspark_str: str) -> str:
    """
    Attempt to extract SQL from a spark.sql("...") string using Regex.
    Returns None if the pattern doesn't match.
    """
    pattern = r"spark\.sql\s*\(\s*(?:\"{3}|'{3}|'|\")(.*?)(?:\"{3}|'{3}|'|\")\s*\)"
    match = re.search(pattern, pyspark_str, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def parse_pyspark_chain(pyspark_code: str) -> str:
    """
    Heuristic parser to convert PySpark DataFrame chains into a basic SQL string.
    Supported methods: .table(), .select(), .filter(), .where()
    
    Input: df.table("my_table").select("col1").filter("col1 > 5")
    Output: SELECT col1 FROM my_table WHERE col1 > 5
    """
    try:
        tree = ast.parse(pyspark_code.strip())
        
        # We assume the last statement is the expression we want to convert
        if not tree.body:
            return ""
        
        node = tree.body[-1]
        if isinstance(node, ast.Expr):
            node = node.value
            
        # Unwind the AST method calls (they are nested inside-out)
        # e.g. filter(select(table(...)))
        chain = []
        curr = node
        
        while isinstance(curr, ast.Call) and isinstance(curr.func, ast.Attribute):
            method = curr.func.attr
            args = [ast.unparse(a).strip("'\"") for a in curr.args]
            chain.append((method, args))
            curr = curr.func.value
            
        # Check for the base source (e.g., spark.table("name") or just 'df')
        source_table = "UNKNOWN_TABLE"
        if isinstance(curr, ast.Call) and isinstance(curr.func, ast.Attribute) and curr.func.attr == 'table':
             if curr.args:
                 source_table = ast.unparse(curr.args[0]).strip("'\"")
        elif isinstance(curr, ast.Name):
            source_table = curr.id
            
        # Process the chain in reverse (from source -> transformations)
        selects = ["*"]
        filters = []
        
        for method, args in reversed(chain):
            if method == 'select':
                selects = args
            elif method in ('filter', 'where'):
                # AST unparse might leave quotes, raw SQL cleanup happens in sqlglot later
                filters.extend(args)
            elif method == 'distinct':
                selects[0] = "DISTINCT " + selects[0]
                
        # Construct the SQL
        query = f"SELECT {', '.join(selects)} FROM {source_table}"
        if filters:
            query += f" WHERE {' AND '.join(filters)}"
            
        return query

    except Exception:
        # If AST parsing fails or structure is too complex, return empty to trigger fallback
        return ""

def transpile_sql(sql_code: str) -> str:
    """
    Uses sqlglot to transpile Spark SQL to Snowflake SQL.
    - identify=True: Forces double-quoting of identifiers.
    """
    try:
        transpiled_list = sqlglot.transpile(
            sql_code, 
            read="spark", 
            write="snowflake", 
            identify=True,
            pretty=True
        )
        return transpiled_list[0] if transpiled_list else ""
    except Exception as e:
        return f"Error transpiling SQL: {str(e)}"

def pyspark_to_snowflake_ddl(pyspark_code: str) -> str:
    """
    Main UDF logic.
    1. Tries to find 'spark.sql(...)'.
    2. If not found, tries to parse DataFrame API chain.
    3. Transpiles resulting SQL to Snowflake dialect.
    4. Wraps in CREATE VIEW.
    """
    if not pyspark_code:
        return "-- Error: Empty Input"

    # Strategy 1: Look for explicit SQL wrapper
    raw_sql = extract_sql_from_string(pyspark_code)
    
    # Strategy 2: If no SQL wrapper, try to parse DataFrame syntax
    if not raw_sql:
        converted_sql = parse_pyspark_chain(pyspark_code)
        if converted_sql:
            raw_sql = converted_sql
        else:
            # Fallback: Treat input as raw SQL (user might have pasted just the query)
            raw_sql = pyspark_code

    # Transpile
    snowflake_sql = transpile_sql(raw_sql)
    
    if snowflake_sql.startswith("Error"):
        return f"-- {snowflake_sql}"

    ddl = f'CREATE OR REPLACE VIEW VW_NEW AS\n{snowflake_sql};'
    return ddl
$$;
