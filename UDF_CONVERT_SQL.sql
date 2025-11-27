CREATE OR REPLACE FUNCTION UDF_CONVERT_SQL(INPUT_STR STRING, PRESERVE_COLUMN_CASE BOOLEAN DEFAULT TRUE)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'format_sql'
AS
$$
import re

def format_sql(input_str, preserve_case):
    if not input_str:
        return ""

    # ---------------------------------------------------------
    # 1. PRE-PROCESSING & REPLACEMENTS
    # ---------------------------------------------------------
    # Handle the specific replacement requirements first
    # Replace < with (, > with ), and struct with OBJECT
    text = input_str.replace('<', '(').replace('>', ')')
    
    # Case-insensitive replace for 'struct'
    text = re.sub(r'(?i)\bstruct\b', 'OBJECT', text)

    # ---------------------------------------------------------
    # 2. CONFIGURATION (Keywords & Types)
    # ---------------------------------------------------------
    # A comprehensive list of standard SQL keywords to reserve and uppercase
    KEYWORDS = {
        "select", "from", "where", "group", "by", "order", "having", "limit",
        "create", "or", "replace", "table", "view", "function", "procedure",
        "with", "as", "and", "or", "not", "in", "is", "null", "true", "false",
        "join", "left", "right", "inner", "outer", "on", "case", "when", "then",
        "else", "end", "distinct", "union", "all", "cast", "try_cast", "external"
    }
    # Convert set to all upper for comparison
    KEYWORDS = {k.upper() for k in KEYWORDS}

    # Standard Snowflake/SQL Data Types
    DATA_TYPES = {
        "STRING", "TEXT", "VARCHAR", "CHAR", "NUMBER", "INT", "INTEGER", "BIGINT", 
        "FLOAT", "DOUBLE", "BOOLEAN", "BOOL", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY",
        "GEOMETRY", "BINARY"
    }

    # ---------------------------------------------------------
    # 3. TOKENIZER
    # ---------------------------------------------------------
    # Regex breakdown:
    # 1. String literals (single or double quoted) -> don't format inside these
    # 2. Alphanumeric words (identifiers/keywords)
    # 3. Symbols we care about (parens, commas, colons)
    token_pattern = r"('[^']*'|\"[^\"]*\"|[a-zA-Z0-9_]+|[(),:])"
    
    # Split the string, filter out empty strings/pure whitespace
    raw_tokens = [t for t in re.split(token_pattern, text) if t and t.strip()]

    # ---------------------------------------------------------
    # 4. PARSING & FORMATTING LOOP
    # ---------------------------------------------------------
    formatted_tokens = []
    indent_level = 0
    INDENT_SPACE = "    " # 4 spaces
    
    i = 0
    while i < len(raw_tokens):
        token = raw_tokens[i]
        upper_token = token.upper()
        
        # --- Logic: Handle Colons (Requirement 7) ---
        # If we encounter a colon, we skip it (remove it effectively).
        # This assumes the colon is used as "col : type".
        if token == ':':
            i += 1
            continue

        # --- Logic: Classification ---
        processed_token = token

        # check if it is a word (identifier or keyword)
        if re.match(r'^[a-zA-Z0-9_]+$', token):
            if upper_token in KEYWORDS:
                # Requirement 3: Keywords to Upper
                processed_token = upper_token
            elif upper_token in DATA_TYPES:
                # Requirement 6: Data Types to Upper
                processed_token = upper_token
            else:
                # Requirement 4 & 5: Table/View/Column names
                # If preserve case is True, we quote it and keep case
                if preserve_case:
                    processed_token = f'"{token}"'
                else:
                    # If False, convert to Upper (standard SQL behavior)
                    processed_token = upper_token

        # --- Logic: Formatting (Indentation & Commas) ---
        
        # Handling Leading Commas (Requirement 9)
        if processed_token == ',':
            formatted_tokens.append(f"\n{INDENT_SPACE * indent_level}, ")
            
        # Handling Opening Parenthesis (Indentation Increase)
        elif processed_token == '(':
            formatted_tokens.append("(")
            indent_level += 1
            formatted_tokens.append(f"\n{INDENT_SPACE * indent_level}")
            
        # Handling Closing Parenthesis (Indentation Decrease)
        elif processed_token == ')':
            indent_level = max(0, indent_level - 1)
            formatted_tokens.append(f"\n{INDENT_SPACE * indent_level})")

        # Handling Standard Tokens
        else:
            # Add a space before the token if the previous token wasn't a newline starter
            # and this isn't the very first token
            if formatted_tokens and not formatted_tokens[-1].endswith('\n') and not formatted_tokens[-1].endswith(' '):
                 formatted_tokens.append(" ")
            
            formatted_tokens.append(processed_token)

        i += 1

    # Join and clean up potential double spaces or awkward newlines
    result_str = "".join(formatted_tokens)
    
    # Final cleanup regex to tighten up parentheses if they got too spaced out
    # e.g., "func ( " -> "func("
    result_str = result_str.strip()
    
    return result_str
$$;
