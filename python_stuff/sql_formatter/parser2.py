import re


class SimpleSQLParser:
    # Common SQL keywords for recognition
    KEYWORDS = {
        'select', 'from', 'where', 'insert', 'update', 'delete', 'join',
        'on', 'and', 'or', 'group', 'by', 'order', 'having', 'as'
    }

    def __init__(self, sql_script):
        self.sql_script = sql_script.strip()
        self.tokens = []
        self.formatted_sql = ""

    def tokenize(self):
        # Split SQL into tokens while preserving quoted strings and handling punctuation
        token_pattern = r"('[^']*'|[a-zA-Z_][a-zA-Z0-9_]*|[(),;=<>!+\-*\/%]|[\s]+)"
        raw_tokens = re.split(token_pattern, self.sql_script)

        # Filter out empty tokens and unnecessary whitespace
        self.tokens = [t.strip() for t in raw_tokens if t and not t.isspace()]
        return self.tokens

    def format_sql(self, indent_size=4, uppercase_keywords=True):
        if not self.tokens:
            self.tokenize()

        indent_level = 0
        result = []
        line = []

        for token in self.tokens:
            # Handle newlines and indentation for major clauses
            if token.lower() in {'select', 'from', 'where', 'join', 'group', 'order'}:
                if line:  # Flush current line before starting a new clause
                    result.append(" ".join(line))
                    line = []
                if token.lower() in {'from', 'where', 'join'}:
                    indent_level = 0   # Indent after SELECT
                elif token.lower() in {'group', 'order'}:
                    indent_level = 0  # Reset indent for GROUP/ORDER

            # Apply formatting to token
            if token.lower() in self.KEYWORDS and uppercase_keywords:
                line.append(token.upper())
            else:
                line.append(token)

            # Handle commas and semicolons for line breaks
            if token in {',', ';'}:
                result.append(" " * (indent_level * indent_size) + " ".join(line))
                line = []

        # Append any remaining tokens
        if line:
            result.append(" " * (indent_level * indent_size) + " ".join(line))

        self.formatted_sql = "\n".join(result)
        return self.formatted_sql


# Example usage
if __name__ == "__main__":
    # Sample SQL script
    sql = """
    select id,name from users where age>18 and status='active' order by id
    """

    # Create parser instance
    parser = SimpleSQLParser(sql)

    # Tokenize (optional: inspect tokens)
    tokens = parser.tokenize()
    print("Tokens:", tokens)

    # Format the SQL
    formatted = parser.format_sql(indent_size=4, uppercase_keywords=True)
    print("\nFormatted SQL:\n", formatted)
