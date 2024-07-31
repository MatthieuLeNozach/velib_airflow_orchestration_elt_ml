import re
from jinja2 import Template
import os

class SQLLoader:
    def __init__(self, sql_file):
        """
        Initialize the SQLLoader with a given SQL file.
        
        :param sql_file: Path to the SQL file containing the statements.
        """
        # Read the content of the SQL file
        with open(sql_file, 'r') as f:
            self.sql_content = f.read()
        
        # Parse the SQL statements from the file content
        self.statements = self._parse_statements()

    def _parse_statements(self):
        """
        Parse the SQL statements from the SQL file content.

        :return: A dictionary with statement names as keys and SQL statements as values.
        """
        # Define the regex pattern to match named SQL statements
        pattern = r'--\s*name:\s*(\w+)\s*(.*?)(?=--\s*name:|$)'
        
        # Find all matches of the pattern in the SQL content
        matches = re.finditer(pattern, self.sql_content, re.DOTALL)
        
        # Return a dictionary of named statements
        return {match.group(1): match.group(2).strip() for match in matches}

    def get_statement(self, name, **params):
        """
        Get a SQL statement by name and render it with optional parameters.

        :param name: The name of the SQL statement to retrieve.
        :param params: Optional parameters to render the SQL statement.
        :return: The rendered SQL statement.
        :raises KeyError: If the statement name is not found in the parsed statements.
        """
        if name not in self.statements:
            raise KeyError(f"Statement '{name}' not found in SQL file")
        
        # Get the template for the named statement
        template = Template(self.statements[name])
        
        # Render the template with the provided parameters
        return template.render(**params)



class MultiFileSQLLoader:
    def __init__(self, sql_directory):
        self.loaders = {}
        for filename in os.listdir(sql_directory):
            if filename.endswith('.sql'):
                name = os.path.splitext(filename)[0]
                self.loaders[name] = SQLLoader(os.path.join(sql_directory, filename))

    def get_statement(self, file, name, **params):
        if file not in self.loaders:
            raise KeyError(f"SQL file '{file}' not found")
        return self.loaders[file].get_statement(name, **params)