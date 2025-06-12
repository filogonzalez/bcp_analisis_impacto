import base64
import re
import ast
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from langchain_text_splitters import RecursiveCharacterTextSplitter

class NotebookProcessor:
    def __init__(self):
        self.w = WorkspaceClient()
    
    def list_notebooks(self, root_path: str = "/") -> list:
        def _list_path(path: str):
            try:
                for obj in self.w.workspace.list(path):
                    if obj.object_type == workspace.ObjectType.NOTEBOOK:
                        yield obj.path
                    elif obj.object_type == workspace.ObjectType.DIRECTORY:
                        yield from _list_path(obj.path)
            except PermissionError as e:
                print(f"Permission denied for {path}: {e}")
        return list(_list_path(root_path))

    def export_notebook(self, path: str) -> dict:
        try:
            export = self.w.workspace.export(path, format=workspace.ExportFormat.SOURCE)
            return {
                "path": path,
                "content": base64.b64decode(export.content).decode('utf-8'),
                "base_language": export.file_type
            }
        except Exception as e:
            print(f"Failed to export {path}: {e}")
            return None

class CodeAwareSplitter:
    def __init__(self):
        # Language-specific splitters with optimized parameters
        self.splitter_map = {
            "py": RecursiveCharacterTextSplitter.from_language(
                language="python",
                chunk_size=1200,
                chunk_overlap=300,
                keep_separator=True
            ),
            "sql": RecursiveCharacterTextSplitter(
                separators=[";\n", "CREATE", "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", 
                           "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN"],
                chunk_size=1500,
                chunk_overlap=300,
                keep_separator=True
            ),
            "scala": RecursiveCharacterTextSplitter.from_language(
                language="scala",
                chunk_size=1200,
                chunk_overlap=300,
                keep_separator=True
            ),
            "default": RecursiveCharacterTextSplitter(
                separators=["\n\n", "\n", " ", ""],
                chunk_size=512,
                chunk_overlap=50
            )
        }
        self.code_block_pattern = re.compile(
            r'(^class\s+\w+:|^def\s+\w+\(.*\):)',
            re.MULTILINE
        )
        self.cell_separator = re.compile(r'^# COMMAND ----------\n', re.MULTILINE)

    def _detect_cell_language(self, cell_content: str, base_language: str) -> str:
        """Detect the language of a cell based on magic commands or content"""
        # Check for magic commands at the beginning of the cell
        lines = cell_content.strip().split('\n')
        for line in lines[:3]:  # Check first few lines for magic commands
            line = line.strip().lower()
            if '%sql' in line:
                return 'sql'
            elif '%python' in line or '%py' in line:
                return 'py'
            elif '%scala' in line:
                return 'scala'
            elif '%r' in line:
                return 'r'
            elif '%md' in line:
                return 'md'
        
        # If no magic command found, use base language
        return base_language
        
    def _clean_magic_commands(self, cell_content: str) -> str:
        """Clean Databricks MAGIC commands from cell content"""
        # Remove # MAGIC prefix from lines
        magic_pattern = re.compile(r'^# MAGIC\s*', re.MULTILINE)
        cleaned_content = magic_pattern.sub('', cell_content)
        
        # Remove magic command lines
        magic_line_pattern = re.compile(r'^\s*%\w+\s*$', re.MULTILINE)
        cleaned_content = magic_line_pattern.sub('', cleaned_content)
        
        # Remove magic command from the beginning of lines
        magic_cmd_pattern = re.compile(r'^\s*%\w+\s*', re.MULTILINE)
        cleaned_content = magic_cmd_pattern.sub('', cleaned_content)
        
        # Clean up extra newlines
        cleaned_content = re.sub(r'\n{3,}', '\n\n', cleaned_content)
        
        return cleaned_content.strip()

    def detect_operation_types(self, content: str, lang: str) -> list:
        """Detect operation types (join, filter, aggregation, etc.)"""
        operations = []
        
        # Check for spark.sql within Python code
        if lang == "py" and re.search(r'spark\.sql\s*\(', content):
            # Extract SQL query string
            sql_queries = re.findall(r'spark\.sql\s*\(\s*"""(.*?)"""\s*\)|spark\.sql\s*\(\s*\'\'\'(.*?)\'\'\'\s*\)|spark\.sql\s*\(\s*"(.*?)"\s*\)|spark\.sql\s*\(\s*\'(.*?)\'\s*\)', content, re.DOTALL)
            
            for query_match in sql_queries:
                # Find the non-empty group in the tuple
                query = next((q for q in query_match if q), "")
                if query:
                    # Apply SQL operation detection on the query
                    if re.search(r'\bJOIN\b', query, re.IGNORECASE):
                        operations.append("join")
                    if re.search(r'\bWHERE\b', query, re.IGNORECASE):
                        operations.append("filter")
                    if re.search(r'\bGROUP\s+BY\b', query, re.IGNORECASE):
                        operations.append("aggregation")
                    if re.search(r'\bORDER\s+BY\b', query, re.IGNORECASE):
                        operations.append("sorting")
                    if re.search(r'\bAVG\s*\(|\bSUM\s*\(|\bCOUNT\s*\(|\bMIN\s*\(|\bMAX\s*\(', query, re.IGNORECASE):
                        operations.append("aggregation")
                    if re.search(r'\bCASE\s+WHEN\b', query, re.IGNORECASE):
                        operations.append("conditional_transform")
                    if re.search(r'\bCAST\s*\(|\bCONVERT\s*\(', query, re.IGNORECASE):
                        operations.append("type_conversion")
        
        if lang == "sql":
            if re.search(r'\bJOIN\b', content, re.IGNORECASE):
                operations.append("join")
            if re.search(r'\bWHERE\b', content, re.IGNORECASE):
                operations.append("filter")
            if re.search(r'\bGROUP\s+BY\b', content, re.IGNORECASE):
                operations.append("aggregation")
            if re.search(r'\bORDER\s+BY\b', content, re.IGNORECASE):
                operations.append("sorting")
            if re.search(r'\bAVG\s*\(|\bSUM\s*\(|\bCOUNT\s*\(|\bMIN\s*\(|\bMAX\s*\(', content, re.IGNORECASE):
                operations.append("aggregation")
            if re.search(r'\bINSERT\b', content, re.IGNORECASE):
                operations.append("insert")
            if re.search(r'\bUPDATE\b', content, re.IGNORECASE):
                operations.append("update")
            if re.search(r'\bDELETE\b', content, re.IGNORECASE):
                operations.append("delete")
            if re.search(r'\bCAST\s*\(|\bCONVERT\s*\(', content, re.IGNORECASE):
                operations.append("type_conversion")
            if re.search(r'\bCASE\s+WHEN\b', content, re.IGNORECASE):
                operations.append("conditional_transform")
        
        elif lang == "py":
            if re.search(r'\.join\(', content):
                operations.append("join")
            if re.search(r'\.filter\(|\bwhere\b', content):
                operations.append("filter")
            if re.search(r'\.groupBy\(|\bgroup\(', content):
                operations.append("aggregation")
            if re.search(r'\.sort\(|\borderBy\(', content):
                operations.append("sorting")
            if re.search(r'\.mean\(|\bsum\(|\bcount\(|\bmin\(|\bmax\(|\baverage\(|\bagg\(', content):
                operations.append("aggregation")
            if re.search(r'\.cast\(|\bastype\(|\bconvert_dtypes\(', content):
                operations.append("type_conversion")
            if re.search(r'\.withColumn\(', content):
                operations.append("column_creation")
            if re.search(r'when\(|\botherwise\(', content):
                operations.append("conditional_transform")
        
        return list(set(operations))

    def _split_by_ast(self, content: str) -> list:
        """Split Python code using AST analysis"""
        try:
            tree = ast.parse(content)
            chunks = []
            current_chunk = []
            current_size = 0
            max_chunk_size = 1200  # Maximum size for a chunk of standalone code
            
            for node in tree.body:
                node_content = ast.unparse(node)
                node_size = len(node_content)
                
                # For imports, always add them to the current chunk
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    current_chunk.append(node_content)
                    current_size += node_size
                # For class and function definitions, process them separately
                elif isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                    # If we have accumulated statements, add them as a chunk
                    if current_chunk:
                        chunks.append('\n'.join(current_chunk))
                        current_chunk = []
                        current_size = 0
                    
                    # Process the class/function definition
                    if node_size > 2000:  # If too large, use the splitter
                        splitter = self.splitter_map["py"]
                        chunks.extend(splitter.split_text(node_content))
                    else:
                        chunks.append(node_content)
                # For other statements, group them into chunks based on size
                else:
                    # If adding this statement would exceed the max size, start a new chunk
                    if current_size + node_size > max_chunk_size and current_chunk:
                        chunks.append('\n'.join(current_chunk))
                        current_chunk = []
                        current_size = 0
                    
                    current_chunk.append(node_content)
                    current_size += node_size
            
            # Add any remaining statements as a chunk
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
                
            return chunks if chunks else [content]
        except SyntaxError:
            return [content]

    def _process_code_structures(self, content: str, base_lang: str) -> list:
        """Process code structures while preserving context and structure"""
        # For SQL cells, use SQL-specific processing
        if base_lang == "sql":
            return self._process_sql_content(content)
            
        # For Python code, use AST-based splitting
        elif base_lang == "py":
            chunks = self._split_by_ast(content)
            # If AST splitting didn't produce any chunks, fall back to pattern-based approach
            if len(chunks) == 1 and len(chunks[0]) == len(content):
                return self._process_with_patterns(content, base_lang)
            return chunks
            
        # For other languages, use the existing pattern-based approach
        return self._process_with_patterns(content, base_lang)
    
    def _process_sql_content(self, content: str) -> list:
        """Process SQL content with awareness of statements and clauses"""
        # If content is small enough, return as is
        if len(content) <= 1500:
            return [content]
            
        # Try to extract complete SQL statements
        statements = []
        current_statement = []
        
        # Strip any leading/trailing whitespace and comments
        content = content.strip()
        
        # First split on semicolons, but be careful with nested statements
        lines = content.split('\n')
        for line in lines:
            line_stripped = line.strip()
            # Skip empty lines
            if not line_stripped:
                current_statement.append(line)  # Keep empty lines for formatting
                continue
                
            # Skip single-line comments but keep them in the statement
            if line_stripped.startswith('--'):
                current_statement.append(line)
                continue
                
            # Add the line to current statement
            current_statement.append(line)
            
            # If this line ends with a semicolon outside of a comment or string,
            # it might be the end of a statement
            if line_stripped.endswith(';'):
                # Ignore if semicolon is inside a comment
                if '--' in line_stripped and line_stripped.rfind('--') < line_stripped.rfind(';'):
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                # Ignore if semicolon is inside a multi-line comment
                elif '/*' in content and '*/' in content and content.rfind('*/') < content.rfind(';'):
                    continue
                else:
                    statements.append('\n'.join(current_statement))
                    current_statement = []
        
        # Add any remaining lines as a final statement
        if current_statement:
            statements.append('\n'.join(current_statement))
            
        # If we have multiple statements, return them
        if len(statements) > 1:
            return statements
            
        # If we're here, we have a single large statement, use the SQL splitter
        splitter = self.splitter_map.get("sql", self.splitter_map["default"])
        return splitter.split_text(content)

    def _process_with_patterns(self, content: str, base_lang: str) -> list:
        """Process code structures using pattern-based approach"""
        chunks = []
        current_block = []
        lines = content.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i]
            if not line.strip():
                i += 1
                continue

            # Check if this is a new code block
            if self.code_block_pattern.match(line):
                # If we have a previous block, process it
                if current_block:
                    block_content = '\n'.join(current_block)
                    chunks.extend(self._split_large_code_blocks(block_content, base_lang))
                    current_block = []

                # Start new block
                current_block = [line]
                i += 1

                # Add all lines that belong to this block
                while i < len(lines):
                    next_line = lines[i]
                    if not next_line.strip():
                        current_block.append(next_line)
                        i += 1
                        continue

                    if self.code_block_pattern.match(next_line):
                        break

                    current_block.append(next_line)
                    i += 1
            else:
                # Add non-block lines to current block
                current_block.append(line)
                i += 1

        # Process any remaining block
        if current_block:
            block_content = '\n'.join(current_block)
            chunks.extend(self._split_large_code_blocks(block_content, base_lang))

        return chunks

    def _split_large_code_blocks(self, content: str, lang: str) -> list:
        """Split large code blocks while preserving structure"""
        if len(content) < 2000:
            return [content]
            
        splitter = self.splitter_map.get(lang, self.splitter_map["default"])
        if not splitter:
            return [content]
            
        return splitter.split_text(content)

    def split(self, content: str, base_language: str) -> list:
        """Split notebook content into logical chunks"""
        cells = self.cell_separator.split(content)
        all_chunks = []
        
        for cell in cells:
            if not cell.strip():
                continue
                
            # Detect the language before cleaning
            lang = self._detect_cell_language(cell, base_language)
            
            # Clean magic commands but preserve the content
            clean_cell = self._clean_magic_commands(cell)
            
            # Process each cell based on language
            cell_chunks = self._process_code_structures(clean_cell, lang)
            
            # Add language information to each chunk
            for chunk in cell_chunks:
                # If chunk is a string, convert to dict with language
                if isinstance(chunk, str):
                    all_chunks.append({
                        "content": chunk,
                        "language": lang
                    })
                else:
                    # If chunk is already a dict, ensure it has language info
                    chunk["language"] = lang
                    all_chunks.append(chunk)
            
        return all_chunks

def process_notebook(notebook: dict) -> list:
    splitter = CodeAwareSplitter()
    initial_chunks = splitter.split(notebook["content"], notebook["base_language"])
    
    # Get language-specific chunk size limit
    language_limits = {
        "py": 1200,
        "sql": 1500,
        "scala": 1200,
        "md": 800,    # Smaller limit for markdown since it's usually explanatory
        "default": 512
    }
    
    # First, preprocess chunks to get language and operation types
    chunks = []
    for chunk in initial_chunks:
        content = chunk["content"] if isinstance(chunk, dict) else chunk
        language = chunk.get("language") if isinstance(chunk, dict) else splitter._detect_cell_language(content, notebook["base_language"])
        operation_types = splitter.detect_operation_types(content, language)
        code_structures = list(set(re.findall(r'(class\s+\w+|def\s+\w+)', content)))
        
        chunks.append({
            "content": content,
            "language": language,
            "size": len(content),
            "operation_types": operation_types,
            "code_structures": code_structures,
            "is_code": language not in ["md"]  # Track if this is actual code or markdown
        })
    
    # Process chunks sequentially, combining runs of same-language chunks up to the limit
    combined_chunks = []
    i = 0
    while i < len(chunks):
        current_chunk = chunks[i]
        curr_content = current_chunk["content"]
        curr_lang = current_chunk["language"]
        curr_size = current_chunk["size"]
        curr_ops = current_chunk["operation_types"]
        curr_structures = current_chunk["code_structures"]
        curr_is_code = current_chunk["is_code"]
        chunk_limit = language_limits.get(curr_lang, language_limits["default"])
        
        # Find how many consecutive chunks of the same language we can combine
        combined_content = curr_content
        combined_ops = curr_ops.copy()
        combined_structures = curr_structures.copy()
        next_index = i + 1
        
        # Only try to combine code with code, markdown with markdown
        while next_index < len(chunks):
            next_chunk = chunks[next_index]
            next_content = next_chunk["content"]
            next_lang = next_chunk["language"]
            next_size = next_chunk["size"]
            next_is_code = next_chunk["is_code"]
            
            # Stop if languages don't match or code/non-code status doesn't match
            if curr_lang != next_lang or curr_is_code != next_is_code:
                break
                
            # Check if adding this chunk would exceed the size limit
            if curr_size + next_size + 1 > chunk_limit:  # +1 for newline
                break
                
            # Add this chunk to our combined content
            combined_content += "\n" + next_content
            curr_size += next_size + 1  # +1 for newline
            
            # Merge operation types and code structures
            combined_ops.extend(next_chunk["operation_types"])
            combined_structures.extend(next_chunk["code_structures"])
            
            next_index += 1
        
        # Add the combined chunk
        combined_chunks.append({
            "content": combined_content,
            "language": curr_lang,
            "operation_types": list(set(combined_ops)),
            "code_structures": list(set(combined_structures)),
            "is_code": curr_is_code
        })
        
        # Move to the next chunk after what we've combined
        i = next_index
    
    processed_chunks = []
    for i, chunk in enumerate(combined_chunks):
        processed_chunks.append({
            "notebook_path": notebook["path"],
            "chunk_id": f"{notebook['path']}_{i}",
            "content": chunk["content"],
            "language": chunk["language"],
            "chunk_size": len(chunk["content"]),
            "is_code": chunk["is_code"],
            "code_structures": chunk["code_structures"],
            "operation_types": chunk["operation_types"]
        })
    
    return processed_chunks