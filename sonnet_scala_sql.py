import re
import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import json
from datetime import datetime

class OperationType(Enum):
    """Enumeration of supported Snowpark operations"""
    SELECT = "select"
    FILTER = "filter"
    WHERE = "where"
    JOIN = "join"
    LEFT_JOIN = "leftJoin"
    RIGHT_JOIN = "rightJoin"
    FULL_JOIN = "fullJoin"
    INNER_JOIN = "innerJoin"
    GROUP_BY = "groupBy"
    AGG = "agg"
    ORDER_BY = "orderBy"
    SORT = "sort"
    LIMIT = "limit"
    WITH_COLUMN = "withColumn"
    WITH_COLUMN_RENAMED = "withColumnRenamed"
    DROP = "drop"
    DISTINCT = "distinct"
    UNION = "union"
    UNION_ALL = "unionAll"
    UNION_BY_NAME = "unionByName"


@dataclass
class SQLContext:
    """Context for building SQL queries"""
    select_columns: List[str] = field(default_factory=lambda: ['*'])
    from_table: str = ''
    table_alias: str = ''
    where_conditions: List[str] = field(default_factory=list)
    join_clauses: List[str] = field(default_factory=list)
    group_by_columns: List[str] = field(default_factory=list)
    having_conditions: List[str] = field(default_factory=list)
    order_by_columns: List[str] = field(default_factory=list)
    limit_value: Optional[str] = None
    is_distinct: bool = False
    with_columns: Dict[str, str] = field(default_factory=dict)
    dropped_columns: List[str] = field(default_factory=list)
    renamed_columns: Dict[str, str] = field(default_factory=dict)
    aggregations: List[str] = field(default_factory=list)
    union_queries: List[str] = field(default_factory=list)


@dataclass
class ConversionResult:
    """Result of a conversion operation"""
    source_file: str
    scala_code: str
    sql_code: str
    success: bool
    error_message: Optional[str] = None
    line_count: int = 0
    conversion_time: float = 0.0


class SnowparkScalaToSQLConverter:
    """
    Comprehensive converter for Snowpark Scala Dataset/DataFrame operations to SQL
    Compatible with Scala 2.12+ and Snowpark 1.x+
    """
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.context = SQLContext()
        self.temp_counter = 0
        
        # Scala to SQL function mappings
        self.function_mappings = {
            'sum': 'SUM',
            'avg': 'AVG',
            'mean': 'AVG',
            'count': 'COUNT',
            'min': 'MIN',
            'max': 'MAX',
            'first': 'FIRST_VALUE',
            'last': 'LAST_VALUE',
            'stddev': 'STDDEV',
            'variance': 'VARIANCE',
            'upper': 'UPPER',
            'lower': 'LOWER',
            'trim': 'TRIM',
            'ltrim': 'LTRIM',
            'rtrim': 'RTRIM',
            'length': 'LENGTH',
            'concat': 'CONCAT',
            'substring': 'SUBSTRING',
            'abs': 'ABS',
            'round': 'ROUND',
            'floor': 'FLOOR',
            'ceil': 'CEIL',
            'sqrt': 'SQRT',
            'coalesce': 'COALESCE',
            'nvl': 'COALESCE',
            'current_date': 'CURRENT_DATE',
            'current_timestamp': 'CURRENT_TIMESTAMP',
            'datediff': 'DATEDIFF',
            'dateadd': 'DATEADD',
            'to_date': 'TO_DATE',
            'to_timestamp': 'TO_TIMESTAMP',
        }
        
        # Operator mappings
        self.operator_mappings = {
            '===': '=',
            '!==': '!=',
            '&&': 'AND',
            '||': 'OR',
            '!': 'NOT',
        }
    
    def convert(self, scala_code: str) -> str:
        """
        Main conversion method
        
        Args:
            scala_code: Snowpark Scala code as string
            
        Returns:
            Formatted SQL query string
        """
        try:
            # Reset context for new conversion
            self.context = SQLContext()
            self.temp_counter = 0
            
            # Clean and normalize code
            cleaned_code = self._clean_scala_code(scala_code)
            
            if self.debug:
                print(f"Cleaned code: {cleaned_code}\n")
            
            # Parse the operation chain
            self._parse_scala_chain(cleaned_code)
            
            # Build SQL from context
            sql = self._build_sql_from_context()
            
            # Format and return
            return self._format_sql(sql)
            
        except Exception as e:
            if self.debug:
                import traceback
                traceback.print_exc()
            raise RuntimeError(f"Conversion error: {str(e)}")
    
    def _clean_scala_code(self, code: str) -> str:
        """Clean and normalize Scala code"""
        # Remove single-line comments
        code = re.sub(r'//.*?(?=\n|$)', '', code)
        
        # Remove multi-line comments
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        
        # Remove val/var declarations
        code = re.sub(r'\b(val|var)\s+\w+\s*=\s*', '', code)
        
        # Normalize whitespace
        code = re.sub(r'\s+', ' ', code)
        
        # Remove trailing semicolons
        code = code.replace(';', '')
        
        return code.strip()
    
    def _parse_scala_chain(self, code: str):
        """Parse the entire Snowpark operation chain"""
        # Extract base table/dataset
        self._extract_base_table(code)
        
        # Extract all chained operations
        operations = self._extract_operations(code)
        
        if self.debug:
            print(f"Found {len(operations)} operations")
            for i, op in enumerate(operations):
                print(f"  {i+1}. {op}")
        
        # Process each operation in order
        for operation in operations:
            self._process_operation(operation)
    
    def _extract_base_table(self, code: str):
        """Extract the base table reference"""
        # Pattern: session.table("TABLE_NAME") or session.table('TABLE_NAME')
        table_match = re.search(
            r'session\s*\.\s*table\s*\(\s*["\']([^"\']+)["\']\s*\)',
            code,
            re.IGNORECASE
        )
        
        if table_match:
            self.context.from_table = table_match.group(1)
            if self.debug:
                print(f"Base table: {self.context.from_table}")
        
        # Pattern: session.sql("SELECT ...") 
        sql_match = re.search(
            r'session\s*\.\s*sql\s*\(\s*["\']([^"\']+)["\']\s*\)',
            code,
            re.IGNORECASE
        )
        
        if sql_match:
            self.context.from_table = f"({sql_match.group(1)})"
    
    def _extract_operations(self, code: str) -> List[Tuple[str, str]]:
        """Extract all chained operations"""
        operations = []
        
        # Pattern to match .operation(params)
        pattern = r'\.(\w+)\s*\('
        
        pos = 0
        while True:
            match = re.search(pattern, code[pos:])
            if not match:
                break
            
            op_name = match.group(1)
            start_pos = pos + match.end()
            
            # Find matching closing parenthesis
            params, end_pos = self._extract_params(code, start_pos - 1)
            
            operations.append((op_name, params))
            pos = start_pos + end_pos
        
        return operations
    
    def _extract_params(self, code: str, start: int) -> Tuple[str, int]:
        """Extract parameters handling nested parentheses"""
        if start >= len(code) or code[start] != '(':
            return '', 0
        
        depth = 0
        i = start
        in_string = False
        string_char = None
        
        while i < len(code):
            char = code[i]
            
            # Handle string literals
            if char in ('"', "'") and (i == 0 or code[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            # Count parentheses only outside strings
            if not in_string:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth == 0:
                        return code[start+1:i], i - start
            
            i += 1
        
        return code[start+1:], i - start
    
    def _process_operation(self, operation: Tuple[str, str]):
        """Process a single operation"""
        op_name, params = operation
        
        # Map to handler method
        handler_map = {
            'select': self._handle_select,
            'filter': self._handle_filter,
            'where': self._handle_filter,
            'join': self._handle_join,
            'leftJoin': lambda p: self._handle_join(p, 'LEFT'),
            'rightJoin': lambda p: self._handle_join(p, 'RIGHT'),
            'fullJoin': lambda p: self._handle_join(p, 'FULL OUTER'),
            'innerJoin': lambda p: self._handle_join(p, 'INNER'),
            'groupBy': self._handle_group_by,
            'agg': self._handle_agg,
            'orderBy': self._handle_order_by,
            'sort': self._handle_order_by,
            'limit': self._handle_limit,
            'withColumn': self._handle_with_column,
            'withColumnRenamed': self._handle_with_column_renamed,
            'drop': self._handle_drop,
            'distinct': self._handle_distinct,
            'union': self._handle_union,
            'unionAll': self._handle_union,
            'unionByName': self._handle_union,
            'dropDuplicates': self._handle_distinct,
            'as': self._handle_alias,
            'alias': self._handle_alias,
        }
        
        handler = handler_map.get(op_name)
        if handler:
            handler(params)
        elif self.debug:
            print(f"Warning: Unhandled operation '{op_name}'")
    
    def _handle_select(self, params: str):
        """Handle select operation"""
        columns = self._parse_columns(params)
        if columns:
            self.context.select_columns = columns
    
    def _handle_filter(self, params: str):
        """Handle filter/where operation"""
        condition = self._parse_condition(params)
        if condition:
            self.context.where_conditions.append(condition)
    
    def _handle_join(self, params: str, join_type: str = 'INNER'):
        """Handle join operations"""
        parts = self._smart_split(params, ',')
        
        if len(parts) < 2:
            return
        
        other_table = self._extract_table_reference(parts[0].strip())
        join_condition = self._parse_join_condition(parts[1].strip())
        
        if len(parts) > 2:
            join_type_param = parts[2].strip().strip('"\'').upper()
            if join_type_param in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'):
                join_type = join_type_param
        
        join_clause = f"{join_type} JOIN {other_table} ON {join_condition}"
        self.context.join_clauses.append(join_clause)
    
    def _handle_group_by(self, params: str):
        """Handle groupBy operation"""
        columns = self._parse_columns(params)
        if columns:
            self.context.group_by_columns = columns
    
    def _handle_agg(self, params: str):
        """Handle agg operation"""
        aggregations = self._parse_aggregations(params)
        self.context.aggregations.extend(aggregations)
    
    def _handle_order_by(self, params: str):
        """Handle orderBy/sort operation"""
        columns = self._parse_order_by_columns(params)
        if columns:
            self.context.order_by_columns.extend(columns)
    
    def _handle_limit(self, params: str):
        """Handle limit operation"""
        self.context.limit_value = params.strip()
    
    def _handle_with_column(self, params: str):
        """Handle withColumn operation"""
        parts = self._smart_split(params, ',', max_split=1)
        
        if len(parts) == 2:
            col_name = parts[0].strip().strip('"\'')
            expression = self._parse_expression(parts[1].strip())
            self.context.with_columns[col_name] = expression
    
    def _handle_with_column_renamed(self, params: str):
        """Handle withColumnRenamed operation"""
        parts = self._smart_split(params, ',')
        
        if len(parts) == 2:
            old_name = parts[0].strip().strip('"\'')
            new_name = parts[1].strip().strip('"\'')
            self.context.renamed_columns[old_name] = new_name
    
    def _handle_drop(self, params: str):
        """Handle drop operation"""
        columns = self._parse_columns(params)
        if columns:
            self.context.dropped_columns.extend(columns)
    
    def _handle_distinct(self, params: str):
        """Handle distinct operation"""
        self.context.is_distinct = True
    
    def _handle_union(self, params: str):
        """Handle union operations"""
        pass
    
    def _handle_alias(self, params: str):
        """Handle alias/as operation"""
        alias = params.strip().strip('"\'')
        self.context.table_alias = alias
    
    def _parse_columns(self, params: str) -> List[str]:
        """Parse column list from parameters"""
        if not params.strip():
            return []
        
        columns = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            col_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if col_match:
                columns.append(col_match.group(1))
                continue
            
            dollar_match = re.match(r'\$"([^"]+)"', part)
            if dollar_match:
                columns.append(dollar_match.group(1))
                continue
            
            df_match = re.match(r'\w+\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if df_match:
                columns.append(df_match.group(1))
                continue
            
            string_match = re.match(r'["\']([^"\']+)["\']', part)
            if string_match:
                columns.append(string_match.group(1))
                continue
            
            if ' as ' in part.lower():
                columns.append(self._parse_expression(part))
                continue
            
            if part and not part.startswith('('):
                columns.append(part)
        
        return columns
    
    def _parse_condition(self, condition: str) -> str:
        """Parse filter condition"""
        condition = condition.strip()
        
        if condition.startswith('(') and condition.endswith(')'):
            condition = condition[1:-1].strip()
        
        condition = re.sub(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\1', condition)
        condition = re.sub(r'\$"([^"]+)"', r'\1', condition)
        
        for scala_op, sql_op in self.operator_mappings.items():
            condition = condition.replace(scala_op, f' {sql_op} ')
        
        condition = re.sub(r'(\w+)\.isNull\(\)', r'\1 IS NULL', condition)
        condition = re.sub(r'(\w+)\.isNotNull\(\)', r'\1 IS NOT NULL', condition)
        
        condition = re.sub(
            r'(\w+)\.isin\s*\((.*?)\)',
            lambda m: f"{m.group(1)} IN ({m.group(2)})",
            condition
        )
        
        condition = re.sub(r'\s+', ' ', condition)
        
        return condition.strip()
    
    def _parse_join_condition(self, condition: str) -> str:
        """Parse join condition"""
        condition = condition.strip()
        
        seq_match = re.match(r'Seq\s*\((.*?)\)', condition, re.IGNORECASE)
        if seq_match:
            cols = self._parse_columns(seq_match.group(1))
            return ' AND '.join([f"a.{col} = b.{col}" for col in cols])
        
        array_match = re.match(r'Array\s*\((.*?)\)', condition, re.IGNORECASE)
        if array_match:
            cols = self._parse_columns(array_match.group(1))
            return ' AND '.join([f"a.{col} = b.{col}" for col in cols])
        
        return self._parse_condition(condition)
    
    def _parse_aggregations(self, params: str) -> List[str]:
        """Parse aggregation functions"""
        aggregations = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            agg_pattern = r'(\w+)\s*\(\s*col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\)(?:\s*\.as\s*\(\s*["\']([^"\']+)["\']\s*\)|\s+as\s+["\']([^"\']+)["\'])?'
            
            match = re.match(agg_pattern, part)
            if match:
                func = match.group(1)
                col = match.group(2)
                alias = match.group(3) or match.group(4)
                
                sql_func = self.function_mappings.get(func, func.upper())
                
                if alias:
                    aggregations.append(f"{sql_func}({col}) AS {alias}")
                else:
                    aggregations.append(f"{sql_func}({col}) AS {func}_{col}")
                continue
            
            expr = self._parse_expression(part)
            if expr:
                aggregations.append(expr)
        
        return aggregations
    
    def _parse_order_by_columns(self, params: str) -> List[str]:
        """Parse ORDER BY columns"""
        columns = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            desc_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.desc\s*(?:\(\))?', part)
            if desc_match:
                columns.append(f"{desc_match.group(1)} DESC")
                continue
            
            asc_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.asc\s*(?:\(\))?', part)
            if asc_match:
                columns.append(f"{asc_match.group(1)} ASC")
                continue
            
            col_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if col_match:
                columns.append(col_match.group(1))
                continue
            
            dollar_match = re.match(r'\$"([^"]+)"', part)
            if dollar_match:
                columns.append(dollar_match.group(1))
                continue
            
            if part:
                columns.append(part)
        
        return columns
    
    def _parse_expression(self, expr: str) -> str:
        """Parse complex expressions"""
        expr = expr.strip()
        
        expr = re.sub(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\1', expr)
        expr = re.sub(r'\$"([^"]+)"', r'\1', expr)
        expr = re.sub(r'lit\s*\(\s*([^)]+)\s*\)', r'\1', expr)
        
        for scala_func, sql_func in self.function_mappings.items():
            expr = re.sub(
                f'\\b{scala_func}\\s*\\(',
                f'{sql_func}(',
                expr,
                flags=re.IGNORECASE
            )
        
        as_match = re.search(r'(.+?)(?:\.as\s*\(\s*["\']([^"\']+)["\']\s*\)|as\s+["\']([^"\']+)["\'])\s*$', expr)
        if as_match:
            expr = f"{as_match.group(1)} AS {as_match.group(2) or as_match.group(3)}"
        
        for scala_op, sql_op in self.operator_mappings.items():
            expr = expr.replace(scala_op, f' {sql_op} ')
        
        expr = self._convert_when_otherwise(expr)
        expr = re.sub(r'\s+', ' ', expr)
        
        return expr.strip()
    
    def _convert_when_otherwise(self, expr: str) -> str:
        """Convert when().otherwise() to CASE WHEN"""
        pattern = r'when\s*\(\s*([^,]+),\s*([^)]+)\s*\)\s*\.otherwise\s*\(\s*([^)]+)\s*\)'
        
        def replacer(match):
            condition = self._parse_condition(match.group(1))
            then_value = match.group(2).strip()
            else_value = match.group(3).strip()
            return f"CASE WHEN {condition} THEN {then_value} ELSE {else_value} END"
        
        expr = re.sub(pattern, replacer, expr)
        
        pattern2 = r'when\s*\(\s*([^,]+),\s*([^)]+)\s*\)'
        
        def replacer2(match):
            condition = self._parse_condition(match.group(1))
            then_value = match.group(2).strip()
            return f"CASE WHEN {condition} THEN {then_value} END"
        
        expr = re.sub(pattern2, replacer2, expr)
        
        return expr
    
    def _extract_table_reference(self, ref: str) -> str:
        """Extract table reference from various formats"""
        table_match = re.search(r'session\s*\.\s*table\s*\(\s*["\']([^"\']+)["\']\s*\)', ref)
        if table_match:
            return table_match.group(1)
        
        var_match = re.match(r'\w+', ref)
        if var_match:
            return f"temp_{self.temp_counter}"
            self.temp_counter += 1
        
        return ref
    
    def _smart_split(self, text: str, delimiter: str, max_split: int = -1) -> List[str]:
        """Split text by delimiter, respecting parentheses and quotes"""
        parts = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        split_count = 0
        
        i = 0
        while i < len(text):
            char = text[i]
            
            if char in ('"', "'") and (i == 0 or text[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            if not in_string:
                if char in '([{':
                    depth += 1
                elif char in ')]}':
                    depth -= 1
                elif char == delimiter and depth == 0:
                    if max_split == -1 or split_count < max_split:
                        parts.append(''.join(current))
                        current = []
                        split_count += 1
                        i += 1
                        continue
            
            current.append(char)
            i += 1
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _build_sql_from_context(self) -> str:
        """Build SQL query from context"""
        sql_parts = []
        
        select_clause = self._build_select_clause()
        sql_parts.append(select_clause)
        
        if self.context.from_table:
            from_clause = f"FROM {self.context.from_table}"
            if self.context.table_alias:
                from_clause += f" AS {self.context.table_alias}"
            sql_parts.append(from_clause)
        
        for join_clause in self.context.join_clauses:
            sql_parts.append(join_clause)
        
        if self.context.where_conditions:
            where_clause = "WHERE " + " AND ".join([f"({cond})" for cond in self.context.where_conditions])
            sql_parts.append(where_clause)
        
        if self.context.group_by_columns:
            group_by_clause = "GROUP BY " + ", ".join(self.context.group_by_columns)
            sql_parts.append(group_by_clause)
        
        if self.context.having_conditions:
            having_clause = "HAVING " + " AND ".join(self.context.having_conditions)
            sql_parts.append(having_clause)
        
        if self.context.order_by_columns:
            order_by_clause = "ORDER BY " + ", ".join(self.context.order_by_columns)
            sql_parts.append(order_by_clause)
        
        if self.context.limit_value:
            sql_parts.append(f"LIMIT {self.context.limit_value}")
        
        return " ".join(sql_parts)
    
    def _build_select_clause(self) -> str:
        """Build SELECT clause from context"""
        distinct = "DISTINCT " if self.context.is_distinct else ""
        
        columns = []
        
        if self.context.aggregations:
            columns = self.context.group_by_columns + self.context.aggregations
        elif self.context.with_columns:
            if self.context.select_columns == ['*']:
                for col_name, expr in self.context.with_columns.items():
                    columns.append(f"{expr} AS {col_name}")
                if columns:
                    columns.insert(0, '*')
            else:
                columns = self.context.select_columns.copy()
                for col_name, expr in self.context.with_columns.items():
                    columns.append(f"{expr} AS {col_name}")
        else:
            columns = self.context.select_columns.copy()
        
        if self.context.renamed_columns:
            new_columns = []
            for col in columns:
                if col in self.context.renamed_columns:
                    new_columns.append(f"{col} AS {self.context.renamed_columns[col]}")
                else:
                    new_columns.append(col)
            columns = new_columns
        
        if self.context.dropped_columns and '*' not in columns:
            columns = [col for col in columns if col not in self.context.dropped_columns]
        
        if not columns:
            columns = ['*']
        
        return f"SELECT {distinct}{', '.join(columns)}"
    
    def _format_sql(self, sql: str) -> str:
        """Format SQL for readability"""
        keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 
                   'FULL OUTER JOIN', 'INNER JOIN', 'GROUP BY', 'HAVING', 
                   'ORDER BY', 'LIMIT']
        
        for keyword in keywords:
            sql = re.sub(f' {keyword} ', f'\n{keyword} ', sql, flags=re.IGNORECASE)
        
        lines = sql.split('\n')
        formatted_lines = []
        for line in lines:
            line = re.sub(r'\s+', ' ', line).strip()
            if line:
                formatted_lines.append(line)
        
        return '\n'.join(formatted_lines)


class FileProcessor:
    """Handle file I/O operations for the converter"""
    
    def __init__(self, converter: SnowparkScalaToSQLConverter):
        self.converter = converter
        self.results: List[ConversionResult] = []
    
    def read_file(self, file_path: str) -> str:
        """Read Scala code from a file"""
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            raise RuntimeError(f"Error reading file {file_path}: {str(e)}")
    
    def write_file(self, content: str, file_path: str):
        """Write SQL code to a file"""
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"✅ SQL written to: {file_path}")
        except Exception as e:
            raise RuntimeError(f"Error writing file {file_path}: {str(e)}")
    
    def convert_file(self, input_file: str, output_file: Optional[str] = None) -> ConversionResult:
        """Convert a single Scala file to SQL"""
        start_time = datetime.now()
        
        try:
            # Read Scala code
            scala_code = self.read_file(input_file)
            line_count = len(scala_code.splitlines())
            
            print(f"\n📄 Processing: {input_file}")
            print(f"   Lines: {line_count}")
            
            # Convert to SQL
            sql_code = self.converter.convert(scala_code)
            
            # Write to output file if specified
            if output_file:
                self.write_file(sql_code, output_file)
            
            conversion_time = (datetime.now() - start_time).total_seconds()
            
            result = ConversionResult(
                source_file=input_file,
                scala_code=scala_code,
                sql_code=sql_code,
                success=True,
                line_count=line_count,
                conversion_time=conversion_time
            )
            
            self.results.append(result)
            print(f"✅ Conversion successful ({conversion_time:.2f}s)")
            
            return result
            
        except Exception as e:
            conversion_time = (datetime.now() - start_time).total_seconds()
            
            result = ConversionResult(
                source_file=input_file,
                scala_code="",
                sql_code="",
                success=False,
                error_message=str(e),
                conversion_time=conversion_time
            )
            
            self.results.append(result)
            print(f"❌ Conversion failed: {str(e)}")
            
            return result
    
    def convert_directory(self, input_dir: str, output_dir: str, pattern: str = "*.scala"):
        """Convert all Scala files in a directory"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Directory not found: {input_dir}")
        
        # Find all matching files
        scala_files = list(input_path.rglob(pattern))
        
        if not scala_files:
            print(f"⚠️  No files matching '{pattern}' found in {input_dir}")
            return
        
        print(f"\n{'='*80}")
        print(f"📂 Found {len(scala_files)} Scala file(s) in {input_dir}")
        print(f"{'='*80}")
        
        # Process each file
        for scala_file in scala_files:
            # Determine output file path
            relative_path = scala_file.relative_to(input_path)
            output_file = output_path / relative_path.with_suffix('.sql')
            
            # Convert file
            self.convert_file(str(scala_file), str(output_file))
        
        # Print summary
        self.print_summary()
    
    def convert_multiple_files(self, file_list: List[str], output_dir: Optional[str] = None):
        """Convert multiple Scala files"""
        print(f"\n{'='*80}")
        print(f"📂 Processing {len(file_list)} file(s)")
        print(f"{'='*80}")
        
        for input_file in file_list:
            if output_dir:
                file_name = Path(input_file).stem + '.sql'
                output_file = str(Path(output_dir) / file_name)
            else:
                output_file = None
            
            self.convert_file(input_file, output_file)
        
        self.print_summary()
    
    def print_summary(self):
        """Print conversion summary"""
        if not self.results:
            return
        
        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful
        total_time = sum(r.conversion_time for r in self.results)
        total_lines = sum(r.line_count for r in self.results)
        
        print(f"\n{'='*80}")
        print("📊 CONVERSION SUMMARY")
        print(f"{'='*80}")
        print(f"Total files:       {len(self.results)}")
        print(f"✅ Successful:     {successful}")
        print(f"❌ Failed:         {failed}")
        print(f"📝 Total lines:    {total_lines}")
        print(f"⏱️  Total time:     {total_time:.2f}s")
        print(f"{'='*80}")
        
        if failed > 0:
            print("\n❌ Failed conversions:")
            for result in self.results:
                if not result.success:
                    print(f"   • {result.source_file}: {result.error_message}")
    
    def save_report(self, report_file: str):
        """Save conversion report to JSON"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_files': len(self.results),
            'successful': sum(1 for r in self.results if r.success),
            'failed': sum(1 for r in self.results if not r.success),
            'total_time': sum(r.conversion_time for r in self.results),
            'results': [
                {
                    'source_file': r.source_file,
                    'success': r.success,
                    'line_count': r.line_count,
                    'conversion_time': r.conversion_time,
                    'error_message': r.error_message
                }
                for r in self.results
            ]
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Report saved to: {report_file}")


def create_sample_scala_file(file_path: str):
    """Create a sample Scala file for testing"""
    sample_code = '''
// Sample Snowpark Scala Code
val customerOrders = session.table("CUSTOMERS")
    .select(col("customer_id"), col("customer_name"), col("email"))
    .filter(col("status") === "active")
    .join(session.table("ORDERS"), 
          col("customers.customer_id") === col("orders.customer_id"), 
          "inner")
    .groupBy(col("customer_id"), col("customer_name"))
    .agg(
        sum(col("order_amount")).as("total_spent"),
        count(col("order_id")).as("order_count"),
        avg(col("order_amount")).as("avg_order_value")
    )
    .filter(col("total_spent") > 1000)
    .orderBy(col("total_spent").desc)
    .limit(100)
'''
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(sample_code)
    
    print(f"✅ Sample file created: {file_path}")


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='Convert Snowpark Scala code to Snowflake SQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Convert single file
  python converter.py -i query.scala -o query.sql
  
  # Convert directory
  python converter.py -d ./scala_files -o ./sql_output
  
  # Convert multiple files
  python converter.py -m file1.scala file2.scala -o ./output
  
  # Create sample file
  python converter.py --create-sample sample.scala
  
  # Enable debug mode
  python converter.py -i query.scala -o query.sql --debug
        '''
    )
    
    parser.add_argument('-i', '--input', help='Input Scala file')
    parser.add_argument('-o', '--output', help='Output SQL file or directory')
    parser.add_argument('-d', '--directory', help='Input directory containing Scala files')
    parser.add_argument('-m', '--multiple', nargs='+', help='Multiple input files')
    parser.add_argument('-p', '--pattern', default='*.scala', help='File pattern for directory mode (default: *.scala)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--create-sample', help='Create a sample Scala file')
    parser.add_argument('--report', help='Save conversion report to JSON file')
    
    args = parser.parse_args()
    
    # Handle sample file creation
    if args.create_sample:
        create_sample_scala_file(args.create_sample)
        return
    
    # Initialize converter and processor
    converter = SnowparkScalaToSQLConverter(debug=args.debug)
    processor = FileProcessor(converter)
    
    try:
        # Single file mode
        if args.input:
            result = processor.convert_file(args.input, args.output)
            if not args.output:
                print(f"\n{'='*80}")
                print("📄 CONVERTED SQL:")
                print(f"{'='*80}")
                print(result.sql_code)
                print(f"{'='*80}")
        
        # Directory mode
        elif args.directory:
            if not args.output:
                print("Error: Output directory (-o) is required for directory mode")
                sys.exit(1)
            processor.convert_directory(args.directory, args.output, args.pattern)
        
        # Multiple files mode
        elif args.multiple:
            processor.convert_multiple_files(args.multiple, args.output)
        
        else:
            parser.print_help()
            sys.exit(0)
        
        # Save report if requested
        if args.report:
            processor.save_report(args.report)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


# Convert and save to file
python converter.py -i input.scala -o output.sql

# Convert and print to console
python converter.py -i input.scala

# With debug mode
python converter.py -i input.scala -o output.sql --debug
