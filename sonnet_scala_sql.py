import re
import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import json
from collections import OrderedDict

class DataSourceType(Enum):
    """Types of data sources in Snowpark"""
    TABLE = "table"
    DATASET = "dataset"
    DATAFRAME = "dataframe"
    SQL_QUERY = "sql"
    VIEW = "view"
    CTE = "cte"
    TEMP_TABLE = "temp_table"


class OperationType(Enum):
    """All supported Snowpark operations"""
    # Selection and Projection
    SELECT = "select"
    SELECT_EXPR = "selectExpr"
    
    # Filtering
    FILTER = "filter"
    WHERE = "where"
    
    # Joins
    JOIN = "join"
    LEFT_JOIN = "leftJoin"
    RIGHT_JOIN = "rightJoin"
    FULL_JOIN = "fullJoin"
    INNER_JOIN = "innerJoin"
    CROSS_JOIN = "crossJoin"
    
    # Aggregation
    GROUP_BY = "groupBy"
    AGG = "agg"
    
    # Ordering
    ORDER_BY = "orderBy"
    SORT = "sort"
    
    # Limiting
    LIMIT = "limit"
    
    # Column Operations
    WITH_COLUMN = "withColumn"
    WITH_COLUMN_RENAMED = "withColumnRenamed"
    DROP = "drop"
    DROP_DUPLICATES = "dropDuplicates"
    
    # Set Operations
    DISTINCT = "distinct"
    UNION = "union"
    UNION_ALL = "unionAll"
    UNION_BY_NAME = "unionByName"
    INTERSECT = "intersect"
    EXCEPT = "except"
    
    # Dataset-specific
    MAP = "map"
    FLAT_MAP = "flatMap"
    MAP_PARTITIONS = "mapPartitions"
    REDUCE = "reduce"
    
    # Window Operations
    WINDOW = "window"
    OVER = "over"
    
    # Actions
    SHOW = "show"
    COLLECT = "collect"
    COUNT = "count"
    FIRST = "first"
    TAKE = "take"
    
    # Table Operations
    CREATE_OR_REPLACE_VIEW = "createOrReplaceView"
    CREATE_TEMP_VIEW = "createTempView"
    WRITE = "write"
    SAVE_AS_TABLE = "saveAsTable"


@dataclass
class DataSource:
    """Represents a data source (table, dataset, dataframe)"""
    name: str
    source_type: DataSourceType
    sql_definition: Optional[str] = None
    variable_name: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    schema: Dict[str, str] = field(default_factory=dict)


@dataclass
class SQLContext:
    """Enhanced context for building SQL queries"""
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
    window_specs: Dict[str, str] = field(default_factory=dict)
    ctes: OrderedDict = field(default_factory=OrderedDict)
    union_queries: List[str] = field(default_factory=list)


@dataclass
class ConversionResult:
    """Result of conversion"""
    source_file: str
    scala_code: str
    sql_code: str
    success: bool
    data_sources: Dict[str, DataSource] = field(default_factory=dict)
    queries: Dict[str, str] = field(default_factory=dict)
    error_message: Optional[str] = None
    line_count: int = 0
    conversion_time: float = 0.0
    warnings: List[str] = field(default_factory=list)


class ComprehensiveSnowparkConverter:
    """
    Complete Snowpark Scala to Snowflake SQL Converter
    Supports: Datasets, DataFrames, Complex Operations, Window Functions, CTEs
    """
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.context = SQLContext()
        self.data_sources: Dict[str, DataSource] = {}
        self.variables: Dict[str, str] = {}  # Track variable assignments
        self.temp_counter = 0
        self.cte_counter = 0
        self.warnings: List[str] = []
        
        # Comprehensive function mappings
        self.function_mappings = {
            # Aggregate functions
            'sum': 'SUM',
            'avg': 'AVG',
            'mean': 'AVG',
            'count': 'COUNT',
            'countDistinct': 'COUNT(DISTINCT',
            'min': 'MIN',
            'max': 'MAX',
            'first': 'FIRST_VALUE',
            'last': 'LAST_VALUE',
            'stddev': 'STDDEV',
            'stddev_samp': 'STDDEV_SAMP',
            'stddev_pop': 'STDDEV_POP',
            'variance': 'VARIANCE',
            'var_samp': 'VAR_SAMP',
            'var_pop': 'VAR_POP',
            
            # String functions
            'upper': 'UPPER',
            'lower': 'LOWER',
            'trim': 'TRIM',
            'ltrim': 'LTRIM',
            'rtrim': 'RTRIM',
            'length': 'LENGTH',
            'concat': 'CONCAT',
            'concat_ws': 'CONCAT_WS',
            'substring': 'SUBSTRING',
            'substr': 'SUBSTR',
            'replace': 'REPLACE',
            'split': 'SPLIT',
            'regexp_replace': 'REGEXP_REPLACE',
            'regexp_extract': 'REGEXP_SUBSTR',
            'initcap': 'INITCAP',
            'reverse': 'REVERSE',
            'translate': 'TRANSLATE',
            'lpad': 'LPAD',
            'rpad': 'RPAD',
            
            # Math functions
            'abs': 'ABS',
            'round': 'ROUND',
            'floor': 'FLOOR',
            'ceil': 'CEIL',
            'ceiling': 'CEIL',
            'sqrt': 'SQRT',
            'pow': 'POWER',
            'power': 'POWER',
            'exp': 'EXP',
            'log': 'LOG',
            'log10': 'LOG',
            'ln': 'LN',
            'sin': 'SIN',
            'cos': 'COS',
            'tan': 'TAN',
            'asin': 'ASIN',
            'acos': 'ACOS',
            'atan': 'ATAN',
            'sign': 'SIGN',
            'mod': 'MOD',
            
            # Date/Time functions
            'current_date': 'CURRENT_DATE',
            'current_timestamp': 'CURRENT_TIMESTAMP',
            'date_add': 'DATEADD',
            'date_sub': 'DATEADD',
            'datediff': 'DATEDIFF',
            'date_format': 'TO_CHAR',
            'to_date': 'TO_DATE',
            'to_timestamp': 'TO_TIMESTAMP',
            'year': 'YEAR',
            'month': 'MONTH',
            'day': 'DAY',
            'dayofmonth': 'DAY',
            'dayofweek': 'DAYOFWEEK',
            'dayofyear': 'DAYOFYEAR',
            'hour': 'HOUR',
            'minute': 'MINUTE',
            'second': 'SECOND',
            'quarter': 'QUARTER',
            'weekofyear': 'WEEKOFYEAR',
            'last_day': 'LAST_DAY',
            'next_day': 'NEXT_DAY',
            'add_months': 'ADD_MONTHS',
            'months_between': 'MONTHS_BETWEEN',
            'trunc': 'TRUNC',
            'date_trunc': 'DATE_TRUNC',
            
            # Null handling
            'coalesce': 'COALESCE',
            'nvl': 'COALESCE',
            'nvl2': 'NVL2',
            'ifnull': 'IFNULL',
            'nullif': 'NULLIF',
            
            # Conditional
            'when': 'CASE WHEN',
            'otherwise': 'ELSE',
            
            # Type conversion
            'cast': 'CAST',
            
            # Window functions
            'row_number': 'ROW_NUMBER',
            'rank': 'RANK',
            'dense_rank': 'DENSE_RANK',
            'percent_rank': 'PERCENT_RANK',
            'ntile': 'NTILE',
            'cume_dist': 'CUME_DIST',
            'lag': 'LAG',
            'lead': 'LEAD',
            
            # Array/Collection functions
            'array': 'ARRAY_CONSTRUCT',
            'array_contains': 'ARRAY_CONTAINS',
            'array_distinct': 'ARRAY_DISTINCT',
            'array_except': 'ARRAY_EXCEPT',
            'array_intersect': 'ARRAY_INTERSECT',
            'array_join': 'ARRAY_TO_STRING',
            'array_max': 'ARRAY_MAX',
            'array_min': 'ARRAY_MIN',
            'array_position': 'ARRAY_POSITION',
            'array_remove': 'ARRAY_REMOVE',
            'array_repeat': 'ARRAY_GENERATE_RANGE',
            'array_sort': 'ARRAY_SORT',
            'array_union': 'ARRAY_UNION',
            'size': 'ARRAY_SIZE',
            'slice': 'ARRAY_SLICE',
            'explode': 'FLATTEN',
            
            # JSON functions
            'get_json_object': 'GET_PATH',
            'json_tuple': 'PARSE_JSON',
            'from_json': 'PARSE_JSON',
            'to_json': 'TO_JSON',
            
            # Other functions
            'md5': 'MD5',
            'sha1': 'SHA1',
            'sha2': 'SHA2',
            'hash': 'HASH',
            'crc32': 'CRC32',
            'base64': 'BASE64_ENCODE',
            'unbase64': 'BASE64_DECODE',
            'decode': 'TRY_BASE64_DECODE_STRING',
            'encode': 'BASE64_ENCODE',
        }
        
        # Operator mappings
        self.operator_mappings = {
            '===': '=',
            '!==': '!=',
            '<>': '!=',
            '&&': 'AND',
            '||': 'OR',
            '!': 'NOT',
            'and': 'AND',
            'or': 'OR',
        }
        
        # Type mappings (Scala to SQL)
        self.type_mappings = {
            'String': 'VARCHAR',
            'Int': 'INTEGER',
            'Integer': 'INTEGER',
            'Long': 'BIGINT',
            'Double': 'DOUBLE',
            'Float': 'FLOAT',
            'Boolean': 'BOOLEAN',
            'Byte': 'TINYINT',
            'Short': 'SMALLINT',
            'Date': 'DATE',
            'Timestamp': 'TIMESTAMP',
            'Decimal': 'DECIMAL',
            'Binary': 'BINARY',
            'Array': 'ARRAY',
            'Map': 'OBJECT',
            'Struct': 'OBJECT',
        }
    
    def convert(self, scala_code: str) -> str:
        """Main conversion method"""
        try:
            # Reset state
            self._reset_state()
            
            # Clean code
            cleaned_code = self._clean_scala_code(scala_code)
            
            if self.debug:
                print(f"📝 Cleaned code:\n{cleaned_code}\n")
            
            # Extract all data source definitions
            self._extract_data_sources(cleaned_code)
            
            # Parse and convert all queries
            queries = self._parse_all_queries(cleaned_code)
            
            # Build final SQL
            sql = self._build_final_sql(queries)
            
            return self._format_sql(sql)
            
        except Exception as e:
            if self.debug:
                import traceback
                traceback.print_exc()
            raise RuntimeError(f"Conversion error: {str(e)}")
    
    def convert_advanced(self, scala_code: str) -> ConversionResult:
        """Advanced conversion with detailed results"""
        start_time = datetime.now()
        
        try:
            # Reset state
            self._reset_state()
            
            # Clean code
            cleaned_code = self._clean_scala_code(scala_code)
            line_count = len(scala_code.splitlines())
            
            # Extract data sources
            self._extract_data_sources(cleaned_code)
            
            # Parse and convert
            queries = self._parse_all_queries(cleaned_code)
            
            # Build SQL
            sql = self._build_final_sql(queries)
            sql = self._format_sql(sql)
            
            conversion_time = (datetime.now() - start_time).total_seconds()
            
            return ConversionResult(
                source_file="",
                scala_code=scala_code,
                sql_code=sql,
                success=True,
                data_sources=self.data_sources.copy(),
                queries=queries,
                line_count=line_count,
                conversion_time=conversion_time,
                warnings=self.warnings.copy()
            )
            
        except Exception as e:
            conversion_time = (datetime.now() - start_time).total_seconds()
            
            return ConversionResult(
                source_file="",
                scala_code=scala_code,
                sql_code="",
                success=False,
                error_message=str(e),
                line_count=len(scala_code.splitlines()),
                conversion_time=conversion_time,
                warnings=self.warnings.copy()
            )
    
    def _reset_state(self):
        """Reset converter state"""
        self.context = SQLContext()
        self.data_sources.clear()
        self.variables.clear()
        self.temp_counter = 0
        self.cte_counter = 0
        self.warnings.clear()
    
    def _clean_scala_code(self, code: str) -> str:
        """Clean and normalize Scala code"""
        # Remove single-line comments
        code = re.sub(r'//.*?(?=\n|$)', '', code)
        
        # Remove multi-line comments
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        
        # Preserve line breaks for multi-line statements
        code = re.sub(r'\s*\n\s*', ' ', code)
        
        # Normalize whitespace
        code = re.sub(r'\s+', ' ', code)
        
        return code.strip()
    
    def _extract_data_sources(self, code: str):
        """Extract all data source definitions (tables, datasets, dataframes)"""
        
        # Pattern: val dfName = session.table("TABLE_NAME")
        table_pattern = r'(?:val|var)\s+(\w+)\s*=\s*session\s*\.\s*table\s*\(\s*["\']([^"\']+)["\']\s*\)'
        for match in re.finditer(table_pattern, code):
            var_name = match.group(1)
            table_name = match.group(2)
            
            self.data_sources[var_name] = DataSource(
                name=table_name,
                source_type=DataSourceType.TABLE,
                variable_name=var_name
            )
            self.variables[var_name] = table_name
            
            if self.debug:
                print(f"📊 Found table: {var_name} -> {table_name}")
        
        # Pattern: val df = session.sql("SELECT ...")
        sql_pattern = r'(?:val|var)\s+(\w+)\s*=\s*session\s*\.\s*sql\s*\(\s*["\']([^"\']+)["\']\s*\)'
        for match in re.finditer(sql_pattern, code):
            var_name = match.group(1)
            sql_query = match.group(2)
            
            self.data_sources[var_name] = DataSource(
                name=f"sql_query_{var_name}",
                source_type=DataSourceType.SQL_QUERY,
                sql_definition=sql_query,
                variable_name=var_name
            )
            
            if self.debug:
                print(f"📊 Found SQL query: {var_name}")
        
        # Pattern: df.createOrReplaceTempView("view_name")
        view_pattern = r'(\w+)\s*\.\s*createOrReplaceTempView\s*\(\s*["\']([^"\']+)["\']\s*\)'
        for match in re.finditer(view_pattern, code):
            source_var = match.group(1)
            view_name = match.group(2)
            
            self.data_sources[view_name] = DataSource(
                name=view_name,
                source_type=DataSourceType.VIEW,
                variable_name=view_name,
                dependencies=[source_var] if source_var in self.data_sources else []
            )
            
            if self.debug:
                print(f"📊 Found temp view: {view_name}")
    
    def _parse_all_queries(self, code: str) -> Dict[str, str]:
        """Parse all query definitions in the code"""
        queries = OrderedDict()
        
        # Pattern: val result = df.operation1().operation2()...
        query_pattern = r'(?:val|var)\s+(\w+)\s*=\s*([^=]+?)(?=(?:val|var|\Z))'
        
        for match in re.finditer(query_pattern, code):
            var_name = match.group(1)
            query_code = match.group(2).strip()
            
            # Skip simple assignments
            if not any(op in query_code for op in ['.select', '.filter', '.where', '.join', '.groupBy', '.agg']):
                continue
            
            try:
                # Reset context for each query
                self.context = SQLContext()
                
                # Parse the query chain
                self._parse_query_chain(query_code)
                
                # Build SQL
                sql = self._build_sql_from_context()
                
                queries[var_name] = sql
                
                if self.debug:
                    print(f"✅ Parsed query: {var_name}")
                
            except Exception as e:
                self.warnings.append(f"Failed to parse query '{var_name}': {str(e)}")
                if self.debug:
                    print(f"⚠️  Warning: {self.warnings[-1]}")
        
        return queries
    
    def _parse_query_chain(self, code: str):
        """Parse a complete query chain"""
        # Extract base source
        self._extract_base_source(code)
        
        # Extract operations
        operations = self._extract_operations(code)
        
        if self.debug:
            print(f"   Operations: {len(operations)}")
        
        # Process each operation
        for operation in operations:
            self._process_operation(operation)
    
    def _extract_base_source(self, code: str):
        """Extract the base data source"""
        # Check for variable reference
        var_match = re.match(r'(\w+)\s*\.', code)
        if var_match:
            var_name = var_match.group(1)
            if var_name in self.variables:
                self.context.from_table = self.variables[var_name]
                return
            elif var_name in self.data_sources:
                source = self.data_sources[var_name]
                if source.source_type == DataSourceType.SQL_QUERY:
                    self.context.from_table = f"({source.sql_definition})"
                else:
                    self.context.from_table = source.name
                return
        
        # Check for inline table reference
        table_match = re.search(r'session\s*\.\s*table\s*\(\s*["\']([^"\']+)["\']\s*\)', code)
        if table_match:
            self.context.from_table = table_match.group(1)
            return
        
        # Check for inline SQL
        sql_match = re.search(r'session\s*\.\s*sql\s*\(\s*["\']([^"\']+)["\']\s*\)', code)
        if sql_match:
            self.context.from_table = f"({sql_match.group(1)})"
            return
    
    def _extract_operations(self, code: str) -> List[Tuple[str, str]]:
        """Extract all chained operations"""
        operations = []
        pattern = r'\.(\w+)\s*\('
        
        pos = 0
        while True:
            match = re.search(pattern, code[pos:])
            if not match:
                break
            
            op_name = match.group(1)
            start_pos = pos + match.end()
            
            # Extract parameters
            params, end_pos = self._extract_params(code, start_pos - 1)
            
            operations.append((op_name, params))
            pos = start_pos + end_pos
        
        return operations
    
    def _extract_params(self, code: str, start: int) -> Tuple[str, int]:
        """Extract parameters handling nested structures"""
        if start >= len(code) or code[start] != '(':
            return '', 0
        
        depth = 0
        i = start
        in_string = False
        string_char = None
        escape = False
        
        while i < len(code):
            char = code[i]
            
            # Handle escapes
            if escape:
                escape = False
                i += 1
                continue
            
            if char == '\\':
                escape = True
                i += 1
                continue
            
            # Handle strings
            if char in ('"', "'", '`'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            # Count parentheses outside strings
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
        
        # Comprehensive handler mapping
        handler_map = {
            # Selection
            'select': self._handle_select,
            'selectExpr': self._handle_select_expr,
            
            # Filtering
            'filter': self._handle_filter,
            'where': self._handle_filter,
            
            # Joins
            'join': self._handle_join,
            'leftJoin': lambda p: self._handle_join(p, 'LEFT'),
            'rightJoin': lambda p: self._handle_join(p, 'RIGHT'),
            'fullJoin': lambda p: self._handle_join(p, 'FULL OUTER'),
            'innerJoin': lambda p: self._handle_join(p, 'INNER'),
            'crossJoin': lambda p: self._handle_join(p, 'CROSS'),
            
            # Aggregation
            'groupBy': self._handle_group_by,
            'agg': self._handle_agg,
            
            # Ordering
            'orderBy': self._handle_order_by,
            'sort': self._handle_order_by,
            
            # Limiting
            'limit': self._handle_limit,
            
            # Column operations
            'withColumn': self._handle_with_column,
            'withColumnRenamed': self._handle_with_column_renamed,
            'drop': self._handle_drop,
            'dropDuplicates': self._handle_drop_duplicates,
            
            # Set operations
            'distinct': self._handle_distinct,
            'union': self._handle_union,
            'unionAll': self._handle_union,
            'unionByName': self._handle_union,
            'intersect': self._handle_intersect,
            'except': self._handle_except,
            
            # Dataset operations
            'map': self._handle_map,
            'flatMap': self._handle_flat_map,
            'mapPartitions': self._handle_map_partitions,
            
            # Aliases
            'as': self._handle_alias,
            'alias': self._handle_alias,
            
            # Window
            'over': self._handle_over,
        }
        
        handler = handler_map.get(op_name)
        if handler:
            handler(params)
        elif self.debug:
            print(f"⚠️  Unhandled operation: {op_name}")
    
    # ============================================================================
    # OPERATION HANDLERS
    # ============================================================================
    
    def _handle_select(self, params: str):
        """Handle select operation"""
        columns = self._parse_columns(params)
        if columns:
            self.context.select_columns = columns
    
    def _handle_select_expr(self, params: str):
        """Handle selectExpr operation (SQL expressions as strings)"""
        expressions = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip().strip('"\'')
            expressions.append(part)
        
        if expressions:
            self.context.select_columns = expressions
    
    def _handle_filter(self, params: str):
        """Handle filter/where operation"""
        condition = self._parse_condition(params)
        if condition:
            self.context.where_conditions.append(condition)
    
    def _handle_join(self, params: str, join_type: str = 'INNER'):
        """Handle all join operations"""
        parts = self._smart_split(params, ',')
        
        if len(parts) < 1:
            return
        
        # Extract other table/dataframe
        other_source = self._extract_source_reference(parts[0].strip())
        
        # Default join condition
        join_condition = "1=1"  # Cross join default
        
        if len(parts) >= 2:
            # Join condition specified
            join_condition = self._parse_join_condition(parts[1].strip())
        
        # Override join type if specified as third parameter
        if len(parts) >= 3:
            join_type_param = parts[2].strip().strip('"\'').upper()
            if join_type_param in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'LEFT_OUTER', 'RIGHT_OUTER', 'FULL_OUTER'):
                join_type = join_type_param.replace('_OUTER', ' OUTER')
        
        join_clause = f"{join_type} JOIN {other_source} ON {join_condition}"
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
        limit_val = params.strip()
        # Handle both integer literals and expressions
        self.context.limit_value = limit_val
    
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
    
    def _handle_drop_duplicates(self, params: str):
        """Handle dropDuplicates operation"""
        if params.strip():
            # dropDuplicates with specific columns
            columns = self._parse_columns(params)
            # This would need ROW_NUMBER() window function
            self.warnings.append("dropDuplicates with specific columns requires window function - using DISTINCT instead")
            self.context.is_distinct = True
        else:
            # dropDuplicates without parameters = DISTINCT
            self.context.is_distinct = True
    
    def _handle_distinct(self, params: str):
        """Handle distinct operation"""
        self.context.is_distinct = True
    
    def _handle_union(self, params: str):
        """Handle union operations"""
        other_source = self._extract_source_reference(params.strip())
        self.context.union_queries.append(other_source)
    
    def _handle_intersect(self, params: str):
        """Handle intersect operation"""
        # Will be handled in final SQL building
        self.warnings.append("INTERSECT operation detected - ensure SQL compatibility")
    
    def _handle_except(self, params: str):
        """Handle except operation"""
        # Will be handled in final SQL building
        self.warnings.append("EXCEPT operation detected - ensure SQL compatibility")
    
    def _handle_map(self, params: str):
        """Handle map operation (Dataset-specific)"""
        # Map requires lambda function conversion - complex transformation
        self.warnings.append("map() operation detected - this requires manual conversion of lambda functions")
        
        # Try to extract simple transformations
        # Example: map(row => row.price * 1.1)
        lambda_match = re.search(r'(\w+)\s*=>\s*(.+)', params)
        if lambda_match:
            var_name = lambda_match.group(1)
            expression = lambda_match.group(2)
            
            # Simple field access and transformation
            if '*' in expression or '+' in expression or '-' in expression or '/' in expression:
                # This is a simple arithmetic expression
                expr_converted = expression.replace(f"{var_name}.", "")
                self.warnings.append(f"Detected map transformation: {expr_converted}")
    
    def _handle_flat_map(self, params: str):
        """Handle flatMap operation"""
        self.warnings.append("flatMap() operation detected - requires manual conversion")
    
    def _handle_map_partitions(self, params: str):
        """Handle mapPartitions operation"""
        self.warnings.append("mapPartitions() operation detected - requires manual conversion")
    
    def _handle_alias(self, params: str):
        """Handle alias/as operation"""
        alias = params.strip().strip('"\'')
        self.context.table_alias = alias
    
    def _handle_over(self, params: str):
        """Handle window function over operation"""
        # Parse window specification
        window_spec = self._parse_window_spec(params)
        if window_spec:
            # Store for use in final query
            self.context.window_specs['default'] = window_spec
    
    # ============================================================================
    # PARSING HELPERS
    # ============================================================================
    
    def _parse_columns(self, params: str) -> List[str]:
        """Parse column list from parameters"""
        if not params.strip():
            return []
        
        columns = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            # col("column_name") or col('column_name')
            col_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if col_match:
                columns.append(col_match.group(1))
                continue
            
            # $"column_name" (Scala string interpolation)
            dollar_match = re.match(r'\$"([^"]+)"', part)
            if dollar_match:
                columns.append(dollar_match.group(1))
                continue
            
            # df("column_name") or dataset("column_name")
            df_match = re.match(r'\w+\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if df_match:
                columns.append(df_match.group(1))
                continue
            
            # Simple string "column_name"
            string_match = re.match(r'["\']([^"\']+)["\']', part)
            if string_match:
                columns.append(string_match.group(1))
                continue
            
            # Expression with alias
            if ' as ' in part.lower() or '.as(' in part.lower():
                expr = self._parse_expression(part)
                columns.append(expr)
                continue
            
            # Complex expression (contains operators or functions)
            if any(op in part for op in ['(', '+', '-', '*', '/', '>', '<', '=']):
                expr = self._parse_expression(part)
                columns.append(expr)
                continue
            
            # Direct column reference
            if part and not part.startswith('('):
                columns.append(part)
        
        return columns
    
    def _parse_condition(self, condition: str) -> str:
        """Parse filter/where condition"""
        condition = condition.strip()
        
        # Remove outer parentheses
        while condition.startswith('(') and condition.endswith(')'):
            # Check if these are matching outer parens
            depth = 0
            matched = True
            for i, char in enumerate(condition[1:-1], 1):
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth < 0:
                        matched = False
                        break
            if matched and depth == 0:
                condition = condition[1:-1].strip()
            else:
                break
        
        # Convert col("name") to name
        condition = re.sub(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\1', condition)
        
        # Convert $"name" to name
        condition = re.sub(r'\$"([^"]+)"', r'\1', condition)
        
        # Convert df("name") to name
        condition = re.sub(r'\w+\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\1', condition)
        
        # Convert operators
        for scala_op, sql_op in self.operator_mappings.items():
            condition = condition.replace(scala_op, f' {sql_op} ')
        
        # Handle method calls
        condition = re.sub(r'(\w+)\.isNull\s*\(\s*\)', r'\1 IS NULL', condition)
        condition = re.sub(r'(\w+)\.isNotNull\s*\(\s*\)', r'\1 IS NOT NULL', condition)
        
        # Handle isin/contains
        condition = re.sub(
            r'(\w+)\.isin\s*\((.*?)\)',
            lambda m: f"{m.group(1)} IN ({m.group(2)})",
            condition
        )
        
        # Handle startsWith, endsWith, contains for strings
        condition = re.sub(
            r'(\w+)\.startsWith\s*\(\s*["\']([^"\']+)["\']\s*\)',
            lambda m: f"{m.group(1)} LIKE '{m.group(2)}%'",
            condition
        )
        
        condition = re.sub(
            r'(\w+)\.endsWith\s*\(\s*["\']([^"\']+)["\']\s*\)',
            lambda m: f"{m.group(1)} LIKE '%{m.group(2)}'",
            condition
        )
        
        condition = re.sub(
            r'(\w+)\.contains\s*\(\s*["\']([^"\']+)["\']\s*\)',
            lambda m: f"{m.group(1)} LIKE '%{m.group(2)}%'",
            condition
        )
        
        # Handle between
        condition = re.sub(
            r'(\w+)\.between\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            lambda m: f"{m.group(1)} BETWEEN {m.group(2)} AND {m.group(3)}",
            condition
        )
        
        # Clean up spaces
        condition = re.sub(r'\s+', ' ', condition)
        
        return condition.strip()
    
    def _parse_join_condition(self, condition: str) -> str:
        """Parse join condition"""
        condition = condition.strip()
        
        # Handle Seq("col1", "col2") - join on matching column names
        seq_match = re.match(r'Seq\s*\((.*?)\)', condition, re.IGNORECASE)
        if seq_match:
            cols = self._parse_columns(seq_match.group(1))
            return ' AND '.join([f"t1.{col} = t2.{col}" for col in cols])
        
        # Handle Array format
        array_match = re.match(r'Array\s*\((.*?)\)', condition, re.IGNORECASE)
        if array_match:
            cols = self._parse_columns(array_match.group(1))
            return ' AND '.join([f"t1.{col} = t2.{col}" for col in cols])
        
        # Parse as normal condition
        return self._parse_condition(condition)
    
    def _parse_aggregations(self, params: str) -> List[str]:
        """Parse aggregation functions"""
        aggregations = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            # Pattern: function(col("column")).as("alias")
            # Pattern: function(col("column")) as "alias"
            agg_pattern = r'(\w+)\s*\(\s*col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\)(?:\s*\.as\s*\(\s*["\']([^"\']+)["\']\s*\)|\s+as\s+["\']([^"\']+)["\'])?'
            
            match = re.match(agg_pattern, part)
            if match:
                func = match.group(1)
                col = match.group(2)
                alias = match.group(3) or match.group(4)
                
                sql_func = self.function_mappings.get(func, func.upper())
                
                # Handle countDistinct specially
                if func == 'countDistinct':
                    agg_expr = f"COUNT(DISTINCT {col})"
                else:
                    agg_expr = f"{sql_func}({col})"
                
                if alias:
                    aggregations.append(f"{agg_expr} AS {alias}")
                else:
                    aggregations.append(f"{agg_expr} AS {func}_{col}")
                continue
            
            # Try parsing as general expression
            expr = self._parse_expression(part)
            if expr:
                aggregations.append(expr)
        
        return aggregations
    
    def _parse_order_by_columns(self, params: str) -> List[str]:
        """Parse ORDER BY columns with direction"""
        columns = []
        parts = self._smart_split(params, ',')
        
        for part in parts:
            part = part.strip()
            
            # col("name").desc or col("name").desc()
            desc_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.desc\s*(?:\(\s*\))?', part)
            if desc_match:
                columns.append(f"{desc_match.group(1)} DESC")
                continue
            
            # col("name").asc or col("name").asc()
            asc_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.asc\s*(?:\(\s*\))?', part)
            if asc_match:
                columns.append(f"{asc_match.group(1)} ASC")
                continue
            
            # $"name".desc
            dollar_desc_match = re.match(r'\$"([^"]+)"\s*\.desc\s*(?:\(\s*\))?', part)
            if dollar_desc_match:
                columns.append(f"{dollar_desc_match.group(1)} DESC")
                continue
            
            # $"name".asc
            dollar_asc_match = re.match(r'\$"([^"]+)"\s*\.asc\s*(?:\(\s*\))?', part)
            if dollar_asc_match:
                columns.append(f"{dollar_asc_match.group(1)} ASC")
                continue
            
            # desc("name") or asc("name")
            func_match = re.match(r'(desc|asc)\s*\(\s*["\']([^"\']+)["\']\s*\)', part, re.IGNORECASE)
            if func_match:
                direction = func_match.group(1).upper()
                col = func_match.group(2)
                columns.append(f"{col} {direction}")
                continue
            
            # Standard col("name")
            col_match = re.match(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', part)
            if col_match:
                columns.append(col_match.group(1))
                continue
            
            # $"name"
            dollar_match = re.match(r'\$"([^"]+)"', part)
            if dollar_match:
                columns.append(dollar_match.group(1))
                continue
            
            # Plain column name or expression
            if part:
                columns.append(part)
        
        return columns
    
    def _parse_expression(self, expr: str) -> str:
        """Parse complex expressions"""
        expr = expr.strip()
        
        # Convert col references
        expr = re.sub(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', r'\1', expr)
        expr = re.sub(r'\$"([^"]+)"', r'\1', expr)
        
        # Convert lit() literals
        expr = re.sub(r'lit\s*\(\s*([^)]+)\s*\)', r'\1', expr)
        
        # Convert functions
        for scala_func, sql_func in self.function_mappings.items():
            # Match function calls
            expr = re.sub(
                f'\\b{scala_func}\\s*\\(',
                f'{sql_func}(',
                expr,
                flags=re.IGNORECASE
            )
        
        # Handle cast: col.cast("type") or cast(col, "type")
        expr = re.sub(
            r'(\w+)\.cast\s*\(\s*["\']?(\w+)["\']?\s*\)',
            lambda m: f"CAST({m.group(1)} AS {self.type_mappings.get(m.group(2), m.group(2))})",
            expr
        )
        
        # Handle .as("alias") or as "alias"
        as_match = re.search(
            r'(.+?)(?:\s*\.as\s*\(\s*["\']([^"\']+)["\']\s*\)|\s+as\s+["\']([^"\']+)["\'])\s*$',
            expr
        )
        if as_match:
            base_expr = as_match.group(1).strip()
            alias = (as_match.group(2) or as_match.group(3)).strip()
            expr = f"{base_expr} AS {alias}"
        
        # Convert operators
        for scala_op, sql_op in self.operator_mappings.items():
            expr = expr.replace(scala_op, f' {sql_op} ')
        
        # Handle when/otherwise (CASE statements)
        expr = self._convert_when_otherwise(expr)
        
        # Clean up spaces
        expr = re.sub(r'\s+', ' ', expr)
        
        return expr.strip()
    
    def _convert_when_otherwise(self, expr: str) -> str:
        """Convert when().otherwise() to CASE WHEN ... END"""
        
        # Pattern: when(condition, value).when(condition2, value2).otherwise(default)
        # This is a chain of when clauses
        
        # First, handle chained when clauses
        when_pattern = r'when\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        otherwise_pattern = r'\.otherwise\s*\(\s*([^)]+)\s*\)'
        
        # Find all when clauses
        when_matches = list(re.finditer(when_pattern, expr))
        otherwise_match = re.search(otherwise_pattern, expr)
        
        if when_matches:
            case_parts = ['CASE']
            
            for match in when_matches:
                condition = self._parse_condition(match.group(1))
                value = match.group(2).strip()
                case_parts.append(f"WHEN {condition} THEN {value}")
            
            if otherwise_match:
                else_value = otherwise_match.group(1).strip()
                case_parts.append(f"ELSE {else_value}")
            
            case_parts.append('END')
            
            # Replace the entire when/otherwise chain
            case_statement = ' '.join(case_parts)
            
            # Find the span of the entire when/otherwise expression
            start = when_matches[0].start()
            end = otherwise_match.end() if otherwise_match else when_matches[-1].end()
            
            expr = expr[:start] + case_statement + expr[end:]
        
        return expr
    
    def _parse_window_spec(self, params: str) -> str:
        """Parse window specification"""
        # Pattern: Window.partitionBy("col1", "col2").orderBy("col3")
        partition_match = re.search(r'partitionBy\s*\((.*?)\)', params)
        order_match = re.search(r'orderBy\s*\((.*?)\)', params)
        
        parts = []
        
        if partition_match:
            partition_cols = self._parse_columns(partition_match.group(1))
            parts.append(f"PARTITION BY {', '.join(partition_cols)}")
        
        if order_match:
            order_cols = self._parse_order_by_columns(order_match.group(1))
            parts.append(f"ORDER BY {', '.join(order_cols)}")
        
        return ' '.join(parts) if parts else ''
    
    def _extract_source_reference(self, ref: str) -> str:
        """Extract table/dataframe reference"""
        ref = ref.strip()
        
        # session.table("TABLE")
        table_match = re.search(r'session\s*\.\s*table\s*\(\s*["\']([^"\']+)["\']\s*\)', ref)
        if table_match:
            return table_match.group(1)
        
        # Variable reference
        if ref in self.variables:
            return self.variables[ref]
        
        if ref in self.data_sources:
            source = self.data_sources[ref]
            return source.name
        
        # Otherwise return as-is (might be a temp table name)
        return ref
    
    def _smart_split(self, text: str, delimiter: str, max_split: int = -1) -> List[str]:
        """Split respecting parentheses, brackets, and quotes"""
        parts = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        escape = False
        split_count = 0
        
        i = 0
        while i < len(text):
            char = text[i]
            
            # Handle escapes
            if escape:
                current.append(char)
                escape = False
                i += 1
                continue
            
            if char == '\\':
                current.append(char)
                escape = True
                i += 1
                continue
            
            # Handle strings
            if char in ('"', "'", '`'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                current.append(char)
                i += 1
                continue
            
            # Handle delimiters outside strings
            if not in_string:
                if char in '([{':
                    depth += 1
                elif char in ')]}':
                    depth -= 1
                elif char == delimiter and depth == 0:
                    if max_split == -1 or split_count < max_split:
                        parts.append(''.join(current).strip())
                        current = []
                        split_count += 1
                        i += 1
                        continue
            
            current.append(char)
            i += 1
        
        if current:
            parts.append(''.join(current).strip())
        
        return parts
    
    # ============================================================================
    # SQL BUILDING
    # ============================================================================
    
    def _build_sql_from_context(self) -> str:
        """Build SQL query from context"""
        sql_parts = []
        
        # SELECT clause
        select_clause = self._build_select_clause()
        sql_parts.append(select_clause)
        
        # FROM clause
        if self.context.from_table:
            from_clause = f"FROM {self.context.from_table}"
            if self.context.table_alias:
                from_clause += f" AS {self.context.table_alias}"
            sql_parts.append(from_clause)
        
        # JOIN clauses
        for join_clause in self.context.join_clauses:
            sql_parts.append(join_clause)
        
        # WHERE clause
        if self.context.where_conditions:
            conditions = [f"({cond})" for cond in self.context.where_conditions]
            where_clause = "WHERE " + " AND ".join(conditions)
            sql_parts.append(where_clause)
        
        # GROUP BY clause
        if self.context.group_by_columns:
            group_by_clause = "GROUP BY " + ", ".join(self.context.group_by_columns)
            sql_parts.append(group_by_clause)
        
        # HAVING clause
        if self.context.having_conditions:
            having_clause = "HAVING " + " AND ".join(self.context.having_conditions)
            sql_parts.append(having_clause)
        
        # UNION queries
        base_query = " ".join(sql_parts)
        
        if self.context.union_queries:
            union_parts = [base_query]
            for union_query in self.context.union_queries:
                union_parts.append(f"UNION ALL\n{union_query}")
            base_query = "\n".join(union_parts)
            sql_parts = [base_query]
        else:
            sql_parts = [base_query]
        
        # ORDER BY clause (after UNION if present)
        if self.context.order_by_columns:
            order_by_clause = "ORDER BY " + ", ".join(self.context.order_by_columns)
            sql_parts.append(order_by_clause)
        
        # LIMIT clause
        if self.context.limit_value:
            sql_parts.append(f"LIMIT {self.context.limit_value}")
        
        return " ".join(sql_parts)
    
    def _build_select_clause(self) -> str:
        """Build SELECT clause"""
        distinct = "DISTINCT " if self.context.is_distinct else ""
        
        columns = []
        
        # Determine columns based on context
        if self.context.aggregations:
            # Aggregation query: group by columns + aggregations
            columns = self.context.group_by_columns + self.context.aggregations
        elif self.context.with_columns:
            # withColumn operations
            if self.context.select_columns == ['*']:
                # Select all + new columns
                base_cols = ['*']
                for col_name, expr in self.context.with_columns.items():
                    base_cols.append(f"{expr} AS {col_name}")
                columns = base_cols
            else:
                # Specific columns + new columns
                columns = self.context.select_columns.copy()
                for col_name, expr in self.context.with_columns.items():
                    columns.append(f"{expr} AS {col_name}")
        else:
            columns = self.context.select_columns.copy()
        
        # Handle renamed columns
        if self.context.renamed_columns:
            new_columns = []
            for col in columns:
                # Check if this column should be renamed
                col_name = col.split()[0] if ' ' not in col or ' AS ' in col.upper() else col
                if col_name in self.context.renamed_columns:
                    if ' AS ' in col.upper():
                        # Already has alias, replace it
                        new_columns.append(f"{col_name} AS {self.context.renamed_columns[col_name]}")
                    else:
                        new_columns.append(f"{col} AS {self.context.renamed_columns[col_name]}")
                else:
                    new_columns.append(col)
            columns = new_columns
        
        # Handle dropped columns
        if self.context.dropped_columns and '*' not in columns:
            columns = [col for col in columns 
                      if not any(dropped in col for dropped in self.context.dropped_columns)]
        
        # Ensure at least something is selected
        if not columns:
            columns = ['*']
        
        return f"SELECT {distinct}{', '.join(columns)}"
    
    def _build_final_sql(self, queries: Dict[str, str]) -> str:
        """Build final SQL from all queries"""
        if not queries:
            return ""
        
        # If only one query, return it directly
        if len(queries) == 1:
            return list(queries.values())[0]
        
        # Multiple queries: use CTEs or separate statements
        # Build as CTEs for now
        cte_parts = []
        main_query = None
        
        for i, (name, sql) in enumerate(queries.items()):
            if i < len(queries) - 1:
                # Use as CTE
                cte_parts.append(f"{name} AS (\n{sql}\n)")
            else:
                # Last query is the main query
                main_query = sql
        
        if cte_parts and main_query:
            cte_clause = "WITH " + ",\n".join(cte_parts)
            return f"{cte_clause}\n{main_query}"
        elif main_query:
            return main_query
        else:
            # Return all queries separated
            return "\n\n-- Next Query\n\n".join(queries.values())
    
    def _format_sql(self, sql: str) -> str:
        """Format SQL for readability"""
        if not sql:
            return ""
        
        # Keywords that should start new lines
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR',
            'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN', 
            'INNER JOIN', 'CROSS JOIN',
            'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT',
            'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT',
            'WITH', 'AS'
        ]
        
        # Add line breaks before major keywords
        for keyword in keywords:
            sql = re.sub(
                f'\\b{keyword}\\b',
                f'\n{keyword}',
                sql,
                flags=re.IGNORECASE
            )
        
        # Clean up multiple newlines
        sql = re.sub(r'\n\s*\n', '\n', sql)
        
        # Format lines
        lines = sql.split('\n')
        formatted_lines = []
        indent_level = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Decrease indent for closing parens
            if line.startswith(')'):
                indent_level = max(0, indent_level - 1)
            
            # Add indentation
            formatted_line = '  ' * indent_level + line
            formatted_lines.append(formatted_line)
            
            # Increase indent for opening parens
            if line.endswith('(') or line.startswith('WITH') or line.startswith('SELECT'):
                indent_level += 1
            
            # Decrease indent after FROM, WHERE, etc.
            if any(line.startswith(kw) for kw in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY']):
                if indent_level > 0:
                    formatted_lines[-1] = '  ' * (indent_level - 1) + line
        
        return '\n'.join(formatted_lines)


# ============================================================================
# FILE PROCESSOR
# ============================================================================

class AdvancedFileProcessor:
    """Enhanced file processor with comprehensive features"""
    
    def __init__(self, converter: ComprehensiveSnowparkConverter):
        self.converter = converter
        self.results: List[ConversionResult] = []
    
    def read_file(self, file_path: str, encoding: str = 'utf-8') -> str:
        """Read file with encoding support"""
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Try to read with specified encoding
            try:
                with open(path, 'r', encoding=encoding) as f:
                    return f.read()
            except UnicodeDecodeError:
                # Fallback to latin-1
                with open(path, 'r', encoding='latin-1') as f:
                    return f.read()
                    
        except Exception as e:
            raise RuntimeError(f"Error reading file {file_path}: {str(e)}")
    
    def write_file(self, content: str, file_path: str, encoding: str = 'utf-8'):
        """Write file with encoding support"""
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding=encoding) as f:
                f.write(content)
            
            print(f"✅ SQL written to: {file_path}")
            
        except Exception as e:
            raise RuntimeError(f"Error writing file {file_path}: {str(e)}")
    
    def convert_file(self, input_file: str, output_file: Optional[str] = None) -> ConversionResult:
        """Convert single file"""
        start_time = datetime.now()
        
        try:
            # Read Scala code
            scala_code = self.read_file(input_file)
            line_count = len(scala_code.splitlines())
            
            print(f"\n📄 Processing: {input_file}")
            print(f"   Lines: {line_count}")
            
            # Convert
            result = self.converter.convert_advanced(scala_code)
            result.source_file = input_file
            
            # Write output
            if output_file and result.success:
                self.write_file(result.sql_code, output_file)
            
            # Print warnings
            if result.warnings:
                print(f"⚠️  Warnings: {len(result.warnings)}")
                for warning in result.warnings[:5]:  # Show first 5
                    print(f"   • {warning}")
                if len(result.warnings) > 5:
                    print(f"   • ... and {len(result.warnings) - 5} more")
            
            if result.success:
                print(f"✅ Conversion successful ({result.conversion_time:.2f}s)")
            else:
                print(f"❌ Conversion failed: {result.error_message}")
            
            self.results.append(result)
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
        """Convert all files in directory"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Directory not found: {input_dir}")
        
        # Find all files
        scala_files = list(input_path.rglob(pattern))
        
        if not scala_files:
            print(f"⚠️  No files matching '{pattern}' found in {input_dir}")
            return
        
        print(f"\n{'='*80}")
        print(f"📂 Found {len(scala_files)} file(s) in {input_dir}")
        print(f"{'='*80}")
        
        # Process each file
        for scala_file in scala_files:
            relative_path = scala_file.relative_to(input_path)
            output_file = output_path / relative_path.with_suffix('.sql')
            
            self.convert_file(str(scala_file), str(output_file))
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print detailed summary"""
        if not self.results:
            return
        
        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful
        total_time = sum(r.conversion_time for r in self.results)
        total_lines = sum(r.line_count for r in self.results)
        total_warnings = sum(len(r.warnings) for r in self.results)
        
        print(f"\n{'='*80}")
        print("📊 CONVERSION SUMMARY")
        print(f"{'='*80}")
        print(f"Total files:        {len(self.results)}")
        print(f"✅ Successful:      {successful}")
        print(f"❌ Failed:          {failed}")
        print(f"📝 Total lines:     {total_lines:,}")
        print(f"⚠️  Total warnings:  {total_warnings}")
        print(f"⏱️  Total time:      {total_time:.2f}s")
        if successful > 0:
            print(f"⚡ Avg time/file:   {total_time/len(self.results):.2f}s")
        print(f"{'='*80}")
        
        if failed > 0:
            print("\n❌ Failed conversions:")
            for result in self.results:
                if not result.success:
                    print(f"   • {result.source_file}")
                    print(f"     Error: {result.error_message}")
    
    def save_report(self, report_file: str):
        """Save detailed JSON report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_files': len(self.results),
                'successful': sum(1 for r in self.results if r.success),
                'failed': sum(1 for r in self.results if not r.success),
                'total_lines': sum(r.line_count for r in self.results),
                'total_warnings': sum(len(r.warnings) for r in self.results),
                'total_time': sum(r.conversion_time for r in self.results),
            },
            'results': []
        }
        
        for result in self.results:
            report['results'].append({
                'source_file': result.source_file,
                'success': result.success,
                'line_count': result.line_count,
                'conversion_time': result.conversion_time,
                'warnings': result.warnings,
                'error_message': result.error_message,
                'data_sources': {
                    name: {
                        'type': ds.source_type.value,
                        'name': ds.name
                    }
                    for name, ds in result.data_sources.items()
                },
                'num_queries': len(result.queries)
            })
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Detailed report saved to: {report_file}")


# ============================================================================
# SAMPLE FILES GENERATOR
# ============================================================================

def create_comprehensive_samples(output_dir: str = "./samples"):
    """Create comprehensive sample Scala files"""
    
    samples = {
        "basic_operations.scala": '''
// Basic DataFrame operations
val customers = session.table("CUSTOMERS")
  .select(col("customer_id"), col("name"), col("email"), col("region"))
  .filter(col("status") === "active")
  .filter(col("region").isin("US", "EU", "ASIA"))
  .orderBy(col("name").asc)
  .limit(1000)
''',
        
        "joins_aggregations.scala": '''
// Joins and aggregations
val orderSummary = session.table("ORDERS")
  .join(session.table("CUSTOMERS"), 
        col("orders.customer_id") === col("customers.customer_id"), 
        "inner")
  .join(session.table("PRODUCTS"),
        col("orders.product_id") === col("products.product_id"),
        "left")
  .groupBy(col("customer_id"), col("customer_name"), col("region"))
  .agg(
    sum(col("order_amount")).as("total_spent"),
    avg(col("order_amount")).as("avg_order"),
    count(col("order_id")).as("order_count"),
    countDistinct(col("product_id")).as("unique_products")
  )
  .filter(col("total_spent") > 10000)
  .orderBy(col("total_spent").desc)
''',
        
        "complex_transformations.scala": '''
// Complex transformations with window functions
val rankedSales = session.table("SALES")
  .withColumn("total_amount", col("quantity") * col("unit_price"))
  .withColumn("discount_amount", col("total_amount") * col("discount_rate"))
  .withColumn("final_amount", col("total_amount") - col("discount_amount"))
  .withColumn("sales_rank", 
    row_number().over(Window.partitionBy("region").orderBy(col("final_amount").desc)))
  .withColumn("running_total",
    sum(col("final_amount")).over(Window.partitionBy("region").orderBy("sale_date")))
  .filter(col("sales_rank") <= 10)
  .select(
    col("region"),
    col("product_name"),
    col("final_amount"),
    col("sales_rank"),
    col("running_total")
  )
''',
        
        "case_when_expressions.scala": '''
// CASE WHEN expressions and conditional logic
val customerSegmentation = session.table("CUSTOMERS")
  .withColumn("segment",
    when(col("total_purchases") > 100000, "Premium")
      .when(col("total_purchases") > 50000, "Gold")
      .when(col("total_purchases") > 10000, "Silver")
      .otherwise("Standard"))
  .withColumn("risk_level",
    when(col("days_since_last_order") > 365, "High")
      .when(col("days_since_last_order") > 180, "Medium")
      .otherwise("Low"))
  .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
  .select(
    col("customer_id"),
    col("full_name"),
    col("segment"),
    col("risk_level"),
    col("total_purchases")
  )
  .orderBy(col("total_purchases").desc)
''',
        
        "dataset_operations.scala": '''
// Dataset-specific operations
val productDataset = session.table("PRODUCTS")
  .select(col("product_id"), col("product_name"), col("category"), col("price"))
  .filter(col("price").isNotNull)
  .filter(col("category").isNotNull)
  .withColumnRenamed("product_id", "id")
  .withColumnRenamed("product_name", "name")
  .drop("internal_code", "legacy_id")
  .distinct()
''',
        
        "multiple_queries.scala": '''
// Multiple related queries with CTEs
val activeCust = session.table("CUSTOMERS")
  .filter(col("status") === "active")
  .select(col("customer_id"), col("customer_name"))

val recentOrders = session.table("ORDERS")
  .filter(col("order_date") >= current_date() - 30)
  .select(col("order_id"), col("customer_id"), col("amount"))

val result = activeCust
  .join(recentOrders, Seq("customer_id"), "inner")
  .groupBy(col("customer_id"), col("customer_name"))
  .agg(
    sum(col("amount")).as("total_amount"),
    count(col("order_id")).as("order_count")
  )
  .orderBy(col("total_amount").desc)
  .limit(100)
''',
        
        "advanced_functions.scala": '''
// Advanced SQL functions
val dataQuality = session.table("RAW_DATA")
  .withColumn("email_clean", lower(trim(col("email"))))
  .withColumn("name_formatted", initcap(col("full_name")))
  .withColumn("phone_digits", regexp_replace(col("phone"), "[^0-9]", ""))
  .withColumn("created_year", year(col("created_date")))
  .withColumn("created_month", month(col("created_date")))
  .withColumn("days_active", datediff(current_date(), col("created_date")))
  .withColumn("amount_rounded", round(col("amount"), 2))
  .withColumn("hash_value", md5(concat(col("id"), col("email"))))
  .filter(col("email_clean").contains("@"))
  .filter(col("phone_digits").length === 10)
'''
    }
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Write sample files
    for filename, content in samples.items():
        file_path = output_path / filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
        print(f"✅ Created: {file_path}")
    
    print(f"\n📁 Created {len(samples)} sample files in {output_dir}")


# ============================================================================
# MAIN CLI
# ============================================================================

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='Comprehensive Snowpark Scala to Snowflake SQL Converter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Convert single file
  python converter.py -i query.scala -o query.sql
  
  # Convert directory
  python converter.py -d ./scala_files -o ./sql_output
  
  # Convert with debug mode
  python converter.py -i query.scala -o query.sql --debug
  
  # Create sample files
  python converter.py --create-samples ./samples
  
  # Generate detailed report
  python converter.py -d ./scala -o ./sql --report report.json
        '''
    )
    
    parser.add_argument('-i', '--input', help='Input Scala file')
    parser.add_argument('-o', '--output', help='Output SQL file or directory')
    parser.add_argument('-d', '--directory', help='Input directory')
    parser.add_argument('-m', '--multiple', nargs='+', help='Multiple input files')
    parser.add_argument('-p', '--pattern', default='*.scala', help='File pattern (default: *.scala)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--create-samples', metavar='DIR', help='Create sample Scala files')
    parser.add_argument('--report', help='Save detailed report to JSON')
    parser.add_argument('--encoding', default='utf-8', help='File encoding (default: utf-8)')
    
    args = parser.parse_args()
    
    # Create samples
    if args.create_samples:
        create_comprehensive_samples(args.create_samples)
        return
    
    # Initialize converter
    converter = ComprehensiveSnowparkConverter(debug=args.debug)
    processor = AdvancedFileProcessor(converter)
    
    try:
        # Single file
        if args.input:
            result = processor.convert_file(args.input, args.output)
            if not args.output:
                print(f"\n{'='*80}")
                print("📄 CONVERTED SQL:")
                print(f"{'='*80}")
                print(result.sql_code)
                print(f"{'='*80}")
        
        # Directory
        elif args.directory:
            if not args.output:
                print("Error: Output directory (-o) required for directory mode")
                sys.exit(1)
            processor.convert_directory(args.directory, args.output, args.pattern)
        
        # Multiple files
        elif args.multiple:
            for input_file in args.multiple:
                if args.output:
                    output_file = str(Path(args.output) / (Path(input_file).stem + '.sql'))
                else:
                    output_file = None
                processor.convert_file(input_file, output_file)
            processor.print_summary()
        
        else:
            parser.print_help()
            sys.exit(0)
        
        # Save report
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
