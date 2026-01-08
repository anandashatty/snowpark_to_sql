import re

def convert_scala_dataset_to_sql(scala_code):
    """
    Parses Scala Spark Dataset transformations and transpiles them to Snowflake SQL.
    """
    # 1. Map Spark DSL Date Logic to Snowflake SQL
    # Replaces .lastDayOfMonthFromYYYYMM with Snowflake's native LAST_DAY
    scala_code = re.sub(
        r"(\w+)\.load_table\.col\.lastDayOfMonthFromYYYYMM", 
        r"LAST_DAY(TO_DATE(\1.load_table, 'YYYYMM'))", 
        scala_code
    )

    # 2. Map Filter logic
    # Replaces .filter(col.isNotEmpty) with standard SQL WHERE
    scala_code = re.sub(
        r"\.filter\((.*?)\.col\.isNotEmpty\)", 
        r"WHERE \1 IS NOT NULL AND \1 <> ''", 
        scala_code
    )

    # 3. Extract logic blocks defined as Scala functions (def create...)
    # Each function becomes a Common Table Expression (CTE) in Snowflake
    cte_blocks = []
    function_pattern = r"def (\w+)\(.*?\).*? = \{(.*?)\n  \}"
    functions = re.findall(function_pattern, scala_code, re.DOTALL)

    for func_name, body in functions:
        cte_name = func_name.replace("create", "")
        
        # Determine the base table (first word in the method chain)
        source_match = re.search(r"^\s*(\w+)", body, re.MULTILINE)
        source_table = source_match.group(1) if source_match else "source"
        
        # Translate Join logic: Spark .join(..., Seq("col1")) -> SQL JOIN ... ON
        join_match = re.search(r"\.join\((\w+), Seq\((.*?)\)\)", body)
        join_sql = ""
        if join_match:
            target, cols = join_match.group(1), join_match.group(2).replace('"', '')
            on_clause = " AND ".join([f"a.{c.strip()} = b.{c.strip()}" for c in cols.split(",")])
            join_sql = f"JOIN {target} b ON {on_clause}"

        # Construct the SQL snippet
        block = f"{cte_name} AS (\n  SELECT *\n  FROM {source_table} a\n  {join_sql}\n)"
        cte_blocks.append(block)

    # Final SQL Assembly
    if not cte_blocks:
        return "-- No valid Scala Dataset patterns found for conversion."
        
    return "WITH " + ",\n".join(cte_blocks) + "\nSELECT * FROM " + cte_blocks[-1].split(" ")[0] + ";"

# --- Example Execution ---
scala_input = """
  private[account] def createFilteredIdsTopAccount(
      accountView: Dataset[AccountView],
      topAccountView: Dataset[TopAccountView]
  ): Dataset[FilteredAccountView] = {
    accountView
      .filter(AccountView.pr70.col.isNotEmpty)
      .select(AccountView.pr213.col, AccountView.load_table.col)
      .join(distinctTopAccountView, Seq("pr70", "load_table"))
  }
"""

print(convert_scala_dataset_to_sql(scala_input))
