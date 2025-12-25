
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snowpark (Scala) -> Snowflake SQL converter (heuristic)

Features:
- session.table("SCHEMA.TABLE") or val df = session.sql("...")
- select(), filter(), groupBy().agg(), orderBy(), limit()
- join(df, condition, "inner"/"left"/"right"/"full")
- withColumn(name, expr)
- withColumnRenamed(old, new)
- drop("c1","c2",...) or drop(col("B"), "C")
- Inlines withColumn aliases in WHERE (filters)

Notes:
- Heuristic (best-effort) parser; no schema inference.
- drop/rename can fully apply only to explicit projections; cannot expand *
- Extend FUNCTION_MAP / regexes for more coverage.

Usage:
    python snowpark_scala_to_sql.py input.scala [-o output.sql]
"""

import re
import sys
import argparse
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple

# ---------------------------
# Utilities
# ---------------------------

def strip_comments(code: str) -> str:
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.S)
    code = re.sub(r"//.*?$", "", code, flags=re.M)
    return code

def strip_outer_parens(s: str) -> str:
    s = s.strip()
    if s.startswith("(") and s.endswith(")"):
        depth = 0
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if depth == 0 and i != len(s) - 1:
                break
        else:
            return s[1:-1].strip()
    return s

def split_top_level_args(args: str) -> List[str]:
    res = []
    buf = []
    depth = 0
    in_str = False
    str_char = None
    for ch in args:
        if in_str:
            buf.append(ch)
            if ch == str_char:
                in_str = False
            elif ch == "\\":
                pass
            continue
        if ch in ("'", '"'):
            in_str = True
            str_char = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth -= 1
            buf.append(ch)
            continue
        if ch == "," and depth == 0:
            item = "".join(buf).strip()
            if item:
                res.append(item)
            buf = []
            continue
        buf.append(ch)
    item = "".join(buf).strip()
    if item:
        res.append(item)
    return res

def unquote(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

# ---------------------------
# Expression translation (robust global replace)
# ---------------------------

def _lit_to_sql(raw: str) -> str:
    raw = raw.strip()
    # numeric?
    if re.match(r"^-?\d+(\.\d+)?$", raw):
        return raw
    # quoted string?
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        return "'" + raw[1:-1].replace("'", "''") + "'"
    # bare token -> quote it
    return "'" + raw.replace("'", "''") + "'"

def replace_operators(s: str) -> str:
    return (
        s.replace("===", " = ")
         .replace("==", " = ")
         .replace("=!=", " <> ")
         .replace("!=", " <> ")
         .replace("&&", " AND ")
         .replace("||", " OR ")
    )

def _replace_cols_and_lits(s: str, var_to_alias: Dict[str, str]) -> str:
    # df.col("X") -> df_alias.X
    def repl_df_col(m):
        var = m.group(1)
        arg = m.group(2)
        alias = var_to_alias.get(var, var)
        return f"{alias}.{unquote(arg)}"
    s = re.sub(r"([a-zA-Z_]\w*)\.col\(([^()]+?)\)", repl_df_col, s)

    # col("X") -> X
    s = re.sub(r"col\(([^()]+?)\)", lambda m: unquote(m.group(1)), s)

    # lit(x) -> 'x' or numeric
    s = re.sub(r"lit\(([^()]+?)\)", lambda m: _lit_to_sql(m.group(1)), s)

    return s

def _replace_predicates_global(s: str) -> str:
    # .like('x%') etc.
    def repl_like(m):
        base = m.group(1)
        pat = "'" + unquote(m.group(2)).replace("'", "''") + "'"
        return f"{base} LIKE {pat}"
    s = re.sub(r"(\S(?:.*?\S)?)\.like\(([^()]+?)\)", repl_like, s)

    def repl_starts(m):
        base = m.group(1)
        val = unquote(m.group(2)).replace("'", "''")
        return f"{base} LIKE '{val}%'"
    s = re.sub(r"(\S(?:.*?\S)?)\.startsWith\(([^()]+?)\)", repl_starts, s)

    def repl_ends(m):
        base = m.group(1)
        val = unquote(m.group(2)).replace("'", "''")
        return f"{base} LIKE '%{val}'"
    s = re.sub(r"(\S(?:.*?\S)?)\.endsWith\(([^()]+?)\)", repl_ends, s)

    def repl_contains(m):
        base = m.group(1)
        val = unquote(m.group(2)).replace("'", "''")
        return f"{base} LIKE '%{val}%'"
    s = re.sub(r"(\S(?:.*?\S)?)\.contains\(([^()]+?)\)", repl_contains, s)

    # null predicates
    s = re.sub(r"\b\.isNull\b", " IS NULL", s)
    s = re.sub(r"\b\.isNotNull\b", " IS NOT NULL", s)

    # order hints
    s = re.sub(r"\b\.asc\b", " ASC", s)
    s = re.sub(r"\b\.desc\b", " DESC", s)
    return s

def _uppercase_known_functions(s: str) -> str:
    # Simple casing for common functions
    funcs = ["lower", "upper", "substr", "coalesce", "sum", "avg", "min", "max", "count"]
    for f in funcs:
        s = re.sub(rf"\b{f}\s*\(", f.upper() + "(", s)
    # countDistinct(x) -> COUNT(DISTINCT x)
    def repl_cd(m):
        inner = m.group(1)
        return f"COUNT(DISTINCT {inner})"
    s = re.sub(r"\bcountDistinct\s*\((.*?)\)", repl_cd, s)
    return s

def translate_expression(expr: str, var_to_alias: Dict[str, str]) -> str:
    e = expr.strip()
    e = strip_outer_parens(e)

    # Replace col/df.col/lit globally (repeat until stable for nested cases)
    prev = None
    while prev != e:
        prev = e
        e = _replace_cols_and_lits(e, var_to_alias)

    # ops, predicates, casing
    e = replace_operators(e)
    e = _replace_predicates_global(e)
    e = _uppercase_known_functions(e)
    return e.strip()

def inline_aliases_in_sql(sql: str, alias_exprs: Dict[str, str]) -> str:
    """
    Replace bare identifiers in SQL text with their expressions, avoiding replacements inside quotes.
    Example: WHERE A2 > 10, alias_exprs={'A2': 'A + 1'} -> WHERE (A + 1) > 10
    """
    if not sql or not alias_exprs:
        return sql
    segments = re.split(r"('(?:''|[^'])*')", sql)  # split at quoted strings, keep delimiters
    items = sorted(alias_exprs.items(), key=lambda kv: -len(kv[0]))  # longest-first
    for i in range(0, len(segments), 2):
        seg = segments[i]
        for alias, expr in items:
            seg = re.sub(rf"\b{re.escape(alias)}\b", f"({expr})", seg)
        segments[i] = seg
    return "".join(segments)

# ---------------------------
# IR & SQL Builder
# ---------------------------

@dataclass
class JoinSpec:
    join_type: str
    right_table: str
    right_alias: str
    condition_sql: str

@dataclass
class QueryIR:
    from_table: str = ""
    from_alias: str = ""
    selects: List[Tuple[str, Optional[str]]] = field(default_factory=list)  # (expr, alias)
    where: Optional[str] = None
    joins: List[JoinSpec] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[str] = field(default_factory=list)
    limit: Optional[int] = None

    with_columns: List[Tuple[str, str]] = field(default_factory=list)  # [(alias, expr_sql)]
    rename_map: List[Tuple[str, str]] = field(default_factory=list)    # [(old, new)]
    drop_columns: List[str] = field(default_factory=list)               # names to drop

    def _apply_renames_to_projection(self, proj: List[Tuple[str, Optional[str]]]) -> List[Tuple[str, Optional[str]]]:
        if not self.rename_map:
            return proj
        out = []
        rename_dict = dict(self.rename_map)
        for expr, alias in proj:
            if alias and alias in rename_dict:
                out.append((expr, rename_dict[alias]))
            else:
                # bare column rename (expr is just an identifier)
                if not alias and re.match(r"^[A-Za-z_]\w*$", expr) and expr in rename_dict:
                    out.append((expr, rename_dict[expr]))
                else:
                    out.append((expr, alias))
        return out

    def _apply_drop_to_projection(self, proj: List[Tuple[str, Optional[str]]]) -> List[Tuple[str, Optional[str]]]:
        if not self.drop_columns:
            return proj
        drop_set = set(self.drop_columns)
        out = []
        for expr, alias in proj:
            if alias and alias in drop_set:
                continue
            if not alias and re.match(r"^[A-Za-z_]\w*$", expr) and expr in drop_set:
                continue
            out.append((expr, alias))
        return out

    def _render_select_list(self) -> List[str]:
        """
        Decide final SELECT projection with selects, with_columns, renames, drops.
        """
        if not self.selects:
            # SELECT * baseline; we cannot drop/rename unknown star columns.
            parts = ["*"]
            wc = list(self.with_columns)
            # Apply rename to withColumns aliases
            rename_dict = dict(self.rename_map)
            wc = [(rename_dict.get(alias, alias), expr) for alias, expr in wc]
            drop_set = set(self.drop_columns)
            for alias, expr in wc:
                if alias in drop_set:
                    continue
                parts.append(f"{expr} AS {alias}")
            return parts

        # Start from explicit selects
        final: List[Tuple[str, Optional[str]]] = list(self.selects)

        # Apply renames and drops to the explicit projection
        final = self._apply_renames_to_projection(final)
        final = self._apply_drop_to_projection(final)

        # Merge withColumns (replace by alias if exists; otherwise append)
        for w_alias, w_expr in self.with_columns:
            # apply rename target
            for old, new in self.rename_map:
                if w_alias == old:
                    w_alias = new
            if w_alias in set(self.drop_columns):
                continue
            replaced = False
            for idx, (expr, alias) in enumerate(final):
                if alias and alias == w_alias:
                    final[idx] = (w_expr, w_alias)
                    replaced = True
                    break
                if not alias and re.match(r"^[A-Za-z_]\w*$", expr) and expr == w_alias:
                    final[idx] = (w_expr, w_alias)
                    replaced = True
                    break
            if not replaced:
                final.append((w_expr, w_alias))

        # Format projection
        out = []
        for expr, alias in final:
            out.append(f"{expr} AS {alias}" if alias else expr)
        return out

    def to_sql(self) -> str:
        if not self.from_table:
            return "-- Unable to determine FROM source."

        lines = []
        # SELECT
        sel_parts = self._render_select_list()
        lines.append("SELECT " + ", ".join(sel_parts) if sel_parts else "SELECT *")

        # FROM
        lines.append(f"FROM {self.from_table}" + (f" {self.from_alias}" if self.from_alias else ""))

        # JOINs
        for j in self.joins:
            lines.append(f"{j.join_type} JOIN {j.right_table} {j.right_alias} ON {j.condition_sql}")

        # WHERE (inline withColumn aliases)
        if self.where:
            alias_map = {alias: expr for alias, expr in self.with_columns}
            inlined_where = inline_aliases_in_sql(self.where, alias_map)
            lines.append(f"WHERE {inlined_where}")

        # GROUP BY
        if self.group_by:
            lines.append("GROUP BY " + ", ".join(self.group_by))

        # ORDER BY
        if self.order_by:
            lines.append("ORDER BY " + ", ".join(self.order_by))

        # LIMIT
        if self.limit is not None:
            lines.append(f"LIMIT {self.limit}")

        return "\n".join(lines) + ";"

# ---------------------------
# Scala parser (heuristic)
# ---------------------------

class SnowparkScalaToSQLConverter:
    def __init__(self, scala_code: str):
        self.code = strip_comments(scala_code)
        self.var_to_table: Dict[str, str] = {}
        self.var_to_alias: Dict[str, str] = {}

    def _discover_tables(self):
        # val sales = session.table("SCHEMA.TABLE")
        for m in re.finditer(r"""val\s+([a-zA-Z_]\w*)\s*=\s*session\.table\((.+?)\)""", self.code):
            var = m.group(1)
            table = unquote(m.group(2))
            alias = self._alias_from_table(table) or var
            self.var_to_table[var] = table
            self.var_to_alias[var] = alias

        # val x = session.sql("select ...")
        for m in re.finditer(r"""val\s+([a-zA-Z_]\w*)\s*=\s*session\.sql\((.+?)\)""", self.code):
            var = m.group(1)
            sql = unquote(m.group(2))
            alias = var
            self.var_to_table[var] = f"({sql})"
            self.var_to_alias[var] = alias

    @staticmethod
    def _alias_from_table(table: str) -> str:
        t = table.strip().strip('"')
        if "." in t:
            return t.split(".")[-1]
        return t

    def _find_main_chain(self) -> Optional[str]:
        # Capture RHS of the LAST val-assignment block up to next 'val ' or EOF (handles multiline).
        pattern = re.compile(r"val\s+([a-zA-Z_]\w*)\s*=\s*(.+?)(?=\n\s*val\s+|\Z)", re.S)
        matches = list(pattern.finditer(self.code))
        if matches:
            rhs = matches[-1].group(2).strip()
            return rhs
        # Fallback: direct chain without val
        m2 = re.search(r"""session\.table\((.+?)\)(?:\.[a-zA-Z_]\w*\(.*?\))+""", self.code, flags=re.S)
        if m2:
            return m2.group(0)
        return None

    def convert(self) -> str:
        self._discover_tables()
        chain = self._find_main_chain()
        if not chain:
            return "-- No Snowpark chain found."

        ir = QueryIR()

        # Base FROM
        m_base_var = re.match(r"""([a-zA-Z_]\w*)""", chain.strip())
        if m_base_var and m_base_var.group(1) in self.var_to_table:
            base_var = m_base_var.group(1)
            ir.from_table = self.var_to_table[base_var]
            ir.from_alias = self.var_to_alias.get(base_var, base_var)
        else:
            m_base_table = re.search(r"""session\.table\((.+?)\)""", chain)
            if m_base_table:
                ir.from_table = unquote(m_base_table.group(1))
                ir.from_alias = self._alias_from_table(ir.from_table)

        var_to_alias = dict(self.var_to_alias)

        # Parse top-level method calls in order
        methods = re.findall(r"""\.([a-zA-Z_]\w*)\((.*?)\)""", chain, flags=re.S)
        for (method, args_str) in methods:
            method = method.strip()
            args_str = args_str.strip()

            if method == "join":
                args = split_top_level_args(args_str)
                if len(args) >= 3:
                    right_df = args[0].strip()
                    cond = args[1].strip()
                    jtype = unquote(args[2]).upper()
                    join_type = {"INNER": "INNER", "LEFT": "LEFT", "RIGHT": "RIGHT", "FULL": "FULL"}.get(jtype, "INNER")

                    if right_df in self.var_to_table:
                        r_table = self.var_to_table[right_df]
                        r_alias = self.var_to_alias.get(right_df, right_df)
                    elif right_df.startswith('session.table('):
                        rt = re.search(r'session\.table\((.+?)\)', right_df)
                        r_table = unquote(rt.group(1)) if rt else right_df
                        r_alias = self._alias_from_table(r_table)
                    else:
                        r_table = unquote(right_df)
                        r_alias = self._alias_from_table(r_table)

                    var_to_alias[right_df] = r_alias
                    cond_sql = translate_expression(cond, var_to_alias)
                    ir.joins.append(JoinSpec(
                        join_type=f"{join_type}",
                        right_table=r_table,
                        right_alias=r_alias,
                        condition_sql=cond_sql
                    ))

            elif method == "filter":
                expr = translate_expression(args_str, var_to_alias)
                if ir.where:
                    ir.where = f"({ir.where}) AND ({expr})"
                else:
                    ir.where = expr

            elif method == "select":
                items = split_top_level_args(args_str)
                for it in items:
                    alias = None
                    m_as = re.match(r"""(.+?)\.(?:as|alias)\((.+?)\)$""", it.strip())
                    if m_as:
                        base = m_as.group(1).strip()
                        alias = unquote(m_as.group(2))
                        base_sql = translate_expression(base, var_to_alias)
                        ir.selects.append((base_sql, alias))
                    else:
                        base_sql = translate_expression(it.strip(), var_to_alias)
                        ir.selects.append((base_sql, None))

            elif method == "groupBy":
                keys = split_top_level_args(args_str)
                ir.group_by = [translate_expression(k, var_to_alias) for k in keys]

            elif method == "agg":
                aggs = split_top_level_args(args_str)
                for agg in aggs:
                    alias = None
                    m_as = re.match(r"""(.+?)\.(?:as|alias)\((.+?)\)$""", agg.strip())
                    if m_as:
                        base = m_as.group(1).strip()
                        alias = unquote(m_as.group(2))
                        base_sql = translate_expression(base, var_to_alias)
                        ir.selects.append((base_sql, alias))
                    else:
                        base_sql = translate_expression(agg.strip(), var_to_alias)
                        ir.selects.append((base_sql, None))

            elif method == "orderBy":
                items = split_top_level_args(args_str)
                for it in items:
                    part = translate_expression(it.strip(), var_to_alias)
                    ir.order_by.append(part)

            elif method == "limit":
                try:
                    ir.limit = int(unquote(args_str))
                except Exception:
                    pass

            elif method == "withColumn":
                args = split_top_level_args(args_str)
                if len(args) == 2:
                    alias = unquote(args[0])
                    expr_sql = translate_expression(args[1], var_to_alias)
                    ir.with_columns.append((alias, expr_sql))

            elif method == "withColumnRenamed":
                args = split_top_level_args(args_str)
                if len(args) == 2:
                    old = unquote(args[0])
                    new = unquote(args[1])
                    ir.rename_map.append((old, new))

            elif method == "drop":
                args = split_top_level_args(args_str)
                for a in args:
                    a_str = a.strip()
                    if re.match(r"""^['"].+?['"]$""", a_str):
                        ir.drop_columns.append(unquote(a_str))
                    else:
                        m = re.match(r"""col\((.+?)\)$""", a_str)
                        if m:
                            ir.drop_columns.append(unquote(m.group(1)))
                        else:
                            ir.drop_columns.append(unquote(a_str))

            # (extend: distinct, having, etc.)

        return ir.to_sql()

# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Convert Scala Snowpark code to Snowflake SQL (heuristic).")
    ap.add_argument("input", help="Path to Scala file")
    ap.add_argument("-o", "--output", help="Write SQL to this file")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        code = f.read()

    converter = SnowparkScalaToSQLConverter(code)
    sql = converter.convert()

    if args.output:
               with open(args.output, "w", encoding="utf-8") as f:
            f.write(sql + "\n")
        print(f"SQL written to {args.output}")
    else:
        print(sql)

if __name__ == "__main__":
