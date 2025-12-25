
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snowpark (Scala) -> Snowflake SQL converter (heuristic, DF-friendly)

Supports:
- DataFrame variables: resolves the base source by walking val-assignments backward.
- session.table("SCHEMA.TABLE"), session.sql("...") as sources.
- select(), selectExpr(), filter()/where(), groupBy().agg(), orderBy(), limit()
- join(otherDf, condition, "inner"/"left"/"right"/"full")
- join(otherDf, Seq("K1","K2"), "inner")  # key-based join
- withColumn(name, expr), withColumnRenamed(old, new), drop(...)
- Inlines withColumn aliases in WHERE.

Notes/limits:
- Heuristic: no schema inference; cannot expand SELECT * for drop/rename.
- If you use very custom Scala patterns (UDFs, lambdas), please share a snippet.

Usage:
    python snowpark_scala_to_sql.py input.scala [-o output.sql] [--debug]
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

def preprocess_scala(code: str) -> str:
    """
    Normalize common Scala idioms to patterns this parser handles better:
      - $"COL" -> col("COL")
    """
    code = re.sub(r'\$\s*"([^"]+)"', r'col("\1")', code)
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
    res: List[str] = []
    buf: List[str] = []
    depth = 0
    in_str = False
    str_char: Optional[str] = None
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
# Expression translation
# ---------------------------

def _lit_to_sql(raw: str) -> str:
    raw = raw.strip()
    if re.match(r"^-?\d+(\.\d+)?$", raw):
        return raw
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        return "'" + raw[1:-1].replace("'", "''") + "'"
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
    # df.col("X") -> df_alias.X (put first to avoid catching functions.col)
    def repl_df_col(m):
        var, arg = m.group(1), m.group(2)
        alias = var_to_alias.get(var, var)
        return f"{alias}.{unquote(arg)}"
    s = re.sub(r"([a-zA-Z_]\w*)\.col\(([^()]+?)\)", repl_df_col, s)

    # F.col("X") / functions.col("X") / col("X") -> X
    s = re.sub(r"(?:\bF|\bfunctions)\.col\(([^()]+?)\)", lambda m: unquote(m.group(1)), s)
    s = re.sub(r"\bcol\(([^()]+?)\)", lambda m: unquote(m.group(1)), s)

    # df("X") -> df_alias.X (safe only if string literal)
    def repl_df_paren(m):
        var, lit = m.group(1), m.group(2)
        alias = var_to_alias.get(var, var)
        return f"{alias}.{unquote(lit)}"
    s = re.sub(r"([a-zA-Z_]\w*)\(\s*(['\"][^'\"]+['\"])\s*\)", repl_df_paren, s)

    # expr("...") / functions.expr("...") -> raw SQL
    s = re.sub(r"(?:\bF|\bfunctions)\.expr\((.+?)\)", lambda m: unquote(m.group(1)), s)
    s = re.sub(r"\bexpr\((.+?)\)", lambda m: unquote(m.group(1)), s)

    # lit(x)
    s = re.sub(r"\blit\(([^()]+?)\)", lambda m: _lit_to_sql(m.group(1)), s)

    return s

def _replace_predicates_global(s: str) -> str:
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

    s = re.sub(r"\b\.isNull\b", " IS NULL", s)
    s = re.sub(r"\b\.isNotNull\b", " IS NOT NULL", s)
    s = re.sub(r"\b\.asc\b", " ASC", s)
    s = re.sub(r"\b\.desc\b", " DESC", s)
    return s

def _uppercase_known_functions(s: str) -> str:
    funcs = ["lower", "upper", "substr", "coalesce", "sum", "avg", "min", "max", "count"]
    for f in funcs:
        s = re.sub(rf"\b{f}\s*\(", f.upper() + "(", s)
    def repl_cd(m):  # countDistinct(x) -> COUNT(DISTINCT x)
        inner = m.group(1)
        return f"COUNT(DISTINCT {inner})"
    s = re.sub(r"\bcountDistinct\s*\((.*?)\)", repl_cd, s)
    return s

def translate_expression(expr: str, var_to_alias: Dict[str, str]) -> str:
    e = expr.strip()
    e = strip_outer_parens(e)
    # Resolve col/F.col/df("X")/lit/expr gradually until stable
    prev = None
    while prev != e:
        prev = e
        e = _replace_cols_and_lits(e, var_to_alias)
    e = replace_operators(e)
    e = _replace_predicates_global(e)
    e = _uppercase_known_functions(e)
    return e.strip()

def inline_aliases_in_sql(sql: str, alias_exprs: Dict[str, str]) -> str:
    """Replace alias references with their expressions in WHERE (avoid quotes)."""
    if not sql or not alias_exprs:
        return sql
    segments = re.split(r"('(?:''|[^'])*')", sql)  # keep quoted segments
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
        out: List[Tuple[str, Optional[str]]] = []
        rename_dict = dict(self.rename_map)
        for expr, alias in proj:
            if alias and alias in rename_dict:
                out.append((expr, rename_dict[alias]))
            else:
                if not alias and re.match(r"^[A-Za-z_]\w*$", expr) and expr in rename_dict:
                    out.append((expr, rename_dict[expr]))
                else:
                    out.append((expr, alias))
        return out

    def _apply_drop_to_projection(self, proj: List[Tuple[str, Optional[str]]]) -> List[Tuple[str, Optional[str]]]:
        if not self.drop_columns:
            return proj
        drop_set = set(self.drop_columns)
        out: List[Tuple[str, Optional[str]]] = []
        for expr, alias in proj:
            if alias and alias in drop_set:
                continue
            if not alias and re.match(r"^[A-Za-z_]\w*$", expr) and expr in drop_set:
                continue
            out.append((expr, alias))
        return out

    def _render_select_list(self) -> List[str]:
        if not self.selects:
            parts: List[str] = ["*"]
            # apply renames/drops to withColumns only
            rename_dict = dict(self.rename_map)
            drop_set = set(self.drop_columns)
            for alias, expr in self.with_columns:
                out_alias = rename_dict.get(alias, alias)
                if out_alias in drop_set:
                    continue
                parts.append(f"{expr} AS {out_alias}")
            return parts

        final: List[Tuple[str, Optional[str]]] = list(self.selects)
        final = self._apply_renames_to_projection(final)
        final = self._apply_drop_to_projection(final)

        # Merge withColumns (replace matching names or append)
        for w_alias, w_expr in self.with_columns:
            # rename target if needed
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

        out: List[str] = []
        for expr, alias in final:
            out.append(f"{expr} AS {alias}" if alias else expr)
        return out

    def to_sql(self) -> str:
        if not self.from_table:
            return "-- Unable to determine FROM source."
        lines: List[str] = []
        sel_parts = self._render_select_list()
        lines.append("SELECT " + ", ".join(sel_parts) if sel_parts else "SELECT *")
        lines.append(f"FROM {self.from_table}" + (f" {self.from_alias}" if self.from_alias else ""))
        for j in self.joins:
            lines.append(f"{j.join_type} JOIN {j.right_table} {j.right_alias} ON {j.condition_sql}")
        if self.where:
            alias_map = {alias: expr for alias, expr in self.with_columns}
            lines.append(f"WHERE {inline_aliases_in_sql(self.where, alias_map)}")
        if self.group_by:
            lines.append("GROUP BY " + ", ".join(self.group_by))
        if self.order_by:
            lines.append("ORDER BY " + ", ".join(self.order_by))
        if self.limit is not None:
            lines.append(f"LIMIT {self.limit}")
        return "\n".join(lines) + ";"

# ---------------------------
# Scala parser (heuristic)
# ---------------------------

class SnowparkScalaToSQLConverter:
    def __init__(self, scala_code: str, debug: bool = False):
        code = strip_comments(scala_code)
        code = preprocess_scala(code)
        self.code = code
        self.debug = debug
        self.var_to_table: Dict[str, str] = {}
        self.var_to_alias: Dict[str, str] = {}
        self.var_to_rhs: Dict[str, str] = {}

    def _discover_tables(self):
        # Capture all val assignments for backtracking DF chains
        for m in re.finditer(r"""val\s+([a-zA-Z_]\w*)\s*=\s*(.+?)(?=\n\s*val\s+|\Z)""", self.code, flags=re.S):
            self.var_to_rhs[m.group(1)] = m.group(2).strip()

        # Direct sources
        for m in re.finditer(r"""val\s+([a-zA-Z_]\w*)\s*=\s*session\.table\((.+?)\)""", self.code):
            var = m.group(1)
            table = unquote(m.group(2))
            alias = self._alias_from_table(table) or var
            self.var_to_table[var] = table
            self.var_to_alias[var] = alias

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

    def _resolve_base_table(self, var: str, depth: int = 0) -> Optional[Tuple[str, str]]:
        if depth > 20:
            return None
        if var in self.var_to_table:
            return (self.var_to_table[var], self.var_to_alias.get(var, var))
        rhs = self.var_to_rhs.get(var)
        if not rhs:
            return None
        # session.table(...)
        m = re.search(r"""session\.table\((.+?)\)""", rhs)
        if m:
            table = unquote(m.group(1))
            return (table, self._alias_from_table(table))
        # session.sql(...)
        m = re.search(r"""session\.sql\((.+?)\)""", rhs)
        if m:
            sql = unquote(m.group(1))
            return (f"({sql})", var)
        # Otherwise resolve first token recursively
        m = re.match(r"""([a-zA-Z_]\w*)""", rhs.strip())
        if m:
            return self._resolve_base_table(m.group(1), depth + 1)
        return None

    def _find_main_chain(self) -> Optional[str]:
        matches = list(re.finditer(r"val\s+([a-zA-Z_]\w*)\s*=\s*(.+?)(?=\n\s*val\s+|\Z)", self.code, flags=re.S))
        if matches:
            return matches[-1].group(2).strip()
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

        # FROM resolution
        m_base_var = re.match(r"""([a-zA-Z_]\w*)""", chain.strip())
        base_var = m_base_var.group(1) if m_base_var else None
        if base_var:
            res = self._resolve_base_table(base_var)
            if res:
                ir.from_table, ir.from_alias = res
        if not ir.from_table:
            m_base_table = re.search(r"""session\.table\((.+?)\)""", chain)
            if m_base_table:
                ir.from_table = unquote(m_base_table.group(1))
                ir.from_alias = self._alias_from_table(ir.from_table)

        var_to_alias = dict(self.var_to_alias)
        if base_var and not var_to_alias.get(base_var):
            var_to_alias[base_var] = ir.from_alias or base_var

        # Parse top-level method calls
        methods = re.findall(r"""\.([a-zA-Z_]\w*)\((.*?)\)""", chain, flags=re.S)

        # Helper: build condition for key-join (Seq("K1","K2"))
        def build_seq_join_on(left_alias: str, right_alias: str, seq_txt: str) -> str:
            inside = seq_txt.strip()
            if inside.startswith("Seq(") and inside.endswith(")"):
                keys_buf = inside[4:-1]
            else:
                keys_buf = inside
            keys = [unquote(k) for k in split_top_level_args(keys_buf)]
            parts = [f"{left_alias}.{k} = {right_alias}.{k}" for k in keys]
            return " AND ".join(parts) if parts else "1=1"

        # Process methods in order
        for (method, args_str) in methods:
            method = method.strip()
            args_str = args_str.strip()

            if method in ("alias", "as") and re.match(r"""^['"].+?['"]$""", args_str) and not ir.selects:
                # Heuristic: DF aliasing (not column alias)
                ir.from_alias = unquote(args_str)
                if base_var:
                    var_to_alias[base_var] = ir.from_alias
                continue

            if method == "join":
                args = split_top_level_args(args_str)
                if len(args) >= 3:
                    right_df = args[0].strip()
                    cond_or_seq = args[1].strip()
                    jtype = unquote(args[2]).upper()
                    join_type = {"INNER": "INNER", "LEFT": "LEFT", "RIGHT": "RIGHT", "FULL": "FULL"}.get(jtype, "INNER")

                    # Resolve right side source
                    if right_df in self.var_to_table:
                        r_table = self.var_to_table[right_df]
                        r_alias = self.var_to_alias.get(right_df, right_df)
                    else:
                        resr = self._resolve_base_table(right_df)
                        if resr:
                            r_table, r_alias = resr
                        elif right_df.startswith('session.table('):
                            rt = re.search(r'session\.table\((.+?)\)', right_df)
                            r_table = unquote(rt.group(1)) if rt else right_df
                            r_alias = self._alias_from_table(r_table)
                        else:
                            r_table = unquote(right_df)
                            r_alias = self._alias_from_table(r_table)

                    if r_alias:
                        var_to_alias[right_df] = r_alias

                    # cond can be SQL expression or Seq("K1","K2")
                    if cond_or_seq.strip().startswith("Seq("):
                        left_a = ir.from_alias or "LEFT"
                        right_a = r_alias or "RIGHT"
                        cond_sql = build_seq_join_on(left_a, right_a, cond_or_seq.strip())
                    else:
                        cond_sql = translate_expression(cond_or_seq, var_to_alias)

                    ir.joins.append(JoinSpec(join_type=join_type, right_table=r_table, right_alias=r_alias, condition_sql=cond_sql))

            elif method in ("filter", "where"):
                expr = translate_expression(args_str, var_to_alias)
                if ir.where:
                    ir.where = f"({ir.where}) AND ({expr})"
                else:
                    ir.where = expr

            elif method == "select":
                items = split_top_level_args(args_str)
                for it in items:
                    it_s = it.strip()
                    m_as = re.match(r"""(.+?)\.(?:as|alias)\((.+?)\)$""", it_s)
                    if m_as:
                        base = m_as.group(1).strip()
                        alias = unquote(m_as.group(2))
                        base_sql = translate_expression(base, var_to_alias)
                        ir.selects.append((base_sql, alias))
                    else:
                        base_sql = translate_expression(it_s, var_to_alias)
                        ir.selects.append((base_sql, None))

            elif method == "selectExpr":
                items = split_top_level_args(args_str)
                for it in items:
                    expr_raw = unquote(it)
                    ir.selects.append((expr_raw, None))

            elif method == "groupBy":
                keys = split_top_level_args(args_str)
                ir.group_by = [translate_expression(k, var_to_alias) for k in keys]

            elif method == "agg":
                aggs = split_top_level_args(args_str)
                for agg in aggs:
                    agg_s = agg.strip()
                    m_as = re.match(r"""(.+?)\.(?:as|alias)\((.+?)\)$""", agg_s)
                    if m_as:
                        base = m_as.group(1).strip()
                        alias = unquote(m_as.group(2))
                        base_sql = translate_expression(base, var_to_alias)
                        ir.selects.append((base_sql, alias))
                    else:
                        base_sql = translate_expression(agg_s, var_to_alias)
                        ir.selects.append((base_sql, None))

            elif method == "orderBy":
                items = split_top_level_args(args_str)
                for it in items:
                    ir.order_by.append(translate_expression(it.strip(), var_to_alias))

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
                    ir.rename_map.append((unquote(args[0]), unquote(args[1])))

            elif method == "drop":
                args = split_top_level_args(args_str)
                for a in args:
                    a_str = a.strip()
                    if re.match(r"""^['"].+?['"]$""", a_str):
                        ir.drop_columns.append(unquote(a_str))
                    else:
                        m = re.match(r"""col\((.+?)\)$""", a_str)
                        ir.drop_columns.append(unquote(m.group(1)) if m else unquote(a_str))

        if self.debug:
            print("[DEBUG] Base chain:", chain)
            print("[DEBUG] FROM:", ir.from_table, "AS", ir.from_alias)
            print("[DEBUG] SELECT count:", len(ir.selects), "withColumns:", ir.with_columns)
            print("[DEBUG] WHERE:", ir.where)
            print("[DEBUG] JOINS:", [ (j.join_type, j.right_table, j.right_alias, j.condition_sql) for j in ir.joins ])
            print("[DEBUG] GROUP BY:", ir.group_by, "ORDER BY:", ir.order_by, "LIMIT:", ir.limit)

        return ir.to_sql()

# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Convert Scala Snowpark code to Snowflake SQL (heuristic, DF-friendly).")
    ap.add_argument("input", help="Path to Scala file")
    ap.add_argument("-o", "--output", help="Write SQL to this file")
       ap.add_argument("--debug", action="store_true", help="Print parsed chain and IR details")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        code = f.read()

    converter = SnowparkScalaToSQLConverter(code, debug=args.debug)
    sql = converter.convert()

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(sql + "\n")
        if args.debug:
            print(f"[DEBUG] SQL written to {args.output}")
    else:
        print(sql)

if __name__ == "__main__":
