from typing import Any, Dict, List, Optional, Set, Tuple


def _try_import_sqlglot():
    try:
        import sqlglot  # type: ignore
        from sqlglot import exp  # type: ignore
        return sqlglot, exp
    except Exception as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "sqlglot is required for SQL lineage parsing. Install with "
            "`pip install sqlglot`."
        ) from exc


def _normalize_table_name(table_exp: Any) -> Optional[str]:
    """
    Build a normalized table identifier as catalog.schema.table when
    available, otherwise return the best-effort name.
    """
    _, exp = _try_import_sqlglot()

    if table_exp is None:
        return None

    # Resolve direct Table expression
    if isinstance(table_exp, exp.Table):
        table_name = table_exp.name
        db = getattr(table_exp, "db", None) or None
        catalog = getattr(table_exp, "catalog", None) or None

        # Guard against partial qualifiers without a table identifier
        if not table_name:
            return None

        if db and catalog:
            return f"{catalog}.{db}.{table_name}"
        if db:
            return f"{db}.{table_name}"
        return table_name

    # Many nodes expose the target as `.this`, which may be a Table
    if hasattr(table_exp, "this") and isinstance(table_exp.this, exp.Table):
        return _normalize_table_name(table_exp.this)

    # Fallback: try to stringify and extract last identifier
    try:
        sql = table_exp.sql(dialect="ansi")  # type: ignore[attr-defined]
        return sql.strip().strip("`") if sql else None
    except Exception:
        return None


def _dedupe_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    unique: List[Dict[str, Any]] = []
    for e in entities:
        eid = e.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            unique.append(e)
    return unique


def _dedupe_relationships(
    relationships: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str, str]] = set()
    unique: List[Dict[str, Any]] = []
    for r in relationships:
        key = (r.get("src", ""), r.get("dst", ""), r.get("type", ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def _func_full_name(func: Any) -> Optional[str]:
    """Return a fully qualified function name using SQL string prefix."""
    try:
        sql = func.sql(dialect="ansi")  # e.g., main.schema.fn(arg1,...)
        if not sql:
            return None
        prefix = sql.split("(", 1)[0].strip()
        return prefix or None
    except Exception:
        return None


def _collect_functions_in_from(select_exp: Any) -> Dict[str, List[str]]:
    """Collect table functions and scalar functions used in FROM clause."""
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return {"table_functions": [], "scalar_functions": []}

    table_funcs: List[str] = []
    scalar_funcs: List[str] = []

    from_clause = getattr(select_exp, "args", {}).get("from")
    if not from_clause:
        return {"table_functions": [], "scalar_functions": []}

    for func in from_clause.find_all(exp.Func):
        full = _func_full_name(func)
        if full:
            table_funcs.append(full)

    return {
        "table_functions": list(dict.fromkeys(table_funcs)),
        "scalar_functions": list(dict.fromkeys(scalar_funcs)),
    }


def _collect_tables(expression: Any) -> Set[str]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return set()
    if expression is None:
        return set()
    tables: Set[str] = set()
    for t in expression.find_all(exp.Table):
        name = _normalize_table_name(t)
        if name:
            tables.add(name)
    return tables


def _collect_aggregations(select_exp: Any) -> List[Dict[str, Any]]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return []

    aggs: List[Dict[str, Any]] = []
    agg_nodes = (exp.Max, exp.Min, exp.Sum, exp.Avg, exp.Count)
    for node in select_exp.find_all(agg_nodes):
        func_name = node.__class__.__name__.upper()
        arg_sql = ""
        try:
            arg_expr = node.args.get("this")
            if arg_expr is not None:
                arg_sql = arg_expr.sql(dialect="ansi")
        except Exception:
            arg_sql = ""
        is_distinct = bool(node.args.get("distinct")) \
            if isinstance(node, exp.Count) else False
        aggs.append({
            "function": func_name,
            "argument": arg_sql,
            "distinct": is_distinct,
        })
    return aggs


def _collect_filters(select_exp: Any) -> List[str]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return []
    where_obj = getattr(select_exp, "where", None)
    if not where_obj or not isinstance(where_obj, exp.Where):
        return []
    try:
        return [where_obj.this.sql(dialect="ansi")]
    except Exception:
        return [
            where_obj.sql(dialect="ansi")
        ] if hasattr(where_obj, "sql") else []


def _collect_group_by(select_exp: Any) -> List[str]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return []
    grp = getattr(select_exp, "group", None)
    if not grp or not isinstance(grp, exp.Group):
        return []
    cols: List[str] = []
    for e in grp.expressions or []:
        try:
            cols.append(e.sql(dialect="ansi"))
        except Exception:
            continue
    return cols


def _collect_order_by(select_exp: Any) -> List[str]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return []
    order = getattr(select_exp, "order", None)
    if not order or not isinstance(order, exp.Order):
        return []
    parts: List[str] = []
    for e in order.expressions or []:
        try:
            parts.append(e.sql(dialect="ansi"))
        except Exception:
            continue
    return parts


def _cte_name(cte: Any) -> Optional[str]:
    """Best-effort extraction of a CTE alias name."""
    alias = getattr(cte, "alias", None)
    if alias is not None:
        ident = getattr(alias, "this", None)
        if hasattr(ident, "this") and ident.this:
            return str(ident.this)
        if hasattr(ident, "name") and ident.name:
            return str(ident.name)
    try:
        sql = cte.sql(dialect="ansi")
        return sql.split("AS", 1)[0].strip() or None
    except Exception:
        return None


def _extract_ctes(parsed: Any) -> List[Dict[str, Any]]:
    try:
        from sqlglot import exp  # type: ignore
    except Exception:  # pragma: no cover - environment dependent
        return []

    with_obj = getattr(parsed, "args", {}).get("with")
    if not with_obj or not isinstance(with_obj, exp.With):
        return []

    ctes: List[Dict[str, Any]] = []
    for cte in with_obj.expressions or []:  # type: ignore[attr-defined]
        name = _cte_name(cte) or ""
        sel = getattr(cte, "this", None)  # underlying SELECT
        if not sel:
            continue
        sources_tables = sorted(_collect_tables(sel))
        from_funcs = _collect_functions_in_from(sel)
        aggregations = _collect_aggregations(sel)
        filters = _collect_filters(sel)
        group_by = _collect_group_by(sel)
        ctes.append({
            "name": name,
            "sources": {
                "tables": sources_tables,
                "table_functions": from_funcs.get("table_functions", []),
                "scalar_functions": from_funcs.get("scalar_functions", []),
            },
            "aggregations": aggregations,
            "filters": filters,
            "group_by": group_by,
        })
    return ctes


def parse_sql_lineage(sql_code: str) -> Dict[str, Any]:
    """
    Keep it simple:
    - For CTAS/INSERT/REPLACE: target and sources
    - Extract CTEs and their sources/functions and basic components
    - For plain SELECT: list referenced tables
    """
    sqlglot, exp = _try_import_sqlglot()

    entities: List[Dict[str, Any]] = []
    relationships: List[Dict[str, Any]] = []

    if not sql_code or not sql_code.strip():
        return {"entities": [], "relationships": [], "components": {}}

    try:
        parsed = sqlglot.parse_one(sql_code, error_level="ignore")
    except Exception:
        return {"entities": [], "relationships": [], "components": {}}

    target_name: Optional[str] = None
    select_expr: Any = None

    create_node = parsed if isinstance(parsed, exp.Create) else parsed.find(exp.Create)
    insert_node = parsed if isinstance(parsed, exp.Insert) else parsed.find(exp.Insert)
    replace_node = parsed if isinstance(parsed, exp.Replace) else parsed.find(exp.Replace)
    node = create_node or insert_node or replace_node

    if node is not None:
        try:
            target_name = _normalize_table_name(node.this)
        except Exception:
            target_name = None
        try:
            select_expr = node.find(exp.Select)
        except Exception:
            select_expr = None

    if target_name:
        entities.append({
            "id": target_name,
            "type": "table",
            "name": target_name,
        })
        for tbl in sorted(_collect_tables(select_expr)):
            entities.append({"id": tbl, "type": "table", "name": tbl})
            if tbl != target_name:
                relationships.append({
                    "src": tbl,
                    "dst": target_name,
                    "type": "reads_from",
                })

    ctes = _extract_ctes(parsed)
    cte_names: Set[str] = set()
    for cte in ctes:
        cte_name = cte.get("name") or ""
        if cte_name:
            cte_names.add(cte_name)
            entities.append({
                "id": cte_name,
                "type": "table",
                "name": cte_name,
                "properties": {"is_cte": True},
            })
        for src_tbl in cte.get("sources", {}).get("tables", []):
            entities.append({"id": src_tbl, "type": "table", "name": src_tbl})
            if cte_name and src_tbl:
                relationships.append({
                    "src": src_tbl,
                    "dst": cte_name,
                    "type": "reads_from",
                })
        for func_full in cte.get("sources", {}).get("table_functions", []):
            func_id = f"function:{func_full}"
            entities.append({"id": func_id, "type": "function", "name": func_full})
            if cte_name:
                relationships.append({
                    "src": func_id,
                    "dst": cte_name,
                    "type": "reads_from",
                })

    if not target_name:
        for t in parsed.find_all(exp.Table):
            name = _normalize_table_name(t)
            if name:
                entities.append({"id": name, "type": "table", "name": name})

    final_from_tables: Set[str] = set()
    final_order_by: List[str] = []
    try:
        if isinstance(parsed, exp.Select):
            final_from_tables = _collect_tables(parsed.args.get("from"))
            final_order_by = _collect_order_by(parsed)
        elif select_expr is not None and isinstance(select_expr, exp.Select):
            final_from_tables = _collect_tables(select_expr.args.get("from"))
            final_order_by = _collect_order_by(select_expr)
    except Exception:
        pass

    result: Dict[str, Any] = {
        "entities": _dedupe_entities(entities),
        "relationships": _dedupe_relationships(relationships),
        "components": {
            "ctes": ctes,
            "final_select": {
                "from": sorted(final_from_tables),
                "order_by": final_order_by,
            },
        },
    }
    return result


