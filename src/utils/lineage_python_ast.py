# lineage_python_ast_v3.py
# Static lineage extractor for PySpark notebooks using Python AST.
# Emits a dict with "entities" and "relationships" suitable for GraphFrames.
#
# Key features:
# - Works with plain .py files and notebook-like JSON/tuples (Databricks export style)
# - Robust string extraction (Constants, f-strings, Names, Attributes, concatenations)
# - Recognizes reads: spark.table / spark.read.table / spark.read.format(...).load(path)
# - Recognizes custom wrapper readers (e.g., read_source(storagelocation, ...))
# - Recognizes unions via reduce(DataFrame.union, dfs) and explicit list membership
# - Tracks generic DF derivations (join/select/groupBy/agg/filter/alias/union/...)
# - Recognizes writes: df.write.saveAsTable/table insertInto/save(path) and option("path", ..)
# - Derives symbolic strings from f-strings and variables (e.g., ${ENV}/bucket/table)
# - Avoids hard-coding names beyond generic Spark conventions; easily extendable

from __future__ import annotations

import ast
import textwrap
from typing import Any, Dict, List, Optional, Set, Tuple

# -------------------------------
# Helpers (name-agnostic, robust)
# -------------------------------

def normalize_source(notebook_like) -> str:
    """
    Accepts:
      - str: raw python source (returns as-is)
      - list[tuple]: e.g. [('python', code, {...}), ('markdown', ...)]
      - list[dict]:  Databricks/nbformat-like cells ('cell_type'/'source' or 'language'/'content')
    Returns: a single Python source string composed from all python cells.
    Also strips lines beginning with Databricks/Zeppelin magics ('%').
    """
    if isinstance(notebook_like, str):
        code = notebook_like
    else:
        parts: List[str] = []
        if isinstance(notebook_like, list):
            for item in notebook_like:
                # tuple formats: ('python', code) or ('python', code, metadata)
                if isinstance(item, tuple):
                    if len(item) >= 2 and isinstance(item[0], str):
                        lang = item[0].lower()
                        code = item[1]
                        if lang == "python" and isinstance(code, str):
                            parts.append(code)
                # dict formats
                elif isinstance(item, dict):
                    if item.get("cell_type") == "code":
                        src = item.get("source")
                        if isinstance(src, list):
                            parts.append("".join(src))
                        elif isinstance(src, str):
                            parts.append(src)
                    elif item.get("language", "").lower() == "python":
                        content = item.get("content") or item.get("source")
                        if isinstance(content, list):
                            parts.append("".join(content))
                        elif isinstance(content, str):
                            parts.append(content)
        code = "\n\n".join(parts)

    # Drop magic lines (e.g., %sql, %md)
    cleaned = []
    for line in code.splitlines():
        if line.lstrip().startswith('%'):
            continue
        cleaned.append(line)
    return textwrap.dedent("\n".join(cleaned)).strip()


def get_attr_chain(node: ast.AST) -> List[str]:
    """Return ['spark','read','format'] for spark.read.format(...), etc."""
    out: List[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        out.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        out.append(cur.id)
    out.reverse()
    return out


def terminal_method_name(call: ast.Call) -> Optional[str]:
    """Right-most attribute (.save, .saveAsTable, .table, .load, etc.)."""
    if not isinstance(call.func, ast.Attribute):
        return None
    return call.func.attr


def base_object(expr: ast.AST) -> Optional[ast.AST]:
    """Left-most base object of an attribute/call chain (e.g., df in df.write.format(...))."""
    node = expr
    if isinstance(node, ast.Call):
        node = node.func
    while isinstance(node, ast.Attribute):
        node = node.value
    return node


def _extract_string_or_symbol(node: ast.AST, resolve_name) -> Optional[str]:
    """
    Accept string-likes: Constant, f-strings (JoinedStr), Name, Attribute/Subscript, concatenations.
    Returns a concrete string or a symbolic string like '${ENV}/path' when unresolved.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value

    if isinstance(node, ast.JoinedStr):  # f-string
        parts: List[str] = []
        for v in node.values:
            if isinstance(v, ast.Str):
                parts.append(v.s)
            elif isinstance(v, ast.FormattedValue):
                try:
                    parts.append("${" + ast.unparse(v.value) + "}")
                except Exception:
                    parts.append("${EXPR}")
        return "".join(parts)

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        l = _extract_string_or_symbol(node.left, resolve_name)
        r = _extract_string_or_symbol(node.right, resolve_name)
        if l and r:
            return l + r

    if isinstance(node, ast.Name):
        resolved = resolve_name(node.id)
        return resolved if resolved else "${" + node.id + "}"

    if isinstance(node, (ast.Attribute, ast.Subscript)):
        try:
            return "${" + ast.unparse(node) + "}"
        except Exception:
            return "${EXPR}"

    return None


def _base_call_in_chain(expr: ast.AST) -> Optional[ast.Call]:
    """If expr is a chained call, return left-most ast.Call; else None/expr."""
    node = expr
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        f = node.func
        leftmost = node
        while isinstance(f, ast.Attribute) and isinstance(f.value, ast.Call):
            leftmost = f.value
            f = leftmost.func
        return leftmost
    return node if isinstance(node, ast.Call) else None


def _collect_input_df_names(node: ast.AST, known_dfs: Set[str]) -> List[str]:
    """Collect Name nodes that look like DF vars (plus broadcast(Name))."""
    names: Set[str] = set()

    class _V(ast.NodeVisitor):
        def visit_Name(self, n: ast.Name):
            if n.id in known_dfs:
                names.add(n.id)
        def visit_Call(self, c: ast.Call):
            if isinstance(c.func, ast.Name) and c.func.id == "broadcast" and c.args:
                arg0 = c.args[0]
                if isinstance(arg0, ast.Name) and arg0.id in known_dfs:
                    names.add(arg0.id)
            self.generic_visit(c)

    _V().visit(node)
    return list(names)


def _collect_options_in_chain(node: ast.Call) -> Dict[str, str]:
    """Walk back a call chain like df.write.format(...).option("path","/X").save() and collect options."""
    opts: Dict[str, str] = {}
    cur: ast.AST = node
    while isinstance(cur, ast.Call):
        if isinstance(cur.func, ast.Attribute) and cur.func.attr == "option" and len(cur.args) >= 2:
            key = _extract_string_or_symbol(cur.args[0], lambda _: None)
            val = _extract_string_or_symbol(cur.args[1], lambda _: None)
            if key and val:
                opts[key] = val
        cur = cur.func.value if isinstance(cur.func, ast.Attribute) else None
    return opts

# --------------------------------------------------
# The core visitor that builds entities/relationships
# --------------------------------------------------

class LineageExtractor(ast.NodeVisitor):
    def __init__(self):
        self.entities: List[Dict[str, Any]] = []
        self.relationships: List[Dict[str, str]] = []

        # remember constants/expressions for Names so we can resolve storagelocations, etc.
        self._const_assigns: Dict[str, ast.AST] = {}

        # which variables are DataFrames
        self._df_vars: Set[str] = set()

        # Known wrapper readers: function name -> spec
        self._reader_wrappers: Dict[str, Dict[str, Any]] = {
            "read_source": {"arg_index": 0, "kind": "path"}
        }

    # ------------- entity helpers -------------

    def _add_entity_once(self, ent: Dict[str, Any]) -> None:
        eid = ent["id"]
        for e in self.entities:
            if e["id"] == eid:
                return
        self.entities.append(ent)

    def _add_var_entity(self, var_name: str) -> None:
        # Avoid polluting with common function names
        if var_name in {"col","when","sum","lit","broadcast","reduce","DataFrame","current_timestamp"}:
            return
        self._add_entity_once({
            "id": f"var:{var_name}",
            "type": "variable",
            "name": var_name,
        })

    def _mark_df(self, var_name: str) -> None:
        self._df_vars.add(var_name)

    def _add_table_entity(self, table: str) -> None:
        self._add_entity_once({
            "id": f"table:{table}",
            "type": "table",
            "name": table,
        })

    def _add_path_entity(self, path: str) -> None:
        self._add_entity_once({
            "id": f"path:{path}",
            "type": "path",
            "name": path,
        })

    def _add_relationship(self, src: str, dst: str, rel_type: str) -> None:
        rec = {"src": src, "dst": dst, "type": rel_type}
        # De-dup
        for r in self.relationships:
            if r == rec:
                return
        self.relationships.append(rec)

    # ------------- name resolver -------------

    def _resolve_name(self, name: str) -> Optional[str]:
        expr = self._const_assigns.get(name)
        if expr is None:
            return None
        return _extract_string_or_symbol(expr, self._resolve_name)

    # ------------- node visitors -------------

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        # traverse body
        return self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> Any:
        # Cache simple RHS so Names can resolve later (storagelocations, lists, etc.)
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            tname = node.targets[0].id
            if isinstance(node.value, (ast.Constant, ast.JoinedStr, ast.BinOp, ast.Name, ast.Attribute, ast.Subscript, ast.List)):
                self._const_assigns[tname] = node.value

        # Only handle single-target assigns as lineage targets
        if not (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            return self.generic_visit(node)

        var_name = node.targets[0].id
        value = node.value

        # --- A) explicit Spark readers ---
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
            # spark.table("db.tbl")
            if isinstance(value.func.value, ast.Name) and value.func.value.id == "spark" and value.func.attr == "table":
                if value.args:
                    name = _extract_string_or_symbol(value.args[0], self._resolve_name)
                    if name:
                        self._add_table_entity(name)
                        self._add_var_entity(var_name)
                        self._mark_df(var_name)
                        self._add_relationship(src=f"table:{name}", dst=f"var:{var_name}", rel_type="reads_from")
                        return

            # spark.read.table(...), spark.read...load(...)
            chain = get_attr_chain(value.func)
            term = terminal_method_name(value)
            has_read = "read" in chain
            if has_read and value.args:
                if term == "table":
                    name = _extract_string_or_symbol(value.args[0], self._resolve_name)
                    if name:
                        self._add_table_entity(name)
                        self._add_var_entity(var_name)
                        self._mark_df(var_name)
                        self._add_relationship(src=f"table:{name}", dst=f"var:{var_name}", rel_type="reads_from")
                        return
                if term == "load":
                    path = _extract_string_or_symbol(value.args[0], self._resolve_name)
                    if path:
                        self._add_path_entity(path)
                        self._add_var_entity(var_name)
                        self._mark_df(var_name)
                        self._add_relationship(src=f"path:{path}", dst=f"var:{var_name}", rel_type="reads_from")
                        return

        # --- B) wrapper reads (read_source(...)) possibly chained ---
        base = _base_call_in_chain(value)
        if isinstance(base, ast.Call) and isinstance(base.func, ast.Name):
            fname = base.func.id
            if fname in self._reader_wrappers:
                spec = self._reader_wrappers[fname]
                argi = spec["arg_index"]
                if len(base.args) > argi:
                    endpoint = _extract_string_or_symbol(base.args[argi], self._resolve_name)
                    if endpoint:
                        kind = spec.get("kind", "path")
                        if kind == "table":
                            self._add_table_entity(endpoint)
                            self._add_var_entity(var_name)
                            self._mark_df(var_name)
                            self._add_relationship(src=f"table:{endpoint}", dst=f"var:{var_name}", rel_type="reads_from")
                        else:
                            self._add_path_entity(endpoint)
                            self._add_var_entity(var_name)
                            self._mark_df(var_name)
                            self._add_relationship(src=f"path:{endpoint}", dst=f"var:{var_name}", rel_type="reads_from")
                        return

        # --- C) reduce(DataFrame.union, dfs) ---
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "reduce" and len(value.args) >= 2:
            op, seq = value.args[0], value.args[1]
            if isinstance(op, ast.Attribute) and isinstance(op.value, ast.Name) and op.value.id == "DataFrame" and op.attr == "union":
                parents: List[str] = []
                if isinstance(seq, ast.List):
                    parents = [e.id for e in seq.elts if isinstance(e, ast.Name)]
                elif isinstance(seq, ast.Name):
                    la = self._const_assigns.get(seq.id)
                    if isinstance(la, ast.List):
                        parents = [e.id for e in la.elts if isinstance(e, ast.Name)]
                if parents:
                    self._add_var_entity(var_name)
                    self._mark_df(var_name)
                    for p in parents:
                        if p in self._df_vars:
                            self._add_var_entity(p)
                            self._add_relationship(src=f"var:{p}", dst=f"var:{var_name}", rel_type="derives_from")
                    return

        # --- D) generic DF derivations (join/select/groupBy/agg/filter/alias/union...) ---
        if isinstance(value, (ast.Call, ast.Attribute)):
            inputs = _collect_input_df_names(value, self._df_vars)
            if inputs:
                self._add_var_entity(var_name)
                self._mark_df(var_name)
                for parent in inputs:
                    self._add_var_entity(parent)
                    self._add_relationship(src=f"var:{parent}", dst=f"var:{var_name}", rel_type="derives_from")
                return

        # Non-DF or config
        self._add_var_entity(var_name)
        return self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:
        # Detect writes: df.write.saveAsTable(...), df.write.insertInto(...), df.write.save(...)
        if isinstance(node.func, ast.Attribute):
            term = terminal_method_name(node)
            root = base_object(node.func)
            root_var = root.id if isinstance(root, ast.Name) else None

            # check if the chain has ".write"
            chain = get_attr_chain(node.func)
            has_write = "write" in chain

            if has_write and term in {"saveAsTable", "insertInto"} and node.args:
                target = _extract_string_or_symbol(node.args[0], self._resolve_name)
                if target:
                    self._add_table_entity(target)
                    if root_var:
                        # ensure DF known
                        self._add_var_entity(root_var)
                        self._mark_df(root_var)
                        self._add_relationship(src=f"var:{root_var}", dst=f"table:{target}", rel_type="writes_to")
                    else:
                        # fallback: try inputs
                        df_inputs = _collect_input_df_names(node.func.value, self._df_vars)
                        for src in df_inputs:
                            self._add_var_entity(src)
                            self._mark_df(src)
                            self._add_relationship(src=f"var:{src}", dst=f"table:{target}", rel_type="writes_to")
                return self.generic_visit(node)

            if has_write and term == "save":
                # Try positional path first
                path: Optional[str] = None
                if node.args:
                    path = _extract_string_or_symbol(node.args[0], self._resolve_name)
                # If no positional arg, look for keyword path
                if not path and node.keywords:
                    for kw in node.keywords:
                        if kw.arg == "path" and kw.value is not None:
                            path = _extract_string_or_symbol(kw.value, self._resolve_name)
                            break
                # If still no path, scan chained .option("path", ...)
                if not path:
                    opts = _collect_options_in_chain(node)
                    path = opts.get("path")
                if path:
                    self._add_path_entity(path)
                    if root_var:
                        self._add_var_entity(root_var)
                        self._mark_df(root_var)
                        self._add_relationship(src=f"var:{root_var}", dst=f"path:{path}", rel_type="writes_to")
                    else:
                        df_inputs = _collect_input_df_names(node.func.value, self._df_vars)
                        for src in df_inputs:
                            self._add_var_entity(src)
                            self._mark_df(src)
                            self._add_relationship(src=f"var:{src}", dst=f"path:{path}", rel_type="writes_to")
                return self.generic_visit(node)

        return self.generic_visit(node)

    # Some writes/reads appear as bare expressions (not assigned)
    def visit_Expr(self, node: ast.Expr) -> Any:
        if isinstance(node.value, ast.Call):
            self.visit_Call(node.value)
        return self.generic_visit(node)

# -------------------------------
# API
# -------------------------------

def extract_lineage(source_code: str) -> Dict[str, Any]:
    """Parse Python source and return {"entities": [...], "relationships": [...]}."""
    tree = ast.parse(source_code)
    v = LineageExtractor()
    v.visit(tree)
    return {"entities": v.entities, "relationships": v.relationships}


def extract_lineage_from_notebook(notebook_like) -> Dict[str, Any]:
    code = normalize_source(notebook_like)
    return extract_lineage(code)

result = extract_lineage_from_notebook(cells)
print(json.dumps(result, indent=2, ensure_ascii=False))