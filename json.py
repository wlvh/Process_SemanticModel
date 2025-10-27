# -*- coding: utf-8 -*-
"""
LLMModelDocLite — 语义模型“轻文档 + 数据探索”生成器（单文件完整版）
- 输出为紧凑 JSON 契约，专供 LLM 消费（不写长篇背景）
- 元数据取自 INFO.VIEW.*（可选：最小降级）
- 表分类与星型图（仅业务相关）
- 轻量数据探索（可选）：
  * 事实表 row_count
  * 锚点（Anchor）= via-key 到 DimDate 的 MAX(date) + 7/30/90 日窗口计数
  * 失败回退 direct（事实表日期列）/fallback（全局 DimDate）
- 精简关系体检（可选，Top-K）：FK 空值率、覆盖率（孤儿率），RED/YELLOW/GREEN
- 维度展示列与别名映射（中英/常用术语）
- 度量分类与依赖（不展开 DAX，避免冗长；可通过开关包含）

依赖：
  pip install pandas
  （在 Fabric/Power BI Notebook 环境）pip install sempy

输出：
  ./llm_contract.json
"""

from __future__ import annotations
import datetime as dt
import re
import json
import time
from typing import Any, Dict, List, Optional, Protocol, Tuple, Set

import pandas as pd

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    np = None
    _NUMPY_AVAILABLE = False

# ----------------------------
# 可选：Fabric SDK
# ----------------------------
try:
    import sempy.fabric as fabric  # Microsoft Fabric / Power BI Python SDK
    _FABRIC_AVAILABLE = True
except Exception:
    fabric = None
    _FABRIC_AVAILABLE = False


# ----------------------------
# Runner（可依赖注入）
# ----------------------------
class DaxQueryRunner(Protocol):
    def evaluate(self, dataset: str, dax: str, workspace: Optional[str]) -> pd.DataFrame: ...


class FabricRunner:
    """默认 Runner：sempy.fabric.evaluate_dax，带轻量重试。"""
    def __init__(self, retries: int = 2, backoff: float = 0.8):
        self.retries = max(0, retries)
        self.backoff = max(0.0, backoff)

    def evaluate(self, dataset: str, dax: str, workspace: Optional[str]) -> pd.DataFrame:
        if not _FABRIC_AVAILABLE:
            raise RuntimeError("Fabric SDK 不可用。请在支持的环境安装 `sempy` 并运行。")
        last_err = None
        for i in range(self.retries + 1):
            try:
                return fabric.evaluate_dax(dataset=dataset, dax_string=dax, workspace=workspace)
            except Exception as e:
                last_err = e
                if i < self.retries:
                    time.sleep(self.backoff * (i + 1))
        # 最后一次失败抛出
        raise last_err


# ----------------------------
# 主类：Lite 文档 + 数据探索
# ----------------------------
class LLMModelDocLite:
    def __init__(self, runner: Optional[DaxQueryRunner] = None, verbose: bool = True):
        self.runner = runner or FabricRunner()
        self.verbose = verbose

    # ======== 对外入口 ========
    def generate(
        self,
        model_name: str,
        workspace: Optional[str] = None,
        *,
        include_measure_dax: bool = False,   # True 则随契约输出 DAX（可能较长）
        profile_mode: str = "light",         # "off" | "light" | "standard"
        relationship_top_k: int = 12,
        include_enums: bool = False,
        max_enum_values: int = 10
    ) -> str:
        md = self._fetch_metadata(model_name, workspace)
        st = self._analyze(md)

        # 轻量数据探索
        data_profile: Dict[str, Any] = {}
        if profile_mode in {"light", "standard"}:
            if self.verbose: print("🩺 数据探索（facts via-key + row_count）...")
            data_profile["facts"] = self._profile_facts_via_key(model_name, workspace, md, st)
            if profile_mode == "standard":
                if self.verbose: print("🧪 关系体检（Top-K 边）...")
                data_profile["relationships"] = self._profile_relationships_lite(
                    model_name, workspace, md, st, top_k=relationship_top_k
                )

        # 组装 LLM 契约
        contract = self._build_llm_contract(
            md=md,
            st=st,
            profiles=data_profile,
            include_measure_dax=include_measure_dax,
            include_enums=include_enums,
            max_enum_values=max_enum_values
        )
        return self._json_dumps(contract)

    # ======== 元数据提取 ========
    def _fetch_metadata(self, model_name: str, workspace: Optional[str]) -> Dict[str, Any]:
        if self.verbose: print("📊 提取元数据（INFO.VIEW.*）...")
        md: Dict[str, Any] = {'tables': [], 'columns': [], 'measures': [], 'relationships': [], 'errors': []}

        queries = {
            'tables': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.TABLES(),
                "table_name",[Name],
                "is_hidden",[IsHidden],
                "description",[Description],
                "storage_mode",[StorageMode]
            )""",
            'columns': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.COLUMNS(),
                "table_name",[Table],
                "column_name",[Name],
                "data_type",[DataType],
                "is_hidden",[IsHidden],
                "is_key",[IsKey],
                "is_nullable",[IsNullable],
                "is_unique",[IsUnique],
                "description",[Description]
            )""",
            'measures': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.MEASURES(),
                "table_name",[Table],
                "measure_name",[Name],
                "dax_expression",[Expression],
                "format_string",[FormatString],
                "is_hidden",[IsHidden],
                "description",[Description]
            )""",
            'relationships': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.RELATIONSHIPS(),
                "from_table",[FromTable],
                "from_column",[FromColumn],
                "from_cardinality",[FromCardinality],
                "to_table",[ToTable],
                "to_column",[ToColumn],
                "to_cardinality",[ToCardinality],
                "is_active",[IsActive],
                "cross_filter_direction",[CrossFilteringBehavior]
            )"""
        }

        for key, dax in queries.items():
            try:
                df = self.runner.evaluate(model_name, dax, workspace)
                df = self._normalize_df(df)
                md[key] = df.to_dict('records')
                if self.verbose: print(f"  ✓ {key}: {len(md[key])}")
            except Exception as e:
                md['errors'].append(f"{key} not available: {e}")
                if self.verbose: print(f"  ⚠ {key} 失败（略过）: {e}")

        # 标注业务表（剔除自动日期表 + 隐藏表）
        auto_date_regex = re.compile(r'^(LocalDateTable_|DateTableTemplate_)', re.IGNORECASE)
        md['auto_date_tables'] = [
            t.get('table_name') for t in md['tables']
            if auto_date_regex.match(t.get('table_name') or '')
        ]
        md['business_tables'] = [
            t for t in md['tables']
            if not auto_date_regex.match(t.get('table_name') or '') and not self._b(t.get('is_hidden'))
        ]
        return md

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [
            (c or '').strip().replace('[', '').replace(']', '').lower().replace(' ', '_')
            for c in df.columns
        ]
        return df

    # ======== 结构分析 ========
    def _analyze(self, md: Dict[str, Any]) -> Dict[str, Any]:
        st: Dict[str, Any] = {'table_types': {}, 'star': {}, 'fact_time': {}}

        # 选择全局 DimDate
        dim_table, dim_key, dim_date_col = self._pick_default_dimdate(md)
        st['date_axis'] = {'table': dim_table, 'key_column': dim_key, 'date_column': dim_date_col}

        # 表分类（简单规则 + 关系信号）
        col_by_table = {}
        for c in md['columns']:
            col_by_table.setdefault(c.get('table_name'), []).append(c)
        rels = [r for r in md['relationships'] if self._active_business_rel(r)]

        # 统计关系方向
        out_count, in_count = {}, {}
        for r in rels:
            out_count[r['from_table']] = out_count.get(r['from_table'], 0) + 1
            in_count[r['to_table']] = in_count.get(r['to_table'], 0) + 1

        for t in [tb.get('table_name') for tb in md['business_tables']]:
            name = (t or '').lower()
            outgoing = out_count.get(t, 0)
            incoming = in_count.get(t, 0)
            cols = col_by_table.get(t, [])
            # 命名优先
            if name.startswith('fact') or name.startswith('vwpcse_fact'):
                st['table_types'][t] = 'fact'
            elif name.startswith('dim') or name.startswith('vwpcse_dim'):
                st['table_types'][t] = 'dimension'
            else:
                # 结构信号
                if outgoing >= 2:
                    st['table_types'][t] = 'fact'
                elif incoming > outgoing:
                    st['table_types'][t] = 'dimension'
                else:
                    # 文本列多 -> 维度
                    num_text = sum('text' in (c.get('data_type') or '').lower() or 'string' in (c.get('data_type') or '').lower() for c in cols)
                    num_num = sum(any(k in (c.get('data_type') or '').lower() for k in ['int','decimal','double','currency','number','whole']) for c in cols)
                    st['table_types'][t] = 'dimension' if num_text > num_num else 'other'

        # 星型图（事实 -> 维度）
        fact_tables = [t for t, ty in st['table_types'].items() if ty == 'fact']
        dim_tables = [t for t, ty in st['table_types'].items() if ty == 'dimension']
        for f in fact_tables:
            st['star'][f] = {'dimensions': []}
        for r in rels:
            fr, to = r.get('from_table'), r.get('to_table')
            if fr in fact_tables and to in dim_tables:
                st['star'][fr]['dimensions'].append({
                    'dimension_table': to,
                    'join_key': f"{r.get('from_column')} → {r.get('to_column')}",
                    'direction': r.get('cross_filter_direction')
                })

        # 每个事实的“默认时间键”与“日期轴”
        for f in fact_tables:
            # 查找与全局/候选 DimDate 的关系
            pick = self._find_time_key_for_fact(f, rels, dim_tables, prefer_table=dim_table)
            if pick:
                st['fact_time'][f] = {
                    'default_time_key': pick[0],      # fact key
                    'date_dimension': pick[1],        # dim table
                    'date_dimension_key': pick[2],    # dim key
                }
            else:
                # 没有键，则尝试事实表内日期列（仅记录，真正 profiling 时再处理）
                st['fact_time'][f] = {'default_time_key': None, 'date_dimension': dim_table, 'date_dimension_key': dim_key}
        return st

    def _pick_default_dimdate(self, md: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """选择全局 DimDate 表、键、日期列。"""
        candidates = [t.get('table_name') for t in md['tables'] if t.get('table_name')]
        # 优先：包含 dimdate/date/calendar 的维度
        def score(name: str) -> int:
            n = name.lower()
            if 'dimdate' in n: return 0
            if n.endswith('dimdate'): return 1
            if 'calendar' in n: return 2
            if n.endswith('date'): return 3
            if 'date' in n: return 4
            return 9
        candidates = sorted(candidates, key=score)
        dim_table = None
        for t in candidates:
            if 'dim' in t.lower() and ('date' in t.lower() or 'calendar' in t.lower()):
                dim_table = t; break
        if not dim_table:
            # 次选：包含 date/calendar 的任何表
            for t in candidates:
                if 'date' in t.lower() or 'calendar' in t.lower():
                    dim_table = t; break

        # 选择键与日期列
        dim_key = None
        dim_date_col = None
        if dim_table:
            cols = [c for c in md['columns'] if c.get('table_name') == dim_table]
            names = [c.get('column_name') for c in cols if c.get('column_name')]
            # 键
            for cand in ['DateKey', 'Date Id', 'DateID']:
                if cand in names: dim_key = cand; break
            if not dim_key:
                # endswith DateKey
                m = [n for n in names if n and n.lower().endswith('datekey')]
                dim_key = m[0] if m else (names[0] if names else None)
            # 日期列
            dim_date_col = self._select_dim_date_col(dim_table, md)
        return dim_table, dim_key, dim_date_col

    def _select_dim_date_col(self, dim_table: str, md: Dict[str, Any]) -> Optional[str]:
        cols = [c for c in md['columns'] if c.get('table_name') == dim_table]
        names = [c.get('column_name') for c in cols if c.get('column_name')]
        typed = [c.get('column_name') for c in cols if 'date' in (c.get('data_type') or '').lower()]
        for prefer in ['CalendarDate', 'Date', 'DateValue']:
            if prefer in names: return prefer
        return typed[0] if typed else (names[0] if names else None)

    def _find_time_key_for_fact(
        self, fact: str, rels: List[Dict[str, Any]], dim_tables: List[str], prefer_table: Optional[str]
    ) -> Optional[Tuple[str, str, str]]:
        """在关系中找 事实->日期维度 的键。"""
        pick: Optional[Tuple[str, str, str]] = None
        # 首选：指向 prefer_table 的关系
        for r in rels:
            if r.get('from_table') == fact and r.get('to_table') == prefer_table:
                from_col = r.get('from_column'); to_col = r.get('to_column') or 'DateKey'
                if from_col: return (from_col, r.get('to_table'), to_col)
        # 次选：任何日期型维度
        for r in rels:
            if r.get('from_table') == fact and r.get('to_table') in dim_tables and any(
                k in (r.get('to_table') or '').lower() for k in ['dimdate', 'calendar', 'date']
            ):
                from_col = r.get('from_column'); to_col = r.get('to_column') or 'DateKey'
                if from_col: pick = (from_col, r.get('to_table'), to_col); break
        return pick

    # ======== 数据探索：facts（via-key） ========
    def _profile_facts_via_key(
        self, model_name: str, workspace: Optional[str], md: Dict[str, Any], st: Dict[str, Any]
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        dim_table = st['date_axis']['table']
        dim_key = st['date_axis']['key_column']
        dim_date = st['date_axis']['date_column']

        # 事实表列表
        fact_tables = [t for t, ty in st['table_types'].items() if ty == 'fact']

        for fact in fact_tables:
            # 1) 行数
            rc = None
            try:
                df = self.runner.evaluate(model_name, f"EVALUATE ROW(\"row_count\", COUNTROWS('{fact}'))", workspace)
                rc = int(df.iloc[0, 0]) if not df.empty else None
            except Exception:
                pass

            # 2) time anchor（优先 via-key）
            cfg = st['fact_time'].get(fact, {}) if st.get('fact_time') else {}
            fkey = cfg.get('default_time_key')
            dtab = cfg.get('date_dimension') or dim_table
            dkey = cfg.get('date_dimension_key') or dim_key
            dcol = self._select_dim_date_col(dtab, md) if dtab else dim_date

            payload = {"source": None, "min": None, "max": None, "anchor": None,
                       "cnt7": None, "cnt30": None, "cnt90": None, "date_axis_column": dcol}

            # a) via-key
            if fkey and dtab and dkey and dcol:
                dax = f"""
EVALUATE
VAR K =
  SELECTCOLUMNS(
    FILTER(VALUES('{fact}'[{fkey}]), NOT ISBLANK('{fact}'[{fkey}])),
    "__k", '{fact}'[{fkey}]
  )
VAR Anchor =
  CALCULATE(MAX('{dtab}'[{dcol}]), TREATAS(K, '{dtab}'[{dkey}]))
VAR MinDate =
  CALCULATE(MIN('{dtab}'[{dcol}]), TREATAS(K, '{dtab}'[{dkey}]))
VAR W7  = IF(NOT ISBLANK(Anchor), FILTER(ALL('{dtab}'[{dcol}]), '{dtab}'[{dcol}]>Anchor-7  && '{dtab}'[{dcol}]<=Anchor))
VAR W30 = IF(NOT ISBLANK(Anchor), FILTER(ALL('{dtab}'[{dcol}]), '{dtab}'[{dcol}]>Anchor-30 && '{dtab}'[{dcol}]<=Anchor))
VAR W90 = IF(NOT ISBLANK(Anchor), FILTER(ALL('{dtab}'[{dcol}]), '{dtab}'[{dcol}]>Anchor-90 && '{dtab}'[{dcol}]<=Anchor))
VAR W7K   = CALCULATETABLE(VALUES('{dtab}'[{dkey}]),  W7)
VAR W30K  = CALCULATETABLE(VALUES('{dtab}'[{dkey}]), W30)
VAR W90K  = CALCULATETABLE(VALUES('{dtab}'[{dkey}]), W90)
RETURN
ROW(
  "min",     MinDate,
  "max",     Anchor,
  "anchor",  Anchor,
  "cnt7",    CALCULATE(COUNTROWS('{fact}'), TREATAS(W7K,  '{fact}'[{fkey}])),
  "cnt30",   CALCULATE(COUNTROWS('{fact}'), TREATAS(W30K, '{fact}'[{fkey}])),
  "cnt90",   CALCULATE(COUNTROWS('{fact}'), TREATAS(W90K, '{fact}'[{fkey}]))
)
"""
                try:
                    df = self.runner.evaluate(model_name, dax, workspace)
                    if not df.empty and pd.notna(df.iloc[0].get("anchor")):
                        r = df.iloc[0].to_dict()
                        payload.update({
                            "min": self._to_iso(r.get("min")),
                            "max": self._to_iso(r.get("max")),
                            "anchor": self._to_iso(r.get("anchor")),
                            "cnt7": self._to_int(r.get("cnt7")),
                            "cnt30": self._to_int(r.get("cnt30")),
                            "cnt90": self._to_int(r.get("cnt90"))
                        })
                        payload["source"] = "via_key"
                except Exception:
                    pass

            # b) direct（事实表内首个日期型列）
            if payload["source"] is None:
                date_cols = [c.get("column_name") for c in md["columns"]
                             if c.get("table_name") == fact and 'date' in (c.get("data_type") or "").lower()]
                if date_cols:
                    col = date_cols[0]
                    dax = f"""
EVALUATE
VAR B = FILTER(ALL('{fact}'[{col}]), NOT ISBLANK('{fact}'[{col}]))
VAR Mi = MINX(B, '{fact}'[{col}])
VAR Ma = MAXX(B, '{fact}'[{col}])
VAR W7  = IF(NOT ISBLANK(Ma), FILTER(B, '{fact}'[{col}]>Ma-7  && '{fact}'[{col}]<=Ma))
VAR W30 = IF(NOT ISBLANK(Ma), FILTER(B, '{fact}'[{col}]>Ma-30 && '{fact}'[{col}]<=Ma))
VAR W90 = IF(NOT ISBLANK(Ma), FILTER(B, '{fact}'[{col}]>Ma-90 && '{fact}'[{col}]<=Ma))
RETURN
ROW(
  "min", Mi, "max", Ma, "anchor", Ma,
  "cnt7", COUNTROWS(W7), "cnt30", COUNTROWS(W30), "cnt90", COUNTROWS(W90)
)
"""
                    try:
                        df = self.runner.evaluate(model_name, dax, workspace)
                        if not df.empty and pd.notna(df.iloc[0].get("anchor")):
                            r = df.iloc[0].to_dict()
                            payload.update({
                                "min": self._to_iso(r.get("min")),
                                "max": self._to_iso(r.get("max")),
                                "anchor": self._to_iso(r.get("anchor")),
                                "cnt7": self._to_int(r.get("cnt7")),
                                "cnt30": self._to_int(r.get("cnt30")),
                                "cnt90": self._to_int(r.get("cnt90"))
                            })
                            payload["source"] = "direct"
                    except Exception:
                        pass

            # c) fallback（全局 DimDate）
            if payload["source"] is None and dim_table and dim_date:
                try:
                    df = self.runner.evaluate(model_name, f"EVALUATE ROW(\"anchor\", MAX('{dim_table}'[{dim_date}]))", workspace)
                    if not df.empty:
                        v = df.iloc[0, 0]
                        iso = self._to_iso(v)
                        payload["anchor"] = iso
                        payload["max"] = iso
                        payload["source"] = "fallback"
                except Exception:
                    pass

            out[fact] = {"row_count": rc, "time": payload}
        return out

    # ======== 数据探索：关系体检（Top-K） ========
    def _profile_relationships_lite(
        self, model_name: str, workspace: Optional[str], md: Dict[str, Any], st: Dict[str, Any], top_k: int = 12
    ) -> Dict[str, Any]:
        # 候选边：事实->（常见维度）活动关系
        common_dims = ("dimdate","dimgeography","dimqueue","dimpartner","dimsap","dimlanguage","dimsite","dimrootcause")
        active_edges = [r for r in md['relationships'] if self._active_business_rel(r)]
        candidates = []
        for r in active_edges:
            to = (r.get('to_table') or '').lower()
            if any(k in to for k in common_dims):
                candidates.append(r)
        if not candidates:
            candidates = active_edges[:]
        edges = candidates[:max(1, top_k)]

        details: List[Dict[str, Any]] = []
        for r in edges:
            ftab, fcol, dtab, dcol = r.get("from_table"), r.get("from_column"), r.get("to_table"), r.get("to_column")

            # 空值率/行数/去重
            blank_ratio = coverage = None
            blank_fk = total_rows = distinct_fk = None
            dax1 = f"""
EVALUATE ROW(
 "blank_fk", COUNTROWS(FILTER('{ftab}', ISBLANK('{ftab}'[{fcol}]))),
 "total_rows", COUNTROWS('{ftab}'),
 "distinct_fk", DISTINCTCOUNT('{ftab}'[{fcol}])
)"""
            try:
                df1 = self.runner.evaluate(model_name, dax1, workspace)
                if not df1.empty:
                    row = df1.iloc[0]
                    blank_fk   = self._to_int(row.get("blank_fk"))
                    total_rows = self._to_int(row.get("total_rows"))
                    distinct_fk= self._to_int(row.get("distinct_fk"))
                    if total_rows and blank_fk is not None:
                        blank_ratio = blank_fk / total_rows
            except Exception:
                pass

            # 孤儿键（Fact 中有、Dim 中没有）
            orphan_fk = None
            dax2 = f"""
EVALUATE
VAR FK = SELECTCOLUMNS(FILTER(VALUES('{ftab}'[{fcol}]), NOT ISBLANK('{ftab}'[{fcol}])), "__k", '{ftab}'[{fcol}])
VAR PK = SELECTCOLUMNS(FILTER(VALUES('{dtab}'[{dcol}]), NOT ISBLANK('{dtab}'[{dcol}])), "__k", '{dtab}'[{dcol}])
RETURN ROW("orphan_fk", COUNTROWS(EXCEPT(FK, PK)))
"""
            try:
                df2 = self.runner.evaluate(model_name, dax2, workspace)
                if not df2.empty:
                    orphan_fk = self._to_int(df2.iloc[0].get("orphan_fk"))
                    if distinct_fk and distinct_fk > 0 and orphan_fk is not None:
                        coverage = 1 - (orphan_fk / distinct_fk)
            except Exception:
                pass

            # 严重度
            sev = "GREEN"
            if (coverage is not None and coverage < 0.95) or (blank_ratio is not None and blank_ratio > 0.05):
                sev = "RED"
            elif (coverage is not None and coverage < 0.98) or (blank_ratio is not None and blank_ratio > 0.02):
                sev = "YELLOW"

            details.append({
                "from": f"{ftab}[{fcol}]",
                "to": f"{dtab}[{dcol}]",
                "blank_fk_ratio": round(blank_ratio, 4) if blank_ratio is not None else None,
                "coverage": round(coverage, 4) if coverage is not None else None,
                "severity": sev,
                "inactive": False,
                "hint": None
            })

        return {"summary": details}

    # ======== 契约组装 ========
    def _build_llm_contract(
        self,
        md: Dict[str, Any],
        st: Dict[str, Any],
        profiles: Dict[str, Any],
        *,
        include_measure_dax: bool,
        include_enums: bool,
        max_enum_values: int
    ) -> Dict[str, Any]:
        # 维度展示列 + 别名
        dimensions: Dict[str, Any] = {}
        for t, ty in st['table_types'].items():
            if ty != 'dimension': continue
            label = self._pick_label_column(t, md)
            alias_target = f"{t}[{label}]" if label else None
            aliases = self._expand_synonyms(label or t.replace('vwpcse_', ''))
            alias_map = {a: alias_target for a in aliases if alias_target}
            dimensions[t] = {"label": label, "aliases": alias_map}

        # 事实摘要（时间键、日期维度、group-by 建议）
        facts: Dict[str, Any] = {}
        for f, ty in st['table_types'].items():
            if ty != 'fact': continue
            ft = st['fact_time'].get(f, {}) if st.get('fact_time') else {}
            group_by = self._suggest_group_by(f, st, md)
            facts[f] = {
                "default_time_key": ft.get('default_time_key'),
                "date_dimension": ft.get('date_dimension') or st['date_axis']['table'],
                "date_dimension_key": ft.get('date_dimension_key') or st['date_axis']['key_column'],
                "group_by_suggestions": group_by[:5] if group_by else []
            }

        # 关系（含非活动 USERELATIONSHIP 提示）
        relationships = []
        for r in md['relationships']:
            if self._is_auto_table(r.get('from_table')) or self._is_auto_table(r.get('to_table')):
                continue
            inactive = not self._b(r.get('is_active'))
            hint = None
            if inactive:
                ft, fc, tt, tc = r.get('from_table'), r.get('from_column'), r.get('to_table'), r.get('to_column')
                if ft and fc and tt and tc:
                    hint = f"USERELATIONSHIP('{ft}'[{fc}], '{tt}'[{tc}])"
            relationships.append({
                "from": f"{r.get('from_table')}[{r.get('from_column')}]",
                "to": f"{r.get('to_table')}[{r.get('to_column')}]",
                "direction": r.get('cross_filter_direction'),
                "inactive": inactive,
                "hint": hint
            })

        # 度量（只给类别/依赖；可开关输出 DAX）
        measures: Dict[str, Any] = {}
        for m in md['measures']:
            if self._b(m.get('is_hidden')):  # 隐藏的略过
                continue
            name = m.get('measure_name')
            if not name: continue
            dax = m.get('dax_expression') or ''
            cat = self._measure_category(dax)
            dep = self._extract_measure_deps(dax)
            entry = {
                "table": m.get('table_name'),
                "category": cat,
                "format": m.get('format_string') or '',
                "depends_on": dep
            }
            if include_measure_dax:
                entry["dax"] = dax
            measures[name] = entry

        # 枚举（可选，常见业务列）
        enums: Dict[str, List[Any]] = {}
        if include_enums:
            enum_candidates = [
                ('vwpcse_factescalationcase', 'EscalationTier'),
                ('vwpcse_facttask_created', 'TaskType'),
                ('vwpcse_factincident_closed', 'Case State')
            ]
            for t, c in enum_candidates:
                dax = f"""
EVALUATE
TOPN({max_enum_values},
  SUMMARIZE('{t}', '{t}'[{c}], "cnt", COUNTROWS('{t}')),
  [cnt], DESC
)"""
                try:
                    df = self.runner.evaluate(dataset=md.get('model_name') or "", dax=dax, workspace=None)
                    if not df.empty:
                        vals = [row[0] for row in df.iloc[:, :1].values.tolist() if row and row[0] is not None]
                        if vals:
                            enums[f"{t}[{c}]"] = vals
                except Exception:
                    pass

        # 合成
        contract: Dict[str, Any] = {
            "version": "llm-contract/1.1",
            "date_axis": st.get('date_axis'),
            "facts": facts,
            "dimensions": dimensions,
            "relationships": relationships,
            "measures": measures,
            "data_profile": profiles or {},
            "counts": {
                "tables": len(md.get('business_tables', [])),
                "measures": len(measures),
                "relationships_active_non_auto": len([r for r in md['relationships'] if self._active_business_rel(r)])
            },
            "warnings": self._model_warnings(dimensions)
        }
        return contract

    # ======== 工具函数 ========
    def _active_business_rel(self, r: Dict[str, Any]) -> bool:
        if not self._b(r.get('is_active')): return False
        return not (self._is_auto_table(r.get('from_table')) or self._is_auto_table(r.get('to_table')))

    @staticmethod
    def _is_auto_table(name: Optional[str]) -> bool:
        if not name: return False
        return bool(re.match(r'^(LocalDateTable_|DateTableTemplate_)', name, re.IGNORECASE))

    @staticmethod
    def _b(v: Any) -> bool:
        if v is None: return False
        if isinstance(v, str):
            return v.strip().lower() in {"true","1","yes","y","t"}
        try:
            return bool(v)
        except Exception:
            return False

    @staticmethod
    def _to_iso(value: Any) -> Optional[str]:
        """将多种日期/时间对象标准化为 ISO8601 字符串。

        Args:
            value: 任意待序列化对象，常见为 pandas.Timestamp、datetime、numpy.datetime64。

        Returns:
            可直接写入 JSON 的 ISO8601 字符串；若无法识别则返回 str(value)。
        """
        # None 直接返回，保持数据缺失语义。
        if value is None:
            return None

        # 先处理 pandas.Timestamp，兼容 NaT。
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.to_pydatetime().isoformat()

        # 处理 Python 原生日期/时间类型。
        if isinstance(value, (dt.datetime, dt.date)):
            return value.isoformat()

        # 处理 NumPy datetime64（若可用）。
        if _NUMPY_AVAILABLE and isinstance(value, np.datetime64):
            ts = pd.Timestamp(value)
            if pd.isna(ts):
                return None
            return ts.to_pydatetime().isoformat()

        # 兜底：转成字符串，避免 json.dumps 失败。
        return str(value)

    @staticmethod
    def _json_dumps(obj: Any) -> str:
        """序列化 JSON，同时安全处理时间对象与 NumPy 标量。

        Args:
            obj: 任意可被 JSON 序列化的数据结构。

        Returns:
            经过缩进与非 ASCII 友好设置的 JSON 字符串。
        """

        def _default(o: Any) -> Any:
            """default 回调：降级处理特殊对象。"""
            # pandas.Timestamp，含 NaT。
            if isinstance(o, pd.Timestamp):
                if pd.isna(o):
                    return None
                return o.to_pydatetime().isoformat()

            # Python datetime/date。
            if isinstance(o, (dt.datetime, dt.date)):
                return o.isoformat()

            # NumPy 标量（数字/布尔/时间）。
            if _NUMPY_AVAILABLE:
                if isinstance(o, np.integer):
                    return int(o)
                if isinstance(o, np.floating):
                    return float(o)
                if isinstance(o, np.bool_):
                    return bool(o)
                if isinstance(o, np.datetime64):
                    return LLMModelDocLite._to_iso(o)

            # 其他对象如果提供 isoformat()，尝试调用。
            if hasattr(o, "isoformat") and callable(o.isoformat):
                return o.isoformat()

            # 最后兜底转成字符串。
            return str(o)

        return json.dumps(obj, ensure_ascii=False, indent=2, default=_default)

    @staticmethod
    def _to_int(x: Any) -> Optional[int]:
        if x is None: return None
        try:
            if isinstance(x, float) and pd.isna(x): return None
            if isinstance(x, str) and x.strip() == "": return None
            return int(x)
        except Exception:
            return None

    def _pick_label_column(self, table: str, md: Dict[str, Any]) -> Optional[str]:
        cols = [c for c in md['columns'] if c.get('table_name') == table and not self._b(c.get('is_hidden'))]
        text_cols = [c for c in cols if any(k in (c.get('data_type') or '').lower() for k in ['text','string'])]
        # 优先匹配 Name/Title
        for c in text_cols:
            n = (c.get('column_name') or '').lower()
            if n.endswith('name') or n.endswith('title'): return c.get('column_name')
        for kw in ['country','region','area','site','queue','category','partner','product','language']:
            for c in text_cols:
                if kw in (c.get('column_name') or '').lower(): return c.get('column_name')
        return text_cols[0]['column_name'] if text_cols else (cols[0]['column_name'] if cols else None)

    @staticmethod
    def _expand_synonyms(label: str) -> List[str]:
        if not label: return []
        base = label.replace('_',' ').strip()
        variants: Set[str] = {base, base.lower(), base.title()}
        mapping = {
            'queue': ['队列','Queue','キュー'],
            'country': ['国家','Country','国'],
            'region': ['区域','Region','リージョン'],
            'area': ['地区','Area','エリア'],
            'site': ['站点','Site','サイト'],
            'partner': ['合作伙伴','Partner','パートナー'],
            'category': ['类别','Category','カテゴリ'],
            'product': ['产品','Product','プロダクト'],
            'language': ['语言','Language','言語']
        }
        low = base.lower()
        for k, words in mapping.items():
            if k in low: variants.update(words)
        return sorted(variants)

    def _suggest_group_by(self, fact: str, st: Dict[str, Any], md: Dict[str, Any]) -> List[str]:
        dims = st['star'].get(fact, {}).get('dimensions', [])
        res: List[str] = []
        for d in dims:
            t = d.get('dimension_table')
            if not t: continue
            label = self._pick_label_column(t, md)
            if label:
                res.append(f"{t}[{label}]")
        return res

    @staticmethod
    def _measure_category(dax: str) -> str:
        d = (dax or '').lower()
        if re.search(r'\bsumx?\(', d): return 'aggregation'
        if re.search(r'\b(distinctcount|count)\b', d): return 'counting'
        if re.search(r'\b(average|median|medianx|stdevx?|variance|percentile)\b', d): return 'statistical'
        if 'calculate(' in d: return 'filtered'
        if re.search(r'\b(dateadd|sameperiod|datesytd|datesmtd|datesqtd)\b', d): return 'time_intelligence'
        if '/' in d or 'divide(' in d: return 'calculation'
        return 'other'

    @staticmethod
    def _extract_measure_deps(dax: str) -> Dict[str, List[str]]:
        if not dax: return {"measures": [], "columns": []}
        col_pairs = re.findall(r"'([^']+)'\[([^\]]+)\]", dax)
        col_refs = {f"{t}[{c}]" for t, c in col_pairs}
        col_names = {c for _, c in col_pairs}
        measure_candidates = re.findall(r'\[([^\[\]]+)\]', dax)
        meas = sorted({m for m in measure_candidates if m not in col_names})
        return {"measures": meas, "columns": sorted(col_refs)}

    def _model_warnings(self, dimensions: Dict[str, Any]) -> List[str]:
        warns: List[str] = []
        # Queue 的双键提示（如果存在）
        if 'vwpcse_dimqueue' in dimensions:
            warns.append("DimQueue 可能存在 QueueKey/QueueID 双键，请优先统一为 QueueKey 或使用桥表。")
        return warns


# ----------------------------
# CLI / Demo
# ----------------------------
if __name__ == "__main__":
    # === 修改为你的模型与工作区 ===
    MODEL_NAME = "PCSE AI"       # 语义模型名称
    WORKSPACE_GUID = None        # 如需指定工作区 GUID；None 使用当前上下文
    # 生成设置
    INCLUDE_MEASURE_DAX = False  # True 会把所有可见度量 DAX 放进契约（可能很长）
    PROFILE_MODE = "light"       # "off" | "light" | "standard"
    REL_TOP_K = 12               # standard 模式下体检的边数量上限
    INCLUDE_ENUMS = False        # 如需枚举 Top 值，设 True
    MAX_ENUM_VALUES = 10
    OUTPUT_PATH = "llm_contract.json"
    # ===========================

    doc = LLMModelDocLite(verbose=True)
    contract_json = doc.generate(
        model_name=MODEL_NAME,
        workspace=WORKSPACE_GUID,
        include_measure_dax=INCLUDE_MEASURE_DAX,
        profile_mode=PROFILE_MODE,
        relationship_top_k=REL_TOP_K,
        include_enums=INCLUDE_ENUMS,
        max_enum_values=MAX_ENUM_VALUES
    )
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(contract_json)
    print(f"\n✅ LLM 契约已写入 {OUTPUT_PATH}")
