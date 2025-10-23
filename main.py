# -*- coding: utf-8 -*-
"""
Comprehensive Semantic Model Documentor (Fabric/Power BI) — robust edition (with data profiling)
- Uses INFO.VIEW.* where available; gracefully falls back for HIERARCHIES/ROLES
- Filters auto date tables in analysis & display
- Heuristic table typing with naming + structure signals (fixes facttask_* misclassification)
- Business-oriented DAX examples with data-driven time anchor (Submitted/Sent)
- Data freshness profiling (min/max/anchor/recent counts) and relationship quality checks
"""

from __future__ import annotations
import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Protocol, Any, Tuple
import pandas as pd

# ----------------------------
# Optional SDK import
# ----------------------------
try:
    import sempy.fabric as fabric  # Microsoft Fabric / Power BI Python SDK
    _FABRIC_AVAILABLE = True
except Exception:
    fabric = None
    _FABRIC_AVAILABLE = False


# ----------------------------
# Runner Abstraction (DI hook)
# ----------------------------
class DaxQueryRunner(Protocol):
    def evaluate(self, dataset: str, dax: str, workspace: Optional[str]) -> pd.DataFrame: ...


class FabricRunner:
    """Default runner using sempy.fabric.evaluate_dax"""
    def evaluate(self, dataset: str, dax: str, workspace: Optional[str]) -> pd.DataFrame:
        if not _FABRIC_AVAILABLE:
            raise RuntimeError("semPy/Fabric SDK not available. Please install `sempy` and run in Fabric-enabled env.")
        return fabric.evaluate_dax(dataset=dataset, dax_string=dax, workspace=workspace)


# ----------------------------
# Main Documentor
# ----------------------------
class ComprehensiveModelDocumentor:
    """
    完整的语义模型文档生成器
    目标：生成让任何人都能调用模型的详细文档（Markdown/JSON）
    """

    def __init__(self, runner: Optional[DaxQueryRunner] = None, verbose: bool = True):
        self.model_metadata: Dict[str, Any] = {}
        self.analysis_timestamp: str = datetime.utcnow().isoformat()
        self.runner = runner or FabricRunner()
        self.verbose = verbose
        self.filtered_auto_relationships: int = 0
        self.nl2dax_index: Dict[str, Any] = {}

    # ---------- Public API ----------
    def generate_complete_documentation(
        self,
        model_name: str,
        workspace: Optional[str] = None,
        output_format: str = 'markdown',
        profile_data: bool = True  # NEW: 默认做数据体检
    ) -> str:
        if self.verbose:
            print(f"📚 生成 {model_name} 的完整文档")
            print("=" * 60)

        # 1) 元数据
        if self.verbose: print("📊 步骤1: 提取完整元数据...")
        self.model_metadata = self._extract_complete_metadata(model_name, workspace)

        # 2) 结构分析
        if self.verbose: print("🔍 步骤2: 分析模型结构...")
        structure = self._analyze_model_structure(self.model_metadata)

        # 2.1) 数据体检（可选）
        profiles: Dict[str, Any] = {}
        rel_quality: Dict[str, Any] = {}
        if profile_data:
            if self.verbose: print("🩺 步骤2.1: 数据新鲜度与关系体检...")
            profiles = self._profile_data_health(model_name, workspace, self.model_metadata, structure)
            rel_quality = self._relationship_quality_checks(model_name, workspace, self.model_metadata)

        # 3) 示例
        if self.verbose: print("💡 步骤3: 生成DAX查询示例...")
        examples = self._generate_dax_examples(self.model_metadata, structure, profiles)

        # 4) 指南
        if self.verbose: print("📝 步骤4: 生成使用指南...")
        guide = self._generate_usage_guide(self.model_metadata, structure)

        # 5) 组装
        if self.verbose: print("📄 步骤5: 组装文档...")
        self.nl2dax_index = self._build_nl2dax_index(
            model_name=model_name,
            workspace=workspace,
            md=self.model_metadata,
            st=structure,
            profiles=profiles
        )

        if output_format.lower() == 'markdown':
            doc = self._build_markdown_document(model_name, self.model_metadata, structure, examples, guide,
                                                profiles=profiles, rel_quality=rel_quality)
        else:
            doc = self._build_json_document(model_name, self.model_metadata, structure, examples, guide,
                                            profiles=profiles, rel_quality=rel_quality)

        if self.verbose:
            print("✅ 文档生成完成！")
        return doc

    # ---------- Metadata ----------
    def _extract_complete_metadata(self, model_name: str, workspace: Optional[str]) -> Dict[str, Any]:
        md: Dict[str, Any] = {
            'tables': [], 'columns': [], 'measures': [], 'relationships': [],
            'hierarchies': [], 'roles': [], 'errors': []
        }

        # Primary queries (INFO.VIEW.*)
        queries_info: Dict[str, str] = {
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
                "description",[Description],
                "format_string",[FormatString],
                "sort_by_column",[SortByColumn]
            )""",
            'measures': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.MEASURES(),
                "table_name",[Table],
                "measure_name",[Name],
                "dax_expression",[Expression],
                "format_string",[FormatString],
                "is_hidden",[IsHidden],
                "description",[Description],
                "display_folder",[DisplayFolder]
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
                "cross_filter_direction",[CrossFilteringBehavior],
                "security_filtering",[SecurityFilteringBehavior]
            )""",
            'hierarchies': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.HIERARCHIES(),
                "table_name",[Table],
                "hierarchy_name",[Name],
                "levels",[Levels],
                "is_hidden",[IsHidden],
                "description",[Description]
            )""",
            'roles': """EVALUATE SELECTCOLUMNS(
                INFO.VIEW.ROLES(),
                "role_name",[Name],
                "description",[Description]
            )"""
        }

        # Fallback queries (TMSCHEMA_*). Keep minimal subset for hierarchies/roles.
        queries_fallback: Dict[str, str] = {
            'hierarchies': """EVALUATE SELECTCOLUMNS(
                TMSCHEMA_HIERARCHIES,
                "hierarchy_name",[Name],
                "is_hidden",[IsHidden],
                "description",[Description]
            )""",
            'roles': """EVALUATE SELECTCOLUMNS(
                TMSCHEMA_ROLES,
                "role_name",[Name],
                "description",[Description]
            )"""
        }

        def run_with_fallback(key: str) -> List[Dict[str, Any]]:
            prefer = queries_info.get(key)
            fallback = queries_fallback.get(key)

            if prefer:
                try:
                    df = self.runner.evaluate(model_name, prefer, workspace)
                    return self._normalize_dataframe(df).to_dict('records')
                except Exception:
                    if key in queries_fallback and fallback:
                        try:
                            df2 = self.runner.evaluate(model_name, fallback, workspace)
                            return self._normalize_dataframe(df2).to_dict('records')
                        except Exception:
                            md['errors'].append(f"{key} not available (INFO.VIEW & TMSCHEMA failed)")
                            if self.verbose:
                                print(f"  ℹ {key}: 不可用（已忽略）")
                            return []
                    else:
                        md['errors'].append(f"{key} not available (INFO.VIEW failed)")
                        if self.verbose:
                            print(f"  ℹ {key}: 不可用（已忽略）")
                        return []
            else:
                if self.verbose:
                    print(f"  ℹ {key}: 无查询定义（已忽略）")
                return []

        for k in ['tables', 'columns', 'measures', 'relationships']:
            records = run_with_fallback(k)
            md[k] = records
            if self.verbose:
                print(f"  ✓ 提取了 {len(records)} 个 {k}")

        for k in ['hierarchies', 'roles']:
            records = run_with_fallback(k)
            md[k] = records
            if records and self.verbose:
                print(f"  ✓ 提取了 {len(records)} 个 {k}")

        auto_date_pattern = re.compile(r'^(LocalDateTable_|DateTableTemplate_)', re.IGNORECASE)
        md['auto_date_tables'] = [
            t.get('table_name', '') for t in md['tables']
            if auto_date_pattern.match(t.get('table_name', '') or '')
        ]
        md['business_tables'] = [
            t for t in md['tables']
            if not auto_date_pattern.match(t.get('table_name', '') or '')
            and not self._safe_bool(t.get('is_hidden'))
        ]
        return md

    @staticmethod
    def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [
            (col or '').strip().replace('[', '').replace(']', '').lower().replace(' ', '_')
            for col in df.columns
        ]
        return df

    # ---------- Analysis ----------
    def _analyze_model_structure(self, md: Dict[str, Any]) -> Dict[str, Any]:
        analysis: Dict[str, Any] = {
            'table_types': {},
            'star_schema': {},
            'key_relationships': [],
            'fact_time_axes': {},
            'measure_summary': {}
        }

        # classify
        for t in md.get('business_tables', []):
            name = t.get('table_name', '')
            analysis['table_types'][name] = self._classify_table(name, md)

        # star schema (exclude auto-date)
        fact_tables = [n for n, t in analysis['table_types'].items() if t == 'fact']
        dim_tables = [n for n, t in analysis['table_types'].items() if t == 'dimension']

        for fact in fact_tables:
            analysis['star_schema'][fact] = {'dimensions': [], 'relationships': []}
            for rel in md.get('relationships', []):
                if not self._safe_bool(rel.get('is_active')): continue
                if self._is_auto_date_table(rel.get('from_table')) or self._is_auto_date_table(rel.get('to_table')): continue
                if rel.get('from_table') == fact and rel.get('to_table') in dim_tables:
                    analysis['star_schema'][fact]['dimensions'].append({
                        'dimension_table': rel.get('to_table'),
                        'join_key': f"{rel.get('from_column')} → {rel.get('to_column')}",
                        'cardinality': f"{rel.get('from_cardinality')}-{rel.get('to_cardinality')}"
                    })

        # key relationships (exclude auto-date)
        for rel in md.get('relationships', []):
            if not self._safe_bool(rel.get('is_active')): continue
            fr, to = rel.get('from_table', ''), rel.get('to_table', '')
            if self._is_auto_date_table(fr) or self._is_auto_date_table(to): continue
            analysis['key_relationships'].append({
                'from': f"{fr}[{rel.get('from_column')}]",
                'to': f"{to}[{rel.get('to_column')}]",
                'type': self._determine_relationship_type(rel),
                'filter_direction': rel.get('cross_filter_direction', 'Single')
            })

        for fact in fact_tables:
            key_info = self._detect_default_time_key(fact, md)
            default_date_column = None
            fact_columns = [
                column for column in md.get('columns', [])
                if column.get('table_name') == fact and 'date' in (column.get('data_type') or '').lower()
            ]
            if fact_columns:
                default_date_column = sorted(
                    [column.get('column_name') for column in fact_columns],
                    key=lambda name: (0 if name and 'closed' in name.lower() else 1)
                )[0]
            analysis['fact_time_axes'][fact] = {
                'default_time_key': key_info[0] if key_info else None,
                'date_dimension': key_info[1] if key_info else None,
                'date_dimension_key': key_info[2] if key_info else None,
                'default_time_column': default_date_column,
                'has_date_axis': bool(key_info)
            }

        # measures summary
        analysis['measure_summary'] = self._analyze_measures(md.get('measures', []))
        return analysis

    def _classify_table(self, table_name: str, md: Dict[str, Any]) -> str:
        name_lc = (table_name or '').lower()
        cols = [c for c in md.get('columns', []) if c.get('table_name') == table_name]
        meas = [m for m in md.get('measures', []) if m.get('table_name') == table_name]

        outgoing = sum(
            1 for r in md.get('relationships', [])
            if r.get('from_table') == table_name and self._safe_bool(r.get('is_active'))
            and not self._is_auto_date_table(r.get('to_table'))
        )
        incoming = sum(
            1 for r in md.get('relationships', [])
            if r.get('to_table') == table_name and self._safe_bool(r.get('is_active'))
            and not self._is_auto_date_table(r.get('from_table'))
        )

        numeric_cols = sum(1 for c in cols if any(t in (c.get('data_type') or '').lower()
                        for t in ['int','integer','decimal','double','number']))
        text_cols = sum(1 for c in cols if any(t in (c.get('data_type') or '').lower()
                        for t in ['text','string']))

        # Naming strong hint (fix for facttask_* tables)
        if name_lc.startswith('vwpcse_fact') or name_lc.startswith('fact'):
            if len(cols) <= 3 and outgoing >= 2 and incoming >= 2:
                return 'bridge'
            return 'fact'

        # date-dimension priority
        if self._looks_like_date_dimension(table_name, md.get('columns', []), md.get('measures', [])):
            return 'dimension'

        # structural signals
        if outgoing >= 2:
            return 'fact'
        if incoming > outgoing or text_cols > numeric_cols:
            return 'dimension'
        if cols and len(cols) <= 3 and outgoing >= 2:
            return 'bridge'
        return 'other'

    def _looks_like_date_dimension(self, table_name: str, columns: List[Dict[str, Any]], measures: List[Dict[str, Any]]) -> bool:
        name_lc = (table_name or '').lower()
        pass_name = any(k in name_lc for k in ['dimdate', 'date', 'calendar'])
        col_types = [(c.get('data_type') or '').lower() for c in columns if c.get('table_name') == table_name]
        has_many_date_like = sum('date' in t for t in col_types) >= 2
        has_few_measures = sum(m.get('table_name') == table_name for m in measures) <= 1
        return (pass_name or has_many_date_like) and has_few_measures

    @staticmethod
    def _determine_relationship_type(rel: Dict[str, Any]) -> str:
        fc = (rel.get('from_cardinality') or '').lower()
        tc = (rel.get('to_cardinality') or '').lower()
        if fc == 'many' and tc == 'one': return '多对一'
        if fc == 'one' and tc == 'many': return '一对多'
        if fc == 'one' and tc == 'one':  return '一对一'
        return '多对多'

    def _analyze_measures(self, measures: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {'total_count': 0, 'by_category': {}, 'complex_measures': []}
        visible = [m for m in measures if not self._safe_bool(m.get('is_hidden'))]
        summary['total_count'] = len(visible)

        def add(cat: str, name: str):
            summary['by_category'].setdefault(cat, []).append(name)

        for m in visible:
            name = m.get('measure_name', '')
            dax = (m.get('dax_expression') or '')
            dax_l = dax.lower()

            if re.search(r'\bsumx?\(', dax_l):
                cat = 'aggregation'
            elif re.search(r'\b(distinctcount|count)\b', dax_l):
                cat = 'counting'
            elif re.search(r'\b(average|median|medianx|stdevx?|variance|percentilex?\.(inc|exc))\b', dax_l):
                cat = 'statistical'
            elif re.search(r'\bcalculate\(', dax_l):
                cat = 'filtered'
            elif re.search(r'\b(dateadd|sameperiod|datesytd)\b', dax_l):
                cat = 'time_intelligence'
            elif '/' in dax_l or re.search(r'\bdivide\(', dax_l):
                cat = 'calculation'
            else:
                cat = 'other'
            add(cat, name)

            if len(dax) > 200 or dax.count('(') > 5:
                summary['complex_measures'].append(name)

        return summary

    # ---------- Data profiling ----------
    def _profile_data_health(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any],
        st: Dict[str, Any]
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {'time_anchors': {}, 'facts_rowcount': {}}

        # Fact tables row count
        fact_tables = [n for n, t in st.get('table_types', {}).items() if t == 'fact']
        for t in fact_tables:
            dax = f"""EVALUATE ROW("row_count", COUNTROWS('{t}'))"""
            try:
                df = self.runner.evaluate(model_name, dax, workspace)
                rc = int(df.iloc[0, 0]) if not df.empty else None
            except Exception:
                rc = None
            result['facts_rowcount'][t] = rc

        # Time anchors per fact
        for t in fact_tables:
            anchor = self._profile_time_anchor_for_table(model_name, workspace, md, t)
            result['time_anchors'][t] = anchor
        return result

    def _detect_default_time_key(
        self,
        fact_table: str,
        md: Dict[str, Any]
    ) -> Optional[Tuple[str, str, str]]:
        """检测事实表到日期维度的键列, 返回 (事实键列, 日期维度表, 日期维度键列)"""
        if not fact_table:
            raise ValueError("fact_table 参数不能为空")
        for relationship in md.get('relationships', []):
            if not self._is_business_relationship(relationship):
                continue
            if relationship.get('from_table') != fact_table:
                continue
            to_table = relationship.get('to_table')
            if not to_table:
                continue
            if 'dimdate' in to_table.lower() or 'calendar' in to_table.lower():
                from_column = relationship.get('from_column')
                to_column = relationship.get('to_column') or 'DateKey'
                if from_column:
                    return (from_column, to_table, to_column)
        return None

    def _select_dim_date_column(self, dim_table: str, md: Dict[str, Any]) -> Optional[str]:
        """选择日期维度表中作为默认日期轴的列"""
        if not dim_table:
            return None
        candidates: List[str] = []
        for column in md.get('columns', []):
            if column.get('table_name') != dim_table:
                continue
            data_type = (column.get('data_type') or '').lower()
            if 'date' not in data_type and 'time' not in data_type:
                continue
            candidates.append(column.get('column_name'))
        if not candidates:
            return None
        for preferred in ['CalendarDate', 'Date', 'DateValue']:
            if preferred in candidates:
                return preferred
        return candidates[0]

    def _select_dimension_label(self, table_name: str, md: Dict[str, Any]) -> Optional[str]:
        """选择维度表中最合适的展示列"""
        if not table_name:
            return None
        candidates: List[str] = []
        for column in md.get('columns', []):
            if column.get('table_name') != table_name:
                continue
            if self._safe_bool(column.get('is_hidden')):
                continue
            if (column.get('data_type') or '').lower() not in ['text', 'string']:
                continue
            candidates.append(column.get('column_name'))
        for candidate in candidates:
            lowered = (candidate or '').lower()
            if lowered.endswith('name') or lowered.endswith('title'):
                return candidate
        return candidates[0] if candidates else None

    def _profile_time_anchor_for_table(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any],
        table: str
    ) -> Dict[str, Any]:
        def _dtype_is_date_like(data_type: str) -> bool:
            lowered = (data_type or '').lower()
            return any(flag in lowered for flag in ['date', 'datetime', 'timestamp', 'time'])

        def _score(column_name: str) -> float:
            lowered = (column_name or '').lower()
            if 'submitted' in lowered: return 6
            if 'sent' in lowered:      return 5
            if 'closed' in lowered:    return 4
            if 'created' in lowered:   return 3.5
            if 'resolved' in lowered:  return 3.2
            if 'calendar' in lowered:  return 3
            if 'date' in lowered:      return 2
            return 1

        date_columns = [
            column.get('column_name')
            for column in md.get('columns', [])
            if column.get('table_name') == table and _dtype_is_date_like(column.get('data_type'))
        ]
        date_columns = sorted(date_columns, key=_score, reverse=True)

        def _profile_on_date_column(column_name: str) -> Optional[Dict[str, Any]]:
            dax = f"""
EVALUATE
VAR _min = CALCULATE(MIN('{table}'[{column_name}]))
VAR _max = CALCULATE(MAX('{table}'[{column_name}]))
VAR _nonblank = COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{column_name}])))
VAR _cnt7  = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{column_name}]) && '{table}'[{column_name}] > _max - 7  && '{table}'[{column_name}] <= _max)), BLANK())
VAR _cnt30 = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{column_name}]) && '{table}'[{column_name}] > _max - 30 && '{table}'[{column_name}] <= _max)), BLANK())
VAR _cnt90 = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{column_name}]) && '{table}'[{column_name}] > _max - 90 && '{table}'[{column_name}] <= _max)), BLANK())
RETURN
ROW("column","{column_name}","min",_min,"max",_max,"anchor",_max,"nonblank",_nonblank,"cnt7",_cnt7,"cnt30",_cnt30,"cnt90",_cnt90)
"""
            df_result = self.runner.evaluate(dataset=model_name, dax=dax, workspace=workspace)
            if df_result.empty:
                return None
            record = df_result.iloc[0].to_dict()
            anchor_value = record.get('anchor')
            if pd.isna(anchor_value):
                return None
            return {
                'anchor_column': record.get('column'),
                'min': record.get('min'),
                'max': record.get('max'),
                'anchor': anchor_value,
                'nonblank': int(record.get('nonblank')) if pd.notna(record.get('nonblank')) else None,
                'cnt7': int(record.get('cnt7')) if pd.notna(record.get('cnt7')) else None,
                'cnt30': int(record.get('cnt30')) if pd.notna(record.get('cnt30')) else None,
                'cnt90': int(record.get('cnt90')) if pd.notna(record.get('cnt90')) else None
            }

        for candidate in date_columns:
            try:
                profiled = _profile_on_date_column(candidate)
                if profiled:
                    return profiled
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 日期列 {table}[{candidate}] 锚点探测失败: {error}")

        key_info = self._detect_default_time_key(table, md)
        if key_info:
            fact_key, dim_table, dim_key = key_info
            dim_date_column = self._select_dim_date_column(dim_table, md)
            if dim_date_column:
                dax_key = f"""
EVALUATE
VAR _validRows = FILTER('{table}', NOT ISBLANK('{table}'[{fact_key}]))
VAR _anchorDate =
    CALCULATE(
        MAX('{dim_table}'[{dim_date_column}]),
        TREATAS(VALUES('{table}'[{fact_key}]), '{dim_table}'[{dim_key}])
    )
VAR _minDate =
    CALCULATE(
        MIN('{dim_table}'[{dim_date_column}]),
        TREATAS(VALUES('{table}'[{fact_key}]), '{dim_table}'[{dim_key}])
    )
VAR _cnt7 =
    IF(
        NOT ISBLANK(_anchorDate),
        CALCULATE(
            COUNTROWS(_validRows),
            FILTER(
                _validRows,
                RELATED('{dim_table}'[{dim_date_column}]) > _anchorDate - 7 &&
                RELATED('{dim_table}'[{dim_date_column}]) <= _anchorDate
            )
        )
    )
VAR _cnt30 =
    IF(
        NOT ISBLANK(_anchorDate),
        CALCULATE(
            COUNTROWS(_validRows),
            FILTER(
                _validRows,
                RELATED('{dim_table}'[{dim_date_column}]) > _anchorDate - 30 &&
                RELATED('{dim_table}'[{dim_date_column}]) <= _anchorDate
            )
        )
    )
VAR _cnt90 =
    IF(
        NOT ISBLANK(_anchorDate),
        CALCULATE(
            COUNTROWS(_validRows),
            FILTER(
                _validRows,
                RELATED('{dim_table}'[{dim_date_column}]) > _anchorDate - 90 &&
                RELATED('{dim_table}'[{dim_date_column}]) <= _anchorDate
            )
        )
    )
RETURN
ROW(
    "column", "{fact_key}",
    "min", _minDate,
    "max", _anchorDate,
    "anchor", _anchorDate,
    "nonblank", COUNTROWS(_validRows),
    "cnt7", _cnt7,
    "cnt30", _cnt30,
    "cnt90", _cnt90
)
"""
                try:
                    df_key = self.runner.evaluate(dataset=model_name, dax=dax_key, workspace=workspace)
                    if not df_key.empty:
                        record = df_key.iloc[0].to_dict()
                        return {
                            'anchor_column': record.get('column'),
                            'min': record.get('min'),
                            'max': record.get('max'),
                            'anchor': record.get('anchor'),
                            'nonblank': int(record.get('nonblank')) if pd.notna(record.get('nonblank')) else None,
                            'cnt7': int(record.get('cnt7')) if pd.notna(record.get('cnt7')) else None,
                            'cnt30': int(record.get('cnt30')) if pd.notna(record.get('cnt30')) else None,
                            'cnt90': int(record.get('cnt90')) if pd.notna(record.get('cnt90')) else None,
                            'anchor_via_key': True,
                            'date_dimension': dim_table,
                            'date_axis_column': dim_date_column
                        }
                except Exception as error:
                    if self.verbose:
                        print(f"⚠️ 键列 {table}[{fact_key}] 锚点探测失败: {error}")

        return {
            'anchor_column': date_columns[0] if date_columns else None,
            'min': None,
            'max': None,
            'anchor': None,
            'nonblank': None,
            'cnt7': None,
            'cnt30': None,
            'cnt90': None
        }

    # ---------- Relationship quality checks ----------
    def _relationship_quality_checks(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any]
    ) -> Dict[str, Any]:
        """关系质量体检, 返回摘要、明细以及Lint信息"""
        lints: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        summary: List[Dict[str, Any]] = []
        severity_order: Dict[str, int] = {'red': 0, 'yellow': 1, 'green': 2}
        self.filtered_auto_relationships = 0

        col_type: Dict[Tuple[str, str], str] = {}
        for column in md.get('columns', []):
            key = (column.get('table_name'), column.get('column_name'))
            col_type[key] = (column.get('data_type') or '').lower()

        to_table_groups: Dict[str, set] = {}
        for relationship in md.get('relationships', []):
            if not self._safe_bool(relationship.get('is_active')):
                continue
            to_table = relationship.get('to_table')
            to_column = relationship.get('to_column')
            if not to_table or not to_column:
                continue
            to_table_groups.setdefault(to_table, set()).add(to_column)

        if 'vwpcse_dimqueue' in to_table_groups:
            queue_columns = {column.lower() for column in to_table_groups['vwpcse_dimqueue']}
            if 'queuekey' in queue_columns and 'queueid' in queue_columns:
                lints.append({'type': 'lint', 'message': 'Queue 维度存在 QueueKey 与 QueueID 并行连接；建议统一代理键或加桥表。'})

        for relationship in md.get('relationships', []):
            if self._is_auto_date_table(relationship.get('from_table')) or self._is_auto_date_table(relationship.get('to_table')):
                self.filtered_auto_relationships += 1
            if not self._is_business_relationship(relationship):
                continue

            from_table = relationship.get('from_table')
            from_column = relationship.get('from_column')
            to_table = relationship.get('to_table')
            to_column = relationship.get('to_column')
            if not from_table or not from_column or not to_table or not to_column:
                continue

            dtype_from = (col_type.get((from_table, from_column)) or '')
            dtype_to = (col_type.get((to_table, to_column)) or '')
            can_check_orphan = bool(dtype_from) and bool(dtype_to) and dtype_from.split()[0] == dtype_to.split()[0]

            dax_rows = (
                f"""
EVALUATE
ROW(
    "blank_fk", COUNTROWS(FILTER('{from_table}', ISBLANK('{from_table}'[{from_column}]))),
    "total_rows", COUNTROWS('{from_table}'),
    "distinct_fk", DISTINCTCOUNT('{from_table}'[{from_column}])
)
"""
            )

            blank_fk = None
            total_rows = None
            distinct_fk = None
            try:
                df_rows = self.runner.evaluate(dataset=model_name, dax=dax_rows, workspace=workspace)
                if not df_rows.empty:
                    blank_fk = int(df_rows.iloc[0].get('blank_fk'))
                    total_rows = int(df_rows.iloc[0].get('total_rows'))
                    distinct_fk = int(df_rows.iloc[0].get('distinct_fk'))
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 无法计算 {from_table}[{from_column}] 的空值统计: {error}")

            orphan_fk = None
            if can_check_orphan:
                dax_orphan = (
                    f"""
EVALUATE
ROW(
    "orphan_fk",
    COUNTROWS(
        EXCEPT(
            VALUES('{from_table}'[{from_column}]),
            VALUES('{to_table}'[{to_column}])
        )
    )
)
"""
                )
                try:
                    df_orphan = self.runner.evaluate(dataset=model_name, dax=dax_orphan, workspace=workspace)
                    if not df_orphan.empty:
                        orphan_fk = int(df_orphan.iloc[0].get('orphan_fk'))
                except Exception as error:
                    if self.verbose:
                        print(f"⚠️ 无法计算 {from_table}[{from_column}] → {to_table}[{to_column}] 的孤儿键: {error}")
            else:
                lints.append({
                    'type': 'lint',
                    'message': f"关系 {from_table}[{from_column}] → {to_table}[{to_column}] 两端数据类型不同（{dtype_from} vs {dtype_to}），无法做孤儿检查，建议统一类型。"
                })

            blank_ratio = None
            orphan_ratio = None
            coverage = None
            if blank_fk is not None and total_rows:
                blank_ratio = blank_fk / total_rows if total_rows else None
            if orphan_fk is not None and distinct_fk:
                orphan_ratio = orphan_fk / distinct_fk if distinct_fk else None
            if orphan_ratio is not None:
                coverage = 1 - orphan_ratio

            severity = 'green'
            indicator = 0.0
            if (coverage is not None and coverage < 0.95) or (blank_ratio is not None and blank_ratio > 0.05):
                severity = 'red'
            elif (coverage is not None and coverage < 0.98) or (blank_ratio is not None and blank_ratio > 0.02):
                severity = 'yellow'
            if blank_ratio is not None:
                indicator = max(indicator, blank_ratio)
            if coverage is not None:
                indicator = max(indicator, 1 - coverage)

            detail_entry = {
                'from': f"{from_table}[{from_column}]",
                'to': f"{to_table}[{to_column}]",
                'blank_fk': blank_fk,
                'orphan_fk': orphan_fk,
                'blank_ratio': blank_ratio,
                'coverage': coverage,
                'severity': severity
            }
            details.append(detail_entry)

            summary.append({
                **detail_entry,
                'score': indicator
            })

        summary_sorted = sorted(
            summary,
            key=lambda item: (severity_order.get(item.get('severity'), 3), -(item.get('score') or 0.0))
        )
        top_summary = summary_sorted[:10]

        return {
            'summary': top_summary,
            'details': details,
            'lints': lints,
            'filtered_auto_relationships': self.filtered_auto_relationships
        }

    # ---------- Examples & Guide ----------
    def _generate_dax_examples(self, md: Dict[str, Any], st: Dict[str, Any], profiles: Dict[str, Any]) -> List[Dict[str, Any]]:
        examples: List[Dict[str, Any]] = []
        table_types = st.get('table_types', {})
        fact_tables = [n for n, t in table_types.items() if t == 'fact']
        dim_tables  = [n for n, t in table_types.items() if t == 'dimension']

        # pick a fact & a dimension text column
        fact = None
        # Prefer customer survey fact if exists
        preferred = ['vwpcse_factcustomersurvey', 'vwpcse_factincident_closed', 'vwpcse_factincident_created']
        for p in preferred:
            if p in fact_tables:
                fact = p; break
        if not fact and fact_tables:
            fact = fact_tables[0]

        # first visible measure
        vis_measures = [m for m in md.get('measures', []) if not self._safe_bool(m.get('is_hidden'))]
        first_m = vis_measures[0] if vis_measures else None

        # Example 1: Single measure
        if first_m:
            examples.append({
                'title': '获取单个度量值',
                'description': '查询一个度量值的总值',
                'dax': f"EVALUATE\nROW(\"结果\", [{first_m['measure_name']}])",
                'category': 'basic'
            })

        # Example 2: TOPN head of a fact
        if fact:
            examples.append({
                'title': f'查看事实表{fact}前10行',
                'description': '获取事实表的前10行数据',
                'dax': f"EVALUATE\nTOPN(10, '{fact}')",
                'category': 'basic'
            })

        date_axis_table = None
        date_axis_column = None
        date_axis_key = None
        for payload in st.get('fact_time_axes', {}).values():
            if payload.get('has_date_axis'):
                date_axis_table = payload.get('date_dimension')
                date_axis_key = payload.get('date_dimension_key')
                date_axis_column = self._select_dim_date_column(date_axis_table, md)
                break
        if not date_axis_table:
            date_axis_table = 'vwpcse_dimdate'
            date_axis_key = 'DateKey'
            date_axis_column = self._select_dim_date_column(date_axis_table, md)

        def _build_anchor_expression(fact_name: str) -> Tuple[str, str]:
            anchor_info = (profiles or {}).get('time_anchors', {}).get(fact_name, {})
            anchor_column = anchor_info.get('anchor_column')
            if not anchor_column:
                return '', ''
            if anchor_info.get('anchor_via_key'):
                dim_table = anchor_info.get('date_dimension') or date_axis_table
                dim_date_col = anchor_info.get('date_axis_column') or date_axis_column
                dim_key_col = date_axis_key
                expression = (
                    f"CALCULATE(MAX('{dim_table}'[{dim_date_col}]), "
                    f"TREATAS(VALUES('{fact_name}'[{anchor_column}]), '{dim_table}'[{dim_key_col}]))"
                )
                return anchor_column, expression
            return anchor_column, f"MAX('{fact_name}'[{anchor_column}])"

        incident_fact = 'vwpcse_factincident_closed'
        if incident_fact in fact_tables and date_axis_table and date_axis_column:
            _, anchor_expr_incident = _build_anchor_expression(incident_fact)
            country_label = self._select_dimension_label('vwpcse_dimgeography', md) or 'Country'
            dax_active = f"""EVALUATE
VAR AnchorDate = {anchor_expr_incident or f"MAX('{incident_fact}'[Case Closed Date])"}
VAR Period = DATESINPERIOD('{date_axis_table}'[{date_axis_column}], AnchorDate, -90, DAY)
RETURN
TOPN(
  10,
  SUMMARIZECOLUMNS(
    'vwpcse_dimgeography'['{country_label}'],
    Period,
    "# Closed", [# Case Closed]
  ),
  [# Closed], DESC
)"""
            examples.append({
                'title': '按国家查看关闭工单（活动关系 + 默认日期轴）',
                'description': '默认 DimDate[CalendarDate] + 活动关系，AnchorDate 取最近关闭日',
                'dax': dax_active,
                'category': 'ranking'
            })

        survey_fact = 'vwpcse_factcustomersurvey'
        if survey_fact in fact_tables and 'vwpcse_dimqueue' in dim_tables and date_axis_table and date_axis_column:
            queue_label = self._select_dimension_label('vwpcse_dimqueue', md) or 'Queue Name'
            anchor_col, anchor_expr_survey = _build_anchor_expression(survey_fact)
            anchor_expr_survey = anchor_expr_survey or f"MAX('{survey_fact}'[{anchor_col or 'SubmittedDate'}])"
            dax_active_queue = f"""EVALUATE
VAR AnchorDate = {anchor_expr_survey}
VAR Window = DATESINPERIOD('{date_axis_table}'[{date_axis_column}], AnchorDate, -90, DAY)
RETURN
TOPN(
  20,
  SUMMARIZECOLUMNS(
    'vwpcse_dimqueue'['{queue_label}'],
    Window,
    "Median CSAT", [Median CSAT]
  ),
  [Median CSAT], DESC
)"""
            dax_treatas = f"""EVALUATE
VAR AnchorDate = {anchor_expr_survey}
VAR Window = DATESINPERIOD('{date_axis_table}'[{date_axis_column}], AnchorDate, -90, DAY)
RETURN
TOPN(
  20,
  ADDCOLUMNS(
    VALUES('vwpcse_dimqueue'['{queue_label}']),
    "Median CSAT", CALCULATE([Median CSAT], TREATAS(Window, '{survey_fact}'[{anchor_col or 'SubmittedDate'}]))
  ),
  [Median CSAT], DESC
)"""
            examples.append({
                'title': '队列的 Median CSAT（活动关系写法）',
                'description': '默认 DimDate 日期轴 + 数据锚点，直接利用活动关系',
                'dax': dax_active_queue,
                'category': 'time_series'
            })
            examples.append({
                'title': '队列的 Median CSAT（TREATAS 写法）',
                'description': '当事实表日期列未与 DimDate 建立活动关系时，使用 TREATAS 应用窗口',
                'dax': dax_treatas,
                'category': 'time_series'
            })

        # Example 5: Basic filter example using CALCULATE
        if fact and first_m:
            # pick a text column on fact
            text_c = next((c for c in md.get('columns', [])
                           if c.get('table_name') == fact and any(t in (c.get('data_type') or '').lower()
                                                                 for t in ['text','string'])), None)
            if text_c:
                examples.append({
                    'title': '条件筛选（CALCULATE）',
                    'description': '对事实表文本列做条件筛选',
                    'dax': f"""EVALUATE
ROW(
    "筛选结果",
    CALCULATE(
        [{first_m['measure_name']}],
        '{text_c["table_name"]}'['{text_c["column_name"]}'] = "示例值"
    )
)""",
                    'category': 'filtering'
                })

        return examples

    def _generate_usage_guide(self, md: Dict[str, Any], st: Dict[str, Any]) -> Dict[str, Any]:
        fact_tables = [n for n, t in st.get('table_types', {}).items() if t == 'fact']
        guide = {
            'quick_start': [
                "1. 连接到Power BI语义模型",
                "2. 使用表名和列名时注意大小写",
                "3. 度量值使用方括号引用: [度量值名称]",
                "4. 表和列使用单引号: '表名'[列名]"
            ],
            'common_patterns': [],
            'best_practices': [
                "优先使用已定义的度量值而不是重新计算",
                "利用关系进行跨表查询，避免手动JOIN",
                "使用CALCULATE进行上下文转换",
                "对大数据集使用TOPN限制结果集",
                "示例中的时间窗口默认使用数据锚点（最近可用日期），可改为上月/上季等固定窗口"
            ],
            'troubleshooting': [
                "错误: 找不到列 → 检查列名大小写和拼写",
                "错误: 循环依赖 → 检查关系设置",
                "性能问题 → 考虑使用聚合表或优化度量值",
                "窗口内无数据 → 使用数据锚点或放宽时间窗，并检查关系是否为活动/需要TREATAS"
            ]
        }
        if fact_tables:
            guide['common_patterns'].append("主要分析基于事实表: " + ", ".join(fact_tables))
        if any(payload.get('has_date_axis') for payload in st.get('fact_time_axes', {}).values()):
            guide['common_patterns'].append("使用 DimDate 日期轴 + 数据锚点进行时间序列分析")
        return guide

    def _build_nl2dax_index(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any],
        st: Dict[str, Any],
        profiles: Dict[str, Any]
    ) -> Dict[str, Any]:
        """构建 NL2DAX 索引, 输出重点信息供模型自动问答使用"""
        fact_time_axes = st.get('fact_time_axes', {})
        default_dim_table = None
        default_dim_key = None
        default_dim_date_column = None
        for fact_name, payload in fact_time_axes.items():
            if payload.get('has_date_axis'):
                default_dim_table = payload.get('date_dimension')
                default_dim_key = payload.get('date_dimension_key')
                default_dim_date_column = self._select_dim_date_column(default_dim_table, md)
                if default_dim_table and default_dim_key:
                    break
        if not default_dim_table:
            default_dim_table = 'vwpcse_dimdate'
            default_dim_key = 'DateKey'
            default_dim_date_column = self._select_dim_date_column(default_dim_table, md)

        date_axis = {
            'table': default_dim_table,
            'date_column': default_dim_date_column,
            'key_column': default_dim_key
        }

        facts: Dict[str, Any] = {}
        profiles = profiles or {}
        time_anchors = profiles.get('time_anchors', {})
        facts_rowcount = profiles.get('facts_rowcount', {})
        for fact_name, payload in fact_time_axes.items():
            anchor_profile = time_anchors.get(fact_name, {})
            facts[fact_name] = {
                'grain': 'incident' if 'incident' in fact_name else ('task' if 'task' in fact_name else 'fact'),
                'default_time_key': payload.get('default_time_key'),
                'default_time_column': anchor_profile.get('anchor_column') or payload.get('default_time_column'),
                'anchor_strategy': 'max(date_column) fallback max(date by key)',
                'row_count': facts_rowcount.get(fact_name)
            }

        dimensions: Dict[str, Any] = {}
        for table in md.get('business_tables', []):
            table_name = table.get('table_name')
            if st.get('table_types', {}).get(table_name) != 'dimension':
                continue
            columns = [column for column in md.get('columns', []) if column.get('table_name') == table_name and not self._safe_bool(column.get('is_hidden'))]
            primary_key = next((column.get('column_name') for column in columns if self._safe_bool(column.get('is_key')) or self._safe_bool(column.get('is_unique'))), None)
            label_column = None
            text_columns = [column for column in columns if (column.get('data_type') or '').lower() in ['string', 'text']]
            for column in text_columns:
                name = column.get('column_name') or ''
                if name.lower().endswith('name') or name.lower().endswith('title'):
                    label_column = name
                    break
            if not label_column and text_columns:
                label_column = text_columns[0].get('column_name')
            natural_key = next((column.get('column_name') for column in columns if column.get('column_name') and column.get('column_name').lower().endswith(('id', 'code'))), None)
            friendly_name = table_name.replace('vwpcse_', '') if table_name else ''
            synonyms = [friendly_name.replace('_', ' ')] if friendly_name else []
            dimensions[table_name] = {
                'primary_key': primary_key,
                'natural_key': natural_key,
                'label': label_column,
                'synonyms': synonyms
            }
        if 'vwpcse_dimqueue' in dimensions:
            dimensions['vwpcse_dimqueue']['label'] = dimensions['vwpcse_dimqueue'].get('label') or 'Queue Name'
            dimensions['vwpcse_dimqueue']['synonyms'] = ['队列', 'Queue', '队列名称']
            dimensions['vwpcse_dimqueue']['join_recommendation'] = 'Prefer QueueKey; QueueID only for Task facts'

        relationships: List[Dict[str, Any]] = []
        for relationship in md.get('relationships', []):
            if not self._is_business_relationship(relationship):
                continue
            relationships.append({
                'from': f"{relationship.get('from_table')}[{relationship.get('from_column')}]",
                'to': f"{relationship.get('to_table')}[{relationship.get('to_column')}]",
                'direction': relationship.get('cross_filter_direction', 'Single')
            })

        measures: Dict[str, Any] = {}
        category_map = st.get('measure_summary', {}).get('by_category', {})
        visible_measures = [measure for measure in md.get('measures', []) if not self._safe_bool(measure.get('is_hidden'))]
        for measure in visible_measures:
            measure_name = measure.get('measure_name')
            if not measure_name:
                continue
            category = 'other'
            for cat_name, measure_list in category_map.items():
                if measure_name in measure_list:
                    category = cat_name
                    break
            format_string = measure.get('format_string') or ''
            unit = 'ratio' if '%' in format_string else ('count' if measure_name.startswith('#') or measure_name.lower().startswith('count') else 'value')
            measures[measure_name] = {
                'category': category,
                'unit': unit,
                'fact_hint': measure.get('table_name')
            }

        enums: Dict[str, List[Any]] = {}
        enum_candidates = [
            ('vwpcse_factescalationcase', 'EscalationTier'),
            ('vwpcse_facttask_created', 'TaskType'),
            ('vwpcse_factincident_closed', 'Case State')
        ]
        for table_name, column_name in enum_candidates:
            dax = (
                f"""
EVALUATE
TOPN(
    10,
    ADDCOLUMNS(
        SUMMARIZE('{table_name}', '{table_name}'[{column_name}]),
        "_order", '{table_name}'[{column_name}]
    ),
    [_order], ASC
)
"""
            )
            try:
                df_enum = self.runner.evaluate(dataset=model_name, dax=dax, workspace=workspace)
                if df_enum.empty:
                    continue
                values = [row[0] for row in df_enum.iloc[:, :1].values.tolist() if row and row[0] is not None]
                if values:
                    enums[f"{table_name}[{column_name}]"] = values
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 无法获取 {table_name}[{column_name}] 枚举: {error}")

        warnings: List[str] = []
        if 'vwpcse_dimqueue' in dimensions:
            warnings.append('DimQueue has dual keys (QueueKey & QueueID). Prefer QueueKey for model-wide consistency.')

        index = {
            'date_axis': date_axis,
            'facts': facts,
            'dimensions': dimensions,
            'relationships': relationships,
            'measures': measures,
            'enums': enums,
            'warnings': warnings
        }

        with open('nl2dax_index.json', 'w', encoding='utf-8') as handle:
            json.dump(index, handle, ensure_ascii=False, indent=2)
        return index

    # ---------- Build Outputs ----------
    def _build_markdown_document(
        self,
        model_name: str,
        md: Dict[str, Any],
        st: Dict[str, Any],
        examples: List[Dict[str, Any]],
        guide: Dict[str, Any],
        profiles: Dict[str, Any] = None,
        rel_quality: Dict[str, Any] = None
    ) -> str:
        parts: List[str] = []
        parts.append(f"# {model_name} - 完整技术文档")
        parts.append(f"\n**生成时间**: {self.analysis_timestamp}")
        parts.append("**文档版本**: 1.3\n")

        parts.append("## 目录")
        parts.append("1. [模型概述](#模型概述)")
        parts.append("2. [数据新鲜度与时间锚点](#数据新鲜度与时间锚点)")
        parts.append("3. [数据结构](#数据结构)")
        parts.append("4. [度量值参考](#度量值参考)")
        parts.append("5. [关系图](#关系图)")
        parts.append("6. [关系完整性体检](#关系完整性体检)")
        parts.append("7. [DAX查询示例](#dax查询示例)")
        parts.append("8. [使用指南](#使用指南)")
        parts.append("9. [NL2DAX 索引](#nl2dax-索引)")
        parts.append("10. [附录](#附录)\n")

        # 概述
        parts.append("## 模型概述\n")
        parts.append("### 关键统计")
        parts.append(f"- **业务表数量**: {len(md.get('business_tables', []))}")
        visible_measures = [m for m in md.get('measures', []) if not self._safe_bool(m.get('is_hidden'))]
        parts.append(f"- **度量值数量**: {len(visible_measures)}")
        rels_business = [
            r for r in md.get('relationships', [])
            if self._safe_bool(r.get('is_active')) and not self._is_auto_date_table(r.get('from_table')) and not self._is_auto_date_table(r.get('to_table'))
        ]
        parts.append(f"- **关系数量**: {len(rels_business)}")
        parts.append(f"- **自动日期表**: {len(md.get('auto_date_tables', []))}个（已自动创建）\n")

        # 新增：数据新鲜度与时间锚点
        parts.append("## 数据新鲜度与时间锚点\n")
        ta = (profiles or {}).get('time_anchors', {}) if profiles else {}
        rc = (profiles or {}).get('facts_rowcount', {}) if profiles else {}
        if ta:
            parts.append("| 事实表 | 锚点列 | 最小日期 | 最大日期 | 锚点日期 | 非空(锚点列) | 近7天 | 近30天 | 近90天 | 行数 |")
            parts.append("|--------|--------|----------|----------|----------|-------------|------|-------|-------|------|")
            for fact, prof in ta.items():
                if not prof: continue
                parts.append(
                    f"| {fact} | {prof.get('anchor_column') or ''} | {prof.get('min') or ''} | {prof.get('max') or ''} | "
                    f"{prof.get('anchor') or ''} | {prof.get('nonblank') or ''} | {prof.get('cnt7') or ''} | "
                    f"{prof.get('cnt30') or ''} | {prof.get('cnt90') or ''} | {rc.get(fact) if rc else ''} |"
                )
            parts.append("")
            parts.append("> **提示**：示例查询默认使用上表的“锚点日期 + 90 天”窗口；若近 90 天为 0，请改用“上月/上季度”等固定窗口。")
            parts.append("")

        # 数据结构
        parts.append("## 数据结构\n")
        for t in md.get('business_tables', []):
            tname = t.get('table_name', '')
            ttype = st.get('table_types', {}).get(tname, 'other')
            parts.append(f"### 📊 {tname} ({ttype})")
            if t.get('description'):
                parts.append(f"*{t['description']}*\n")

            tcols = [c for c in md.get('columns', []) if c.get('table_name') == tname and not self._safe_bool(c.get('is_hidden'))]
            if tcols:
                parts.append("| 列名 | 数据类型 | 说明 | 特性 |")
                parts.append("|------|----------|------|------|")
                for c in tcols[:15]:
                    name = c.get('column_name',''); dtype = c.get('data_type',''); desc = c.get('description','') or ''
                    feats: List[str] = []
                    if self._safe_bool(c.get('is_key')):      feats.append('🔑主键')
                    if self._safe_bool(c.get('is_unique')):   feats.append('✨唯一')
                    if not self._safe_bool(c.get('is_nullable')): feats.append('❗非空')
                    parts.append(f"| `{name}` | {dtype} | {desc} | {' '.join(feats)} |")
                if len(tcols) > 15:
                    parts.append(f"\n*...还有{len(tcols)-15}个列*")
            parts.append("")

        # 度量
        parts.append("## 度量值参考\n")
        by_cat = st.get('measure_summary', {}).get('by_category', {})
        for cat, names in by_cat.items():
            if not names: continue
            parts.append(f"### {cat.replace('_',' ').title()}\n")
            for nm in names[:10]:
                m = next((mm for mm in md.get('measures', []) if mm.get('measure_name') == nm), None)
                if not m: continue
                parts.append(f"#### [{nm}]")
                dax = (m.get('dax_expression') or '')
                dax = re.sub(r'==', '=', dax)
                parts.append("```dax")
                parts.append(dax if len(dax) <= 1200 else (dax[:1200] + '...'))
                parts.append("```")
                if m.get('format_string'): parts.append(f"**格式**: {m['format_string']}")
                if m.get('description'):   parts.append(f"**说明**: {m['description']}")
            if len(names) > 10:
                parts.append(f"\n*该类别还有{len(names)-10}个度量值*")
        parts.append("")

        # 关系
        parts.append("## 关系图\n")
        if st.get('star_schema'):
            parts.append("### 星型模式结构\n")
            for fact, sch in st['star_schema'].items():
                dims = sch.get('dimensions', [])
                if not dims: continue
                parts.append(f"**{fact}** (事实表)")
                for d in dims:
                    parts.append(f"  ├─→ {d['dimension_table']} ({d['join_key']})")
                parts.append("")
        krs = st.get('key_relationships', [])
        if krs:
            parts.append("### 关系详情\n")
            parts.append("| 源 | 目标 | 类型 | 筛选方向 |")
            parts.append("|-----|------|------|----------|")
            for r in krs[:80]:
                parts.append(f"| {r['from']} | {r['to']} | {r['type']} | {r['filter_direction']} |")
            if len(krs) > 80:
                parts.append(f"\n*...共{len(krs)}个关系*")
        parts.append("")

        # 新增：关系完整性体检
        parts.append("## 关系完整性体检\n")
        if rel_quality:
            summary_rows = rel_quality.get('summary', [])
            lint_msgs = [msg['message'] for msg in rel_quality.get('lints', [])]
            filtered_auto = rel_quality.get('filtered_auto_relationships', 0)
            if summary_rows:
                parts.append("| 外键 | 主键 | 空值占比 | 覆盖率 | 告警级别 | 空值数 | 孤儿键数 |")
                parts.append("|------|------|---------|--------|----------|--------|----------|")
                for row in summary_rows:
                    blank_ratio = '' if row.get('blank_ratio') is None else f"{row['blank_ratio']:.2%}"
                    coverage = '' if row.get('coverage') is None else f"{row['coverage']:.2%}"
                    parts.append(
                        f"| {row.get('from')} | {row.get('to')} | {blank_ratio} | {coverage} | "
                        f"{row.get('severity','green').upper()} | {row.get('blank_fk','')} | {row.get('orphan_fk','')} |"
                    )
            if lint_msgs:
                parts.append("\n**模型提示**")
                for message in lint_msgs:
                    parts.append(f"- {message}")
            parts.append(f"\n*已过滤 {filtered_auto} 条自动日期表关系（详见附录）*")
        parts.append("")

        # 示例
        parts.append("## DAX查询示例\n")
        cats: Dict[str, List[Dict[str, Any]]] = {}
        for ex in examples: cats.setdefault(ex.get('category','other'), []).append(ex)
        labels = {'basic':'基础查询','intermediate':'中级查询','time_series':'时间序列','filtering':'筛选查询','ranking':'排名分析','statistical':'统计分析','other':'其他'}
        for cat, exs in cats.items():
            parts.append(f"### {labels.get(cat, cat)}\n")
            for ex in exs:
                parts.append(f"#### {ex['title']}")
                parts.append(f"*{ex['description']}*\n")
                parts.append("```dax")
                parts.append(ex['dax'])
                parts.append("```\n")

        # 指南
        parts.append("## 使用指南\n")
        parts.append("### 快速开始")
        for item in guide.get('quick_start', []): parts.append(f"- {item}")
        parts.append("")
        if guide.get('common_patterns'):
            parts.append("### 常见模式")
            for item in guide['common_patterns']: parts.append(f"- {item}")
            parts.append("")
        parts.append("### 最佳实践")
        for item in guide.get('best_practices', []): parts.append(f"- {item}")
        parts.append("")
        parts.append("### 故障排除")
        for item in guide.get('troubleshooting', []): parts.append(f"- {item}")
        parts.append("")

        if self.nl2dax_index:
            parts.append("## NL2DAX 索引\n")
            parts.append("- **默认日期轴**: "
                         f"{self.nl2dax_index.get('date_axis', {}).get('table')}["
                         f"{self.nl2dax_index.get('date_axis', {}).get('date_column')}] ↔ "
                         f"{self.nl2dax_index.get('date_axis', {}).get('key_column')}")
            parts.append("- **事实表摘要**: 提供默认时间键、锚点策略、行数等信息")
            parts.append("- **维度展示列**: label/synonym 信息已收录，供 NL2DAX 快速对齐术语")
            parts.append("- **文件位置**: `nl2dax_index.json` (与本文档同目录)\n")

        # 附录
        parts.append("## 附录\n")
        if st.get('fact_time_axes'):
            parts.append("### 可用的日期轴判定\n")
            parts.append("| 事实表 | 默认日期列 | 默认日期键 | 日期维度 | 判定 |")
            parts.append("|--------|--------------|------------|----------|------|")
            for fact_name, payload in st['fact_time_axes'].items():
                verdict = "✅ 已匹配日期维度" if payload.get('has_date_axis') else "❌ 未匹配日期维度"
                parts.append(
                    f"| {fact_name} | {payload.get('default_time_column') or ''} | "
                    f"{payload.get('default_time_key') or ''} | {payload.get('date_dimension') or ''} | {verdict} |"
                )
            parts.append("")
        if md.get('auto_date_tables'):
            parts.append("### 自动生成的日期表")
            parts.append("Power BI为以下日期列自动创建了时间智能表：\n")
            for t in md['auto_date_tables'][:10]:
                parts.append(f"- `{t}` (hidden)")
            if len(md['auto_date_tables']) > 10:
                parts.append(f"- ...共{len(md['auto_date_tables'])}个")
        if md.get('errors'):
            parts.append("\n### 取数提示")
            for e in md['errors']:
                parts.append(f"- {e}")

        return "\n".join(parts)

    def _build_json_document(self, model_name: str, md: Dict[str, Any], st: Dict[str, Any],
                             examples: List[Dict[str, Any]], guide: Dict[str, Any],
                             profiles: Dict[str, Any] = None, rel_quality: Dict[str, Any] = None) -> str:
        return json.dumps({
            'model_name': model_name,
            'generated_at': self.analysis_timestamp,
            'metadata': md,
            'structure_analysis': st,
            'dax_examples': examples,
            'usage_guide': guide,
            'profiles': profiles or {},
            'relationship_quality': rel_quality or {},
            'nl2dax_index': self.nl2dax_index
        }, indent=2, ensure_ascii=False)

    # ---------- Utils ----------
    @staticmethod
    def _safe_bool(value: Any) -> bool:
        try:
            if value is None: return False
            if isinstance(value, (float, int)) and pd.isna(value): return False
            return bool(value)
        except Exception:
            return False

    @staticmethod
    def _is_auto_date_table(name: Optional[str]) -> bool:
        if not name: return False
        return bool(re.match(r'^(LocalDateTable_|DateTableTemplate_)', name, re.IGNORECASE))

    def _is_business_relationship(self, relationship: Dict[str, Any]) -> bool:
        """判断关系是否属于业务关系, 自动日期表或非活动关系会被过滤"""
        if relationship is None:
            raise ValueError("relationship 参数不能为空")
        is_active = self._safe_bool(relationship.get('is_active'))
        if not is_active:
            return False
        from_table = relationship.get('from_table')
        to_table = relationship.get('to_table')
        if self._is_auto_date_table(from_table) or self._is_auto_date_table(to_table):
            return False
        return True


# ----------------------------
# CLI / Demo
# ----------------------------
if __name__ == "__main__":
    # ==== 修改为你的模型与工作区 ====
    MODEL_NAME = "PCSE AI"   # 示例
    WORKSPACE_GUID = None    # e.g. "00000000-0000-0000-0000-000000000000" 或 None 使用默认
    OUTPUT_FORMAT = "markdown"  # or "json"
    OUTPUT_PATH = "model_complete_documentation.md" if OUTPUT_FORMAT == "markdown" \
                  else "model_complete_documentation.json"
    PROFILE_DATA = True  # 生成数据新鲜度/关系体检
    # =================================

    doc = ComprehensiveModelDocumentor(verbose=True)
    documentation = doc.generate_complete_documentation(
        model_name=MODEL_NAME,
        workspace=WORKSPACE_GUID,
        output_format=OUTPUT_FORMAT,
        profile_data=PROFILE_DATA
    )
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(documentation)
    print(f"\n✅ 文档已保存到 {OUTPUT_PATH}")
