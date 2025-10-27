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
from typing import Dict, List, Optional, Protocol, Any, Tuple, Set
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
        self.compact_mode: bool = True
        self.max_columns_per_table: int = 8
        self.include_measure_dax: bool = False
        self.show_other_tables_in_main: bool = False

    # ---------- Public API ----------
    def generate_complete_documentation(
        self,
        model_name: str,
        workspace: Optional[str] = None,
        output_format: str = 'markdown',
        profile_data: bool = True,  # NEW: 默认做数据体检
        compact: bool = True,
        max_columns_per_table: int = 8,
        include_measure_dax: bool = False
    ) -> str:
        """生成完整语义模型文档

        参数:
            model_name: 目标语义模型名称。
            workspace: Fabric 工作区名称；若为空则使用当前上下文。
            output_format: 'markdown' 或 'json'，控制最终输出格式。
            profile_data: 是否执行数据体检，默认为 True。
            compact: 是否启用紧凑模式，仅展示核心列与摘要。
            max_columns_per_table: 紧凑模式下每张表展示的最大列数。
            include_measure_dax: 是否在正文中直接展示度量 DAX。

        返回:
            生成的完整文档字符串。
        """
        if self.verbose:
            print(f"📚 生成 {model_name} 的完整文档")
            print("=" * 60)

        self.compact_mode = compact
        self.max_columns_per_table = max_columns_per_table
        self.include_measure_dax = include_measure_dax

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
            if key_info:
                default_date_column = self._match_date_column_for_key(fact, key_info[0], md)
            if not default_date_column:
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

        numeric_type_flags = [
            'int', 'integer', 'whole number', 'decimal',
            'fixed decimal', 'double', 'float', 'number', 'currency'
        ]
        numeric_cols = sum(
            1
            for c in cols
            if any(flag in (c.get('data_type') or '').lower() for flag in numeric_type_flags)
        )
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

    def _match_date_column_for_key(self, fact: str, key_col: str, md: Dict[str, Any]) -> Optional[str]:
        """基于时间键语义匹配事实表最合适的日期列"""
        if not fact or not key_col:
            return None

        # 统一时间键语义，便于和日期列名称做模糊对齐
        base = re.sub(r'key$', '', key_col, flags=re.IGNORECASE)
        base = base.replace('_', '').lower()
        preferences = ['submitted', 'sent', 'closed', 'created', 'resolved', 'calendar', 'date', 'time']

        # 找出事实表内所有日期类型的列
        fact_columns = [
            column for column in md.get('columns', [])
            if column.get('table_name') == fact and 'date' in (column.get('data_type') or '').lower()
        ]
        if not fact_columns:
            return None

        # 构建标准化映射，方便做包含关系匹配
        normalized_columns = [
            (column.get('column_name'), (column.get('column_name') or '').replace('_', '').replace(' ', '').lower())
            for column in fact_columns
        ]

        # 先尝试基于键名的直接包含关系
        if base:
            for original, normalized in normalized_columns:
                if base in normalized:
                    return original

        # 若未命中，按优先关键词依次尝试
        for preference in preferences:
            for original, normalized in normalized_columns:
                if preference in normalized:
                    return original

        # 最后兜底返回列表中的第一列
        return normalized_columns[0][0] if normalized_columns else None

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
        priority_keywords = ['name', 'title', 'country', 'region', 'area', 'site', 'queue', 'category']
        for keyword in priority_keywords:
            for candidate in candidates:
                candidate_value = candidate or ''
                if re.search(rf'\b{keyword}\b', candidate_value, flags=re.IGNORECASE):
                    return candidate
        for candidate in candidates:
            lowered = (candidate or '').lower()
            if lowered.endswith('name') or lowered.endswith('title'):
                return candidate
        return candidates[0] if candidates else None

    def _dax_profile_on_date_column(
        self,
        table: str,
        column: str,
        expression: Optional[str] = None,
        display_column: Optional[str] = None
    ) -> str:
        """生成用于日期列体检的 DAX 语句, 仅在非空记录上统计。

        参数:
            table: 事实表名称, 需要加单引号引用。
            column: 日期列名称, 必须属于 `table`。
            expression: 可选的 DAX 表达式, 当原列需要先做类型转换时传入。
            display_column: 在输出行中展示的列标签, 默认为列名。

        返回:
            包含最小日期、最大日期、近 N 天计数等信息的 DAX 查询字符串。
        """

        # expression 为所有计算引用的表达式, 默认为原列。
        target_expr = expression or f"'{table}'[{column}]"
        label = (display_column or column or '').replace('"', '""')

        # 通过 ADDCOLUMNS 写入统一的 __value 列, 确保后续比较使用同一数据类型。
        # 这样即便原始列是文本, 经过 VALUE/DATEVALUE 转换后, 比较操作也始终在数值时间轴上进行。
        return f"""
EVALUATE
VAR _base =
    ADDCOLUMNS(
        FILTER(
            ALL('{table}'),
            NOT ISBLANK({target_expr})
        ),
        "__value", {target_expr}
    )
VAR _min = MINX(_base, [__value])
VAR _max = MAXX(_base, [__value])
VAR _cnt7 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _base,
                [__value] > _max - 7
                    && [__value] <= _max
            )
        ),
        BLANK()
    )
VAR _cnt30 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _base,
                [__value] > _max - 30
                    && [__value] <= _max
            )
        ),
        BLANK()
    )
VAR _cnt90 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _base,
                [__value] > _max - 90
                    && [__value] <= _max
            )
        ),
        BLANK()
    )
RETURN
ROW(
    "column", "{label}",
    "min", _min,
    "max", _max,
    "anchor", _max,
    "nonblank", COUNTROWS(_base),
    "cnt7", _cnt7,
    "cnt30", _cnt30,
    "cnt90", _cnt90
)
"""

    def _profile_time_anchor_for_table(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any],
        table: str
    ) -> Dict[str, Any]:
        """探测事实表的时间锚点, 返回锚点表达式及统计数据。"""
        # ---- 小工具：候选列选择 ----
        def _dtype_is_date(data_type: str) -> bool:
            """严格判断日期或日期时间类型。"""
            lowered = (data_type or '').lower()
            return any(flag in lowered for flag in ['date', 'datetime', 'timestamp'])

        def _score(column_name: str) -> float:
            """根据列名打分, Submitted/Sent/Closed 等优先。"""
            lowered = (column_name or '').lower()
            base = 1.0
            if 'submitted' in lowered:
                base = 6.0
            elif 'sent' in lowered:
                base = 5.0
            elif 'closed' in lowered:
                base = 4.0
            elif 'created' in lowered:
                base = 3.5
            elif 'resolved' in lowered:
                base = 3.2
            elif 'calendar' in lowered:
                base = 3.0
            elif 'date' in lowered:
                base = 2.0
            if 'time' in lowered and 'date' not in lowered:
                base -= 0.6
            return base

        anchor_order: List[str] = ['direct', 'via_key', 'coalesce', 'fallback']

        table_columns = [
            column for column in md.get('columns', []) if column.get('table_name') == table
        ]
        normalized_type_map: Dict[str, str] = {}
        for column in table_columns:
            column_name = column.get('column_name')
            if not column_name:
                continue
            data_type_value = column.get('data_type') or ''
            normalized_type_map[column_name] = self._coerce_type(data_type=data_type_value)

        # 1) 收集候选日期列：优先真实日期类型, 其次名称包含日期词根。
        typed_date_cols = [
            column.get('column_name')
            for column in table_columns
            if _dtype_is_date(column.get('data_type'))
        ]
        typed_date_cols = [column for column in typed_date_cols if column]
        typed_date_cols = sorted(set(typed_date_cols), key=_score, reverse=True)

        name_candidates_primary: List[str] = []
        name_candidates_time_only: List[str] = []
        for column in table_columns:
            column_name = column.get('column_name')
            if not column_name:
                continue
            lowered_name = column_name.lower()
            data_type_lowered = (column.get('data_type') or '').lower()
            if 'date' in lowered_name or _dtype_is_date(column.get('data_type')):
                if column_name not in name_candidates_primary:
                    name_candidates_primary.append(column_name)
                continue
            if 'time' in lowered_name:
                if any(flag in data_type_lowered for flag in ['date', 'datetime', 'timestamp']):
                    if column_name not in name_candidates_primary:
                        name_candidates_primary.append(column_name)
                elif column_name not in name_candidates_time_only:
                    name_candidates_time_only.append(column_name)

        name_candidates = name_candidates_primary + name_candidates_time_only

        direct_candidates: List[str] = []
        for candidate in typed_date_cols + name_candidates:
            if candidate not in direct_candidates:
                direct_candidates.append(candidate)

        # 2) 直接用事实表日期列做锚点（逐个尝试, 扩展到前 8 个）。
        for candidate in direct_candidates[:8]:
            column_reference = f"'{table}'[{candidate}]"
            normalized_type = normalized_type_map.get(candidate, 'text')
            target_expr = column_reference
            if normalized_type == 'text':
                target_expr = f"IFERROR(VALUE({column_reference}), BLANK())"
                if self.verbose:
                    print(f"ℹ️ {table}[{candidate}] 为文本列, 尝试用 VALUE + IFERROR 转换后探测锚点…")
            try:
                dax = self._dax_profile_on_date_column(
                    table=table,
                    column=candidate,
                    expression=target_expr,
                    display_column=candidate
                )
                df_result = self.runner.evaluate(dataset=model_name, dax=dax, workspace=workspace)
                if df_result.empty:
                    continue
                record = df_result.iloc[0].to_dict()
                if pd.isna(record.get('anchor')):
                    if self.verbose:
                        print(f"ℹ️ {table}[{candidate}] 无有效锚点，继续尝试…")
                    continue
                return {
                    'anchor_column': record.get('column'),
                    'anchor_reference_column': candidate,
                    'min': record.get('min'),
                    'max': record.get('max'),
                    'anchor': record.get('anchor'),
                    'nonblank': self._to_int_or_none(record.get('nonblank')),
                    'cnt7': self._to_int_or_none(record.get('cnt7')),
                    'cnt30': self._to_int_or_none(record.get('cnt30')),
                    'cnt90': self._to_int_or_none(record.get('cnt90')),
                    'anchor_expr_direct': f"MAXX(ALL('{table}'), {target_expr})",
                    'anchor_order': anchor_order
                }
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 日期列 {table}[{candidate}] 锚点探测失败: {error}")

        # 3) via-key：用 DimDate + 键映射, 强制过滤空值并处理类型差异。
        key_info = self._detect_default_time_key(table, md)
        if key_info:
            fact_key, dim_table, dim_key = key_info
            fact_dtype = (next(
                (
                    column.get('data_type')
                    for column in md.get('columns', [])
                    if column.get('table_name') == table and column.get('column_name') == fact_key
                ),
                ''
            ) or '').lower()
            dim_dtype = (next(
                (
                    column.get('data_type')
                    for column in md.get('columns', [])
                    if column.get('table_name') == dim_table and column.get('column_name') == dim_key
                ),
                ''
            ) or '').lower()

            dim_date_column = self._select_dim_date_column(dim_table, md)
            if dim_date_column:
                fact_type = self._coerce_type(data_type=fact_dtype)
                dim_type = self._coerce_type(data_type=dim_dtype)
                fact_to_dim = self._coerce_expr(
                    table=table,
                    column=fact_key,
                    current_type=fact_type,
                    target_type=dim_type
                )
                dim_to_fact = self._coerce_expr(
                    table=dim_table,
                    column=dim_key,
                    current_type=dim_type,
                    target_type=fact_type
                )

                dax_key = f"""
EVALUATE
VAR KeyFact =
    SELECTCOLUMNS(
        FILTER(
            VALUES('{table}'[{fact_key}]),
            NOT ISBLANK({fact_to_dim})
        ),
        "__k", {fact_to_dim}
    )
VAR AnchorDate =
    CALCULATE(
        MAX('{dim_table}'[{dim_date_column}]),
        TREATAS(KeyFact, '{dim_table}'[{dim_key}])
    )
VAR MinDate =
    CALCULATE(
        MIN('{dim_table}'[{dim_date_column}]),
        TREATAS(KeyFact, '{dim_table}'[{dim_key}])
    )
VAR Win90Dim =
    CALCULATETABLE(
        VALUES('{dim_table}'[{dim_key}]),
        FILTER(
            ALL('{dim_table}'[{dim_date_column}]),
            NOT ISBLANK(AnchorDate)
                && '{dim_table}'[{dim_date_column}] > AnchorDate - 90
                && '{dim_table}'[{dim_date_column}] <= AnchorDate
        )
    )
VAR Win30Dim =
    CALCULATETABLE(
        VALUES('{dim_table}'[{dim_key}]),
        FILTER(
            ALL('{dim_table}'[{dim_date_column}]),
            NOT ISBLANK(AnchorDate)
                && '{dim_table}'[{dim_date_column}] > AnchorDate - 30
                && '{dim_table}'[{dim_date_column}] <= AnchorDate
        )
    )
VAR Win7Dim =
    CALCULATETABLE(
        VALUES('{dim_table}'[{dim_key}]),
        FILTER(
            ALL('{dim_table}'[{dim_date_column}]),
            NOT ISBLANK(AnchorDate)
                && '{dim_table}'[{dim_date_column}] > AnchorDate - 7
                && '{dim_table}'[{dim_date_column}] <= AnchorDate
        )
    )
VAR Win90Fact = SELECTCOLUMNS(Win90Dim, "__k", {dim_to_fact})
VAR Win30Fact = SELECTCOLUMNS(Win30Dim, "__k", {dim_to_fact})
VAR Win7Fact  = SELECTCOLUMNS(Win7Dim,  "__k", {dim_to_fact})
VAR Cnt90 = CALCULATE(COUNTROWS('{table}'), TREATAS(Win90Fact, '{table}'[{fact_key}]))
VAR Cnt30 = CALCULATE(COUNTROWS('{table}'), TREATAS(Win30Fact, '{table}'[{fact_key}]))
VAR Cnt7  = CALCULATE(COUNTROWS('{table}'), TREATAS(Win7Fact , '{table}'[{fact_key}]))
RETURN
ROW(
    "column", "{fact_key}",
    "min", MinDate,
    "max", AnchorDate,
    "anchor", AnchorDate,
    "nonblank", COUNTROWS(KeyFact),
    "cnt7", Cnt7,
    "cnt30", Cnt30,
    "cnt90", Cnt90
)
"""
                try:
                    df_key = self.runner.evaluate(dataset=model_name, dax=dax_key, workspace=workspace)
                    if not df_key.empty:
                        record = df_key.iloc[0].to_dict()
                        if pd.notna(record.get('anchor')):
                            anchor_expr_via_key = (
                                "CALCULATE(" +
                                f"MAX('{dim_table}'[{dim_date_column}]), " +
                                "TREATAS(" +
                                "SELECTCOLUMNS(" +
                                f"FILTER(VALUES('{table}'[{fact_key}]), NOT ISBLANK({fact_to_dim})), \"__k\", {fact_to_dim}), " +
                                f"'{dim_table}'[{dim_key}]" +
                                ")" +
                                ")"
                            )
                            return {
                                'anchor_column': record.get('column'),
                                'anchor_reference_column': fact_key,
                                'min': record.get('min'),
                                'max': record.get('max'),
                                'anchor': record.get('anchor'),
                                'nonblank': self._to_int_or_none(record.get('nonblank')),
                                'cnt7': self._to_int_or_none(record.get('cnt7')),
                                'cnt30': self._to_int_or_none(record.get('cnt30')),
                                'cnt90': self._to_int_or_none(record.get('cnt90')),
                                'anchor_via_key': True,
                                'anchor_expr_via_key': anchor_expr_via_key,
                                'date_dimension': dim_table,
                                'date_axis_column': dim_date_column,
                                'anchor_order': anchor_order
                            }
                except Exception as error:
                    if self.verbose:
                        print(f"⚠️ 键列 {table}[{fact_key}] via-key 锚点探测失败: {error}")

        # 4) COALESCE 兜底：组合多个日期列, 同样过滤空值。
        if len(typed_date_cols) >= 2:
            coalesce_columns = typed_date_cols[:3]
            coalesce_expr = "COALESCE(" + ", ".join([f"'{table}'[{column}]" for column in coalesce_columns]) + ")"
            dax_coalesce = f"""
EVALUATE
VAR _base =
    ADDCOLUMNS(
        ALL('{table}'),
        "__d", {coalesce_expr}
    )
VAR _filtered = FILTER(_base, NOT ISBLANK([__d]))
VAR _min = MINX(_filtered, [__d])
VAR _max = MAXX(_filtered, [__d])
VAR _cnt7 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _filtered,
                [__d] > _max - 7 && [__d] <= _max
            )
        ),
        BLANK()
    )
VAR _cnt30 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _filtered,
                [__d] > _max - 30 && [__d] <= _max
            )
        ),
        BLANK()
    )
VAR _cnt90 =
    IF(
        NOT ISBLANK(_max),
        COUNTROWS(
            FILTER(
                _filtered,
                [__d] > _max - 90 && [__d] <= _max
            )
        ),
        BLANK()
    )
RETURN
ROW(
    "column", "{coalesce_expr}",
    "min", _min,
    "max", _max,
    "anchor", _max,
    "nonblank", COUNTROWS(_filtered),
    "cnt7", _cnt7,
    "cnt30", _cnt30,
    "cnt90", _cnt90
)
"""
            try:
                df_coalesce = self.runner.evaluate(dataset=model_name, dax=dax_coalesce, workspace=workspace)
                if not df_coalesce.empty:
                    record = df_coalesce.iloc[0].to_dict()
                    if pd.notna(record.get('anchor')):
                        if self.verbose:
                            joined = ', '.join(coalesce_columns)
                            print(f"ℹ️ 使用 COALESCE 作为 {table} 的日期锚点: {joined}")
                        return {
                            'anchor_column': record.get('column'),
                            'anchor_reference_column': coalesce_columns[0],
                            'min': record.get('min'),
                            'max': record.get('max'),
                            'anchor': record.get('anchor'),
                            'nonblank': self._to_int_or_none(record.get('nonblank')),
                            'cnt7': self._to_int_or_none(record.get('cnt7')),
                            'cnt30': self._to_int_or_none(record.get('cnt30')),
                            'cnt90': self._to_int_or_none(record.get('cnt90')),
                            'anchor_via_coalesce': True,
                            'anchor_expr_coalesce': f"MAXX(ALL('{table}'), {coalesce_expr})",
                            'anchor_order': anchor_order
                        }
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ COALESCE 锚点探测失败 {table}: {error}")

        # 5) 彻底兜底：返回结构占位, 供上层继续兜底。
        fallback_column = None
        if direct_candidates:
            fallback_column = direct_candidates[0]
        elif name_candidates:
            fallback_column = name_candidates[0]

        return {
            'anchor_column': fallback_column,
            'anchor_reference_column': fallback_column,
            'min': None,
            'max': None,
            'anchor': None,
            'nonblank': None,
            'cnt7': None,
            'cnt30': None,
            'cnt90': None,
            'anchor_order': anchor_order
        }

    def _to_int_or_none(self, value: Any) -> Optional[int]:
        """安全地将任意输入转换为整数。

        参数:
            value: 任意待转换的值, 支持标量类型或可转换为整数的字符串。

        返回:
            若输入可转换为整数则返回对应的 int, 否则返回 None 并输出提示。
        """
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        if isinstance(value, str):
            if value.strip() == '':
                return None
        if pd.isna(value):
            return None
        try:
            return int(value)
        except (TypeError, ValueError) as error:
            if self.verbose:
                print(f"⚠️ 无法将值 {value} 转换为整数: {error}")
            return None

    def _coerce_type(self, data_type: str) -> str:
        """将数据类型归一到 number/text/date 三大类。"""

        lowered = (data_type or '').lower()
        number_flags = [
            'int', 'integer', 'whole number', 'decimal', 'double', 'fixed decimal', 'currency', 'number'
        ]
        date_flags = ['date', 'datetime', 'timestamp', 'time']
        if any(flag in lowered for flag in number_flags):
            return 'number'
        if any(flag in lowered for flag in date_flags):
            return 'date'
        return 'text'

    def _coerce_expr(
        self,
        table: str,
        column: str,
        current_type: str,
        target_type: str
    ) -> str:
        """构造将列值转换为目标类型的 DAX 表达式, 并通过 IFERROR 兜底非法值。"""

        reference = f"'{table}'[{column}]"
        if target_type == 'number':
            if current_type == 'number':
                return reference
            return f"IFERROR(VALUE({reference}), BLANK())"
        if target_type == 'text':
            if current_type == 'text':
                return reference
            return f"FORMAT({reference}, \"0\")"
        if target_type == 'date':
            if current_type == 'date':
                return reference
            return f"IFERROR(VALUE({reference}), BLANK())"
        return reference

    def _select_join_type(self, left_type: str, right_type: str) -> str:
        """根据左右列类型决定孤儿检查的统一目标类型。"""

        # 日期类型优先保持日期处理, 否则优先使用数字, 最后退回文本。
        if 'date' in {left_type, right_type}:
            return 'date'
        if 'number' in {left_type, right_type}:
            return 'number'
        return 'text'

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
            type_from = self._coerce_type(data_type=dtype_from)
            type_to = self._coerce_type(data_type=dtype_to)
            target_type = self._select_join_type(left_type=type_from, right_type=type_to)
            type_mismatch = type_from != type_to
            fk_expr = self._coerce_expr(
                table=from_table,
                column=from_column,
                current_type=type_from,
                target_type=target_type
            )
            pk_expr = self._coerce_expr(
                table=to_table,
                column=to_column,
                current_type=type_to,
                target_type=target_type
            )

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
                    blank_fk = self._to_int_or_none(df_rows.iloc[0].get('blank_fk'))
                    total_rows = self._to_int_or_none(df_rows.iloc[0].get('total_rows'))
                    distinct_fk = self._to_int_or_none(df_rows.iloc[0].get('distinct_fk'))
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 无法计算 {from_table}[{from_column}] 的空值统计: {error}")

            orphan_fk = None
            dax_orphan = (
                f"""
EVALUATE
VAR FKVals =
    SELECTCOLUMNS(
        FILTER(
            VALUES('{from_table}'[{from_column}]),
            NOT ISBLANK({fk_expr})
        ),
        "__k", {fk_expr}
    )
VAR PKVals =
    SELECTCOLUMNS(
        FILTER(
            VALUES('{to_table}'[{to_column}]),
            NOT ISBLANK({pk_expr})
        ),
        "__k", {pk_expr}
    )
RETURN
ROW(
    "orphan_fk",
    COUNTROWS(EXCEPT(FKVals, PKVals))
)
"""
            )
            try:
                df_orphan = self.runner.evaluate(dataset=model_name, dax=dax_orphan, workspace=workspace)
                if not df_orphan.empty:
                    orphan_fk = self._to_int_or_none(df_orphan.iloc[0].get('orphan_fk'))
            except Exception as error:
                if self.verbose:
                    print(f"⚠️ 无法计算 {from_table}[{from_column}] → {to_table}[{to_column}] 的孤儿键: {error}")

            if type_mismatch:
                lints.append({
                    'type': 'lint',
                    'message': (
                        f"关系 {from_table}[{from_column}] → {to_table}[{to_column}] 存在类型差异（{dtype_from} ↔ {dtype_to}），"  # noqa: E501
                        f"已按 {target_type} 自动比对，建议在模型层统一类型。"
                    )
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
                'severity': severity,
                'type_mismatch': type_mismatch,
                'comparison_type': target_type
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

        def _has_measure(metadata: Dict[str, Any], measure_name: str) -> bool:
            """判断指定度量是否存在且未隐藏。

            参数:
                metadata: 模型元数据字典。
                measure_name: 目标度量名称。

            返回:
                若存在可见度量则返回 True, 否则 False。
            """
            for measure in metadata.get('measures', []):
                if measure.get('measure_name') == measure_name and not self._safe_bool(measure.get('is_hidden')):
                    return True
            return False

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
            """根据锚点顺序生成可直接 COALESCE 的表达式。"""

            anchor_info = (profiles or {}).get('time_anchors', {}).get(fact_name, {})
            anchor_column = anchor_info.get('anchor_reference_column') or anchor_info.get('anchor_column')
            if not anchor_column:
                return '', ''
            dim_table = anchor_info.get('date_dimension') or date_axis_table
            dim_date_col = anchor_info.get('date_axis_column') or date_axis_column
            fallback_expr = None
            if dim_table and dim_date_col:
                fallback_expr = f"MAX('{dim_table}'[{dim_date_col}])"
            order = anchor_info.get('anchor_order') or ['direct', 'via_key', 'coalesce', 'fallback']
            candidate_map = {
                'direct': anchor_info.get('anchor_expr_direct'),
                'via_key': anchor_info.get('anchor_expr_via_key'),
                'coalesce': anchor_info.get('anchor_expr_coalesce'),
                'fallback': fallback_expr
            }
            expressions: List[str] = []
            for key in order:
                expr = candidate_map.get(key)
                if expr and expr not in expressions:
                    expressions.append(expr)
            if fallback_expr and fallback_expr not in expressions:
                expressions.append(fallback_expr)
            if not expressions:
                return anchor_column, fallback_expr or ''
            if len(expressions) == 1:
                return anchor_column, expressions[0]
            return anchor_column, f"COALESCE({', '.join(expressions)})"

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
    'vwpcse_dimgeography'[{country_label}],
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
            use_inline_median = not _has_measure(md, 'Median CSAT')
            median_expr = "[Median CSAT]" if not use_inline_median else (
                "MEDIANX("
                "FILTER('vwpcse_factcustomersurvey', NOT ISBLANK('vwpcse_factcustomersurvey'[CsatScore])), "
                "'vwpcse_factcustomersurvey'[CsatScore])"
            )
            anchor_reference = anchor_col or 'SubmittedDate'
            median_expr_treatas = (
                f"CALCULATE({median_expr}, TREATAS(Window, '{survey_fact}'[{anchor_reference}]))"
            )
            dax_active_queue = f"""EVALUATE
VAR AnchorDate = {anchor_expr_survey}
VAR Window = DATESINPERIOD('{date_axis_table}'[{date_axis_column}], AnchorDate, -90, DAY)
RETURN
TOPN(
  20,
  SUMMARIZECOLUMNS(
    'vwpcse_dimqueue'[{queue_label}],
    Window,
    "Median CSAT", {median_expr}
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
    VALUES('vwpcse_dimqueue'[{queue_label}]),
    "Median CSAT", {median_expr_treatas}
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
        '{text_c["table_name"]}'[{text_c["column_name"]}] = "示例值"
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

    def _expand_synonyms(self, label: Optional[str]) -> List[str]:
        """生成多语言同义词集合"""
        if not label:
            return []

        # 标准化输入，消除下划线/大小写影响
        base = label.replace('_', ' ').strip()
        variants: Set[str] = {base, base.lower(), base.title()}
        keyword_mapping = {
            'queue': ['队列', 'Queue', 'キュー'],
            'country': ['国家', 'Country', '国'],
            'region': ['区域', 'Region', 'リージョン'],
            'area': ['地区', 'Area', 'エリア'],
            'site': ['站点', 'Site', 'サイト'],
            'partner': ['合作伙伴', 'Partner', 'パートナー'],
            'category': ['类别', 'Category', 'カテゴリ'],
        }
        lowered = base.lower()
        # 根据关键词扩展不同语言的常见别名
        for keyword, words in keyword_mapping.items():
            if keyword in lowered:
                variants.update(words)
        return sorted(variants)

    def _extract_measure_dependencies(self, dax_expression: Optional[str]) -> Dict[str, List[str]]:
        """解析度量 DAX 表达式依赖的列与度量"""
        if not dax_expression:
            return {'measures': [], 'columns': []}

        # 捕获 '表'[列] 模式，区分列引用
        column_pairs = re.findall(r"'([^']+)'\[([^\]]+)\]", dax_expression)
        column_refs = {f"{table}[{column}]" for table, column in column_pairs}
        column_names = {column for _, column in column_pairs}
        # 捕获孤立的 [名称] 作为度量引用，并排除已识别的列
        measure_candidates = re.findall(r'\[([^\[\]]+)\]', dax_expression)
        measure_refs = sorted({candidate for candidate in measure_candidates if candidate not in column_names})
        return {
            'measures': measure_refs,
            'columns': sorted(column_refs)
        }

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
        time_defaults: Dict[str, Any] = {}
        for fact_name, payload in fact_time_axes.items():
            anchor_profile = time_anchors.get(fact_name, {})
            # 聚合锚点相关的关键字段，优先使用数据体检结果，其次退回结构分析推断
            anchor_column = anchor_profile.get('anchor_column') or payload.get('default_time_column')
            anchor_reference_column = anchor_profile.get('anchor_reference_column') or anchor_column
            dim_table_name = anchor_profile.get('date_dimension') or payload.get('date_dimension')
            dim_key_name = anchor_profile.get('date_dimension_key') or payload.get('date_dimension_key')
            dim_date_column = anchor_profile.get('date_axis_column') or (
                self._select_dim_date_column(dim_table_name, md) if dim_table_name else None
            )

            anchor_expr_direct: Optional[str] = anchor_profile.get('anchor_expr_direct')
            anchor_expr_via_key: Optional[str] = anchor_profile.get('anchor_expr_via_key')
            anchor_expr_coalesce: Optional[str] = anchor_profile.get('anchor_expr_coalesce')
            anchor_order: List[str] = anchor_profile.get('anchor_order') or ['direct', 'via_key', 'coalesce', 'fallback']

            if not anchor_expr_via_key and payload.get('default_time_key') and dim_table_name and dim_key_name and dim_date_column:
                fact_key_name = payload.get('default_time_key')
                fact_dtype = (next(
                    (
                        column.get('data_type')
                        for column in md.get('columns', [])
                        if column.get('table_name') == fact_name and column.get('column_name') == fact_key_name
                    ),
                    ''
                ) or '').lower()
                dim_dtype = (next(
                    (
                        column.get('data_type')
                        for column in md.get('columns', [])
                        if column.get('table_name') == dim_table_name and column.get('column_name') == dim_key_name
                    ),
                    ''
                ) or '').lower()
                fact_type = self._coerce_type(data_type=fact_dtype)
                dim_type = self._coerce_type(data_type=dim_dtype)
                fact_to_dim_expr = self._coerce_expr(
                    table=fact_name,
                    column=fact_key_name,
                    current_type=fact_type,
                    target_type=dim_type
                )
                anchor_expr_via_key = (
                    "CALCULATE(" +
                    f"MAX('{dim_table_name}'[{dim_date_column}]), " +
                    "TREATAS(" +
                    "SELECTCOLUMNS(" +
                    f"FILTER(VALUES('{fact_name}'[{fact_key_name}]), NOT ISBLANK({fact_to_dim_expr})), \"__k\", {fact_to_dim_expr}), " +
                    f"'{dim_table_name}'[{dim_key_name}]" +
                    ")" +
                    ")"
                )

            fallback_dim_table = dim_table_name or default_dim_table
            fallback_dim_date_column = dim_date_column or default_dim_date_column
            anchor_expr_fallback: Optional[str] = None
            if fallback_dim_table and fallback_dim_date_column:
                anchor_expr_fallback = f"MAX('{fallback_dim_table}'[{fallback_dim_date_column}])"

            anchor_block = {
                'direct': anchor_expr_direct,
                'via_key': anchor_expr_via_key,
                'coalesce': anchor_expr_coalesce,
                'fallback': anchor_expr_fallback,
                'order': anchor_order
            }

            # 将行计数统一转为 int，便于比较和排序
            def _coerce_count(value: Any) -> Optional[int]:
                if value is None:
                    return None
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return None

            suggested_windows: List[str] = []
            if _coerce_count(anchor_profile.get('cnt90')):
                suggested_windows.append('-90D')
            if _coerce_count(anchor_profile.get('cnt30')):
                suggested_windows.append('-30D')
            if _coerce_count(anchor_profile.get('cnt7')):
                suggested_windows.append('-7D')
            if not suggested_windows:
                suggested_windows = ['LM', 'LQ']

            # 推导首选时间窗长度，供 NL2DAX 默认使用
            window_days = 90 if '-90D' in suggested_windows else (
                30 if '-30D' in suggested_windows else (
                    7 if '-7D' in suggested_windows else None
                )
            )

            facts[fact_name] = {
                'grain': 'incident' if 'incident' in fact_name else ('task' if 'task' in fact_name else 'fact'),
                'default_time_key': payload.get('default_time_key'),
                'default_time_column': anchor_column,
                'anchor_strategy': 'direct → via_key → coalesce → fallback',
                'row_count': facts_rowcount.get(fact_name),
                'time': {
                    'anchor': dict(anchor_block),
                    'reference_column': anchor_reference_column,
                    'date_dimension': dim_table_name,
                    'date_dimension_key': dim_key_name,
                    'date_axis_column': dim_date_column,
                    'windows': suggested_windows,
                    'preferred_window_days': window_days
                }
            }

            time_defaults[fact_name] = {
                'anchor_column': anchor_column,
                'reference_column': anchor_reference_column,
                'dim_table': dim_table_name,
                'dim_key': dim_key_name,
                'dim_date_column': dim_date_column,
                'anchor': dict(anchor_block),
                'window_days': window_days,
                'suggested_windows': suggested_windows
            }

        dimensions: Dict[str, Any] = {}
        for table in md.get('business_tables', []):
            table_name = table.get('table_name')
            if st.get('table_types', {}).get(table_name) != 'dimension':
                continue
            columns = [
                column for column in md.get('columns', [])
                if column.get('table_name') == table_name and not self._safe_bool(column.get('is_hidden'))
            ]
            primary_key = next(
                (
                    column.get('column_name')
                    for column in columns
                    if self._safe_bool(column.get('is_key')) or self._safe_bool(column.get('is_unique'))
                ),
                None
            )
            text_columns = [
                column for column in columns
                if (column.get('data_type') or '').lower() in ['string', 'text']
            ]
            label_column = self._select_dimension_label(table_name, md)
            if not label_column and text_columns:
                label_column = text_columns[0].get('column_name')
            natural_key = next(
                (
                    column.get('column_name')
                    for column in columns
                    if column.get('column_name') and column.get('column_name').lower().endswith(('id', 'code'))
                ),
                None
            )
            friendly_name = table_name.replace('vwpcse_', '') if table_name else ''
            alias_variants = self._expand_synonyms(label_column or friendly_name)
            alias_target = None
            if label_column:
                alias_target = f"{table_name}[{label_column}]"
            elif primary_key:
                alias_target = f"{table_name}[{primary_key}]"
            elif columns:
                alias_target = f"{table_name}[{columns[0].get('column_name')}]"
            alias_map = {variant: alias_target for variant in alias_variants if alias_target}
            dimensions[table_name] = {
                'primary_key': primary_key,
                'natural_key': natural_key,
                'label': label_column,
                'aliases': alias_map,
                'alias_target': alias_target
            }
        if 'vwpcse_dimqueue' in dimensions:
            dim_queue = dimensions['vwpcse_dimqueue']
            dim_queue['label'] = dim_queue.get('label') or 'Queue Name'
            alias_target = dim_queue.get('alias_target') or f"vwpcse_dimqueue[{dim_queue.get('label') or 'Queue Name'}]"
            alias_map = dim_queue.get('aliases', {})
            for alias in ['队列', 'Queue', '队列名称']:
                if alias_target:
                    alias_map.setdefault(alias, alias_target)
            dim_queue['aliases'] = alias_map
            dim_queue['join_recommendation'] = 'Prefer QueueKey; QueueID only for Task facts'
        for dim_entry in dimensions.values():
            dim_entry.pop('alias_target', None)

        group_by_suggestions: Dict[str, List[str]] = {}
        for fact_name, schema in st.get('star_schema', {}).items():
            dimensions_info = schema.get('dimensions', [])
            suggestions: List[str] = []
            for dimension_payload in dimensions_info:
                dimension_table = dimension_payload.get('dimension_table')
                if not dimension_table:
                    continue
                label_name = self._select_dimension_label(dimension_table, md)
                if label_name:
                    candidate = f"{dimension_table}[{label_name}]"
                    if candidate not in suggestions:
                        suggestions.append(candidate)
            if suggestions:
                group_by_suggestions[fact_name] = suggestions[:5]

        relationships: List[Dict[str, Any]] = []
        column_types: Dict[Tuple[str, str], str] = {
            (column.get('table_name'), column.get('column_name')): (column.get('data_type') or '')
            for column in md.get('columns', [])
        }
        default_time_keys_map = {
            fact_name: payload.get('default_time_key')
            for fact_name, payload in fact_time_axes.items()
        }
        for relationship in md.get('relationships', []):
            if not self._is_business_relationship(relationship):
                continue
            from_table = relationship.get('from_table')
            from_column = relationship.get('from_column')
            to_table = relationship.get('to_table')
            to_column = relationship.get('to_column')
            is_active = self._safe_bool(relationship.get('is_active'))
            dtype_from = column_types.get((from_table, from_column), '')
            dtype_to = column_types.get((to_table, to_column), '')
            type_mismatch = self._coerce_type(data_type=dtype_from) != self._coerce_type(data_type=dtype_to)
            relationship_call = (
                f"USERELATIONSHIP('{from_table}'[{from_column}], '{to_table}'[{to_column}])"
                if from_table and from_column and to_table and to_column else None
            )
            userelationship_hint = None
            default_key = default_time_keys_map.get(from_table)
            if not is_active and relationship_call:
                if default_key and default_key != from_column:
                    userelationship_hint = (
                        f"默认活动键为 {default_key}；按 {from_column} 口径分析时调用 {relationship_call}。"
                    )
                else:
                    userelationship_hint = f"该关系为非活动状态；需要时调用 {relationship_call}。"
            relationships.append({
                'from': f"{from_table}[{from_column}]",
                'to': f"{to_table}[{to_column}]",
                'direction': relationship.get('cross_filter_direction', 'Single'),
                'inactive': not is_active,
                'type_mismatch': type_mismatch,
                'userelationship_hint': userelationship_hint
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
            dependencies = self._extract_measure_dependencies(measure.get('dax_expression'))
            measures[measure_name] = {
                'category': category,
                'unit': unit,
                'fact_hint': measure.get('table_name'),
                'depends_on': dependencies
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
    SUMMARIZE('{table_name}', '{table_name}'[{column_name}], "cnt", COUNTROWS('{table_name}')),
    [cnt], DESC
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
            'version': '2.0',
            'date_axis': date_axis,
            'facts': facts,
            'dimensions': dimensions,
            'relationships': relationships,
            'measures': measures,
            'enums': enums,
            'group_by_suggestions': group_by_suggestions,
            'warnings': warnings,
            'time_defaults': time_defaults
        }

        with open('nl2dax_index.json', 'w', encoding='utf-8') as handle:
            json.dump(index, handle, ensure_ascii=False, indent=2)
        return index

    # ---------- Build Outputs ----------
    def _prioritize_columns(self, table_name: str, cols: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """按照信息密度对列进行排序，优先输出主键/日期键/外键/标签列。

        参数:
            table_name: 当前处理的表名，用于可能的表级特判。
            cols: 列元数据列表。

        返回:
            调整顺序后的列列表。
        """

        def _score(column: Dict[str, Any]) -> Tuple[int, int, int, int, int, int]:
            """计算列优先级分数，值越小越靠前。"""
            name = (column.get('column_name') or '').lower()
            dtype = (column.get('data_type') or '').lower()
            # 主键、唯一键优先
            is_pk = 0 if self._safe_bool(column.get('is_key')) or self._safe_bool(column.get('is_unique')) else 1
            # 日期键（DateKey 结尾）次之
            is_time_key = 0 if name.endswith('datekey') else 1
            # 日期/时间类型列优先
            is_date = 0 if any(flag in dtype for flag in ['date', 'datetime', 'timestamp']) else 1
            # 外键（Key 结尾）
            is_fk = 0 if name.endswith('key') else 1
            # 标签列（名称/标题）
            is_label = 0 if re.search(r'(name|title)$', name) else 1
            return (is_pk, is_time_key, is_date, is_fk, is_label, len(name))

        sorted_cols = sorted(cols, key=_score)
        return sorted_cols

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
        measure_definitions: List[Dict[str, str]] = []
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
        suggestions_map = (self.nl2dax_index or {}).get('group_by_suggestions', {})
        other_tables: List[str] = []
        for t in md.get('business_tables', []):
            tname = t.get('table_name', '')
            ttype = st.get('table_types', {}).get(tname, 'other')
            if ttype == 'other' and not self.show_other_tables_in_main:
                other_tables.append(tname)
                continue
            parts.append(f"### 📊 {tname} ({ttype})")
            if t.get('description'):
                parts.append(f"*{t['description']}*\n")

            tcols = [c for c in md.get('columns', []) if c.get('table_name') == tname and not self._safe_bool(c.get('is_hidden'))]
            tcols = self._prioritize_columns(tname, tcols)
            if tcols:
                parts.append("| 列名 | 数据类型 | 说明 | 特性 |")
                parts.append("|------|----------|------|------|")
                column_limit = len(tcols)
                if self.compact_mode:
                    column_limit = min(len(tcols), self.max_columns_per_table)
                for c in tcols[:column_limit]:
                    name = c.get('column_name',''); dtype = c.get('data_type',''); desc = c.get('description','') or ''
                    feats: List[str] = []
                    if self._safe_bool(c.get('is_key')):      feats.append('🔑主键')
                    if self._safe_bool(c.get('is_unique')):   feats.append('✨唯一')
                    if not self._safe_bool(c.get('is_nullable')): feats.append('❗非空')
                    parts.append(f"| `{name}` | {dtype} | {desc} | {' '.join(feats)} |")
                if len(tcols) > column_limit:
                    parts.append(f"\n*...还有{len(tcols)-column_limit}个列 (紧凑模式受限于 {self.max_columns_per_table} 列)*")
            if ttype == 'fact':
                suggestions = suggestions_map.get(tname, [])[:3]
                if suggestions:
                    parts.append("".join([
                        "*推荐分组列*: ",
                        ", ".join(f"`{suggestion}`" for suggestion in suggestions)
                    ]))
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
                dax = (m.get('dax_expression') or '')
                dax = re.sub(r'==', '=', dax)
                if self.include_measure_dax:
                    parts.append(f"#### [{nm}]")
                    parts.append("```dax")
                    parts.append(dax if len(dax) <= 1200 else (dax[:1200] + '...'))
                    parts.append("```")
                    if m.get('format_string'): parts.append(f"**格式**: {m['format_string']}")
                    if m.get('description'):   parts.append(f"**说明**: {m['description']}")
                else:
                    bullet = f"- **{nm}**"
                    description = m.get('description') or ''
                    if description:
                        bullet += f"：{description}"
                    parts.append(bullet)
                    format_string = m.get('format_string')
                    if format_string:
                        parts.append(f"  - 格式: {format_string}")
                    measure_definitions.append({'name': nm, 'dax': dax})
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
                    blank_ratio_value = row.get('blank_ratio')
                    coverage_value = row.get('coverage')
                    blank_ratio = 'N/A' if blank_ratio_value is None else f"{blank_ratio_value:.2%}"
                    coverage = 'N/A' if coverage_value is None else f"{coverage_value:.2%}"
                    blank_fk_value = row.get('blank_fk')
                    orphan_fk_value = row.get('orphan_fk')
                    blank_fk_text = 'N/A' if blank_fk_value is None else str(blank_fk_value)
                    orphan_fk_text = 'N/A' if orphan_fk_value is None else str(orphan_fk_value)
                    parts.append(
                        f"| {row.get('from')} | {row.get('to')} | {blank_ratio} | {coverage} | "
                        f"{row.get('severity','green').upper()} | {blank_fk_text} | {orphan_fk_text} |"
                    )
            if lint_msgs:
                parts.append("\n**模型提示**")
                for message in lint_msgs:
                    parts.append(f"- {message}")
            inactive_relations = [
                rel for rel in (self.nl2dax_index or {}).get('relationships', [])
                if rel.get('inactive')
            ]
            if inactive_relations:
                parts.append("\n**非活动关系与 USERELATIONSHIP 建议**")
                for rel in inactive_relations:
                    hint_text = rel.get('userelationship_hint') or '此关系为非活动状态，按需使用 USERELATIONSHIP() 激活过滤。'
                    parts.append(f"- `{rel.get('from')} → {rel.get('to')}`: {hint_text}")
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
            parts.append("- **维度展示列**: label 与 aliases 映射已收录，供 NL2DAX 快速对齐术语")
            parts.append("- **推荐分组列**: group_by_suggestions 提供事实表常用维度字段")
            parts.append("- **度量依赖图**: depends_on 字段列出所引用的度量与列")
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
        if not self.include_measure_dax and measure_definitions:
            parts.append("### 度量值定义（完整 DAX）\n")
            parts.append("<details>")
            parts.append("<summary>点击展开查看全部度量定义</summary>\n")
            for definition in measure_definitions:
                parts.append(f"#### [{definition['name']}]")
                parts.append("```dax")
                parts.append(definition['dax'])
                parts.append("```")
            parts.append("</details>\n")
        if md.get('auto_date_tables'):
            parts.append("### 自动生成的日期表")
            parts.append("Power BI为以下日期列自动创建了时间智能表：\n")
            for t in md['auto_date_tables'][:10]:
                parts.append(f"- `{t}` (hidden)")
            if len(md['auto_date_tables']) > 10:
                parts.append(f"- ...共{len(md['auto_date_tables'])}个")
        if other_tables:
            parts.append("### other 类型表一览")
            parts.append("以下表在主文中隐藏以保持紧凑，可在此处查阅：")
            for table_name in other_tables:
                parts.append(f"- `{table_name}`")
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
        """将多种布尔表示安全转换为 bool。"""
        try:
            if value is None:
                return False
            if isinstance(value, (float, int)) and pd.isna(value):
                return False
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes", "y", "t"}:
                    return True
                if lowered in {"false", "0", "no", "n", "f", ""}:
                    return False
                return False
            return bool(value)
        except Exception as error:
            print(f"⚠️ _safe_bool 转换失败: {error}")
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
