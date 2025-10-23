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
        rel_quality: List[Dict[str, Any]] = []
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
            'date_dimensions': [],
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

        # date-dim columns & time-intelligence flag (only via primary date dim, not auto-date)
        primary_date_dim_names = set([
            t for t in analysis['table_types'].keys() if 'dimdate' in (t or '').lower() or 'calendar' in (t or '').lower()
        ])
        date_cols = [
            c for c in md.get('columns', [])
            if 'date' in (c.get('data_type') or '').lower()
            and (c.get('table_name') or '') not in md.get('auto_date_tables', [])
        ]
        for c in date_cols:
            time_intel = False
            for rel in md.get('relationships', []):
                if not self._safe_bool(rel.get('is_active')): continue
                if rel.get('from_table') == c.get('table_name') and rel.get('from_column') == c.get('column_name'):
                    if rel.get('to_table') in primary_date_dim_names:
                        time_intel = True
                        break
            analysis['date_dimensions'].append({
                'table': c.get('table_name'),
                'column': c.get('column_name'),
                'time_intelligence_enabled': time_intel
            })

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

    def _profile_time_anchor_for_table(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any],
        table: str
    ) -> Dict[str, Any]:
        def _score(col_name: str) -> float:
            n = (col_name or '').lower()
            if 'submitted' in n: return 6
            if 'sent' in n:      return 5
            if 'closed' in n:    return 4
            if 'created' in n:   return 3.5
            if 'calendar' in n:  return 3
            if 'date' in n:      return 2
            return 1

        candidates = [
            c.get('column_name') for c in md.get('columns', [])
            if c.get('table_name') == table and 'date' in (c.get('data_type') or '').lower()
        ]
        candidates = sorted(candidates, key=_score, reverse=True)

        if not candidates:
            return {
                'anchor_column': None, 'min': None, 'max': None, 'anchor': None,
                'nonblank': None, 'cnt7': None, 'cnt30': None, 'cnt90': None
            }

        for col in candidates:
            dax = f"""
EVALUATE
VAR _min = CALCULATE(MIN('{table}'[{col}]))
VAR _max = CALCULATE(MAX('{table}'[{col}]))
VAR _nonblank = COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{col}])))
VAR _cnt7  = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{col}]) && '{table}'[{col}] > _max - 7  && '{table}'[{col}] <= _max)), BLANK())
VAR _cnt30 = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{col}]) && '{table}'[{col}] > _max - 30 && '{table}'[{col}] <= _max)), BLANK())
VAR _cnt90 = IF(NOT ISBLANK(_max), COUNTROWS(FILTER('{table}', NOT ISBLANK('{table}'[{col}]) && '{table}'[{col}] > _max - 90 && '{table}'[{col}] <= _max)), BLANK())
RETURN
ROW("column","{col}","min",_min,"max",_max,"anchor",_max,"nonblank",_nonblank,"cnt7",_cnt7,"cnt30",_cnt30,"cnt90",_cnt90)
"""
            try:
                df = self.runner.evaluate(model_name, dax, workspace)
                if df.empty:
                    continue
                rec = df.iloc[0].to_dict()
                if pd.isna(rec.get('anchor')):
                    continue
                return {
                    'anchor_column': rec.get('column'),
                    'min': rec.get('min'),
                    'max': rec.get('max'),
                    'anchor': rec.get('anchor'),
                    'nonblank': int(rec.get('nonblank')) if pd.notna(rec.get('nonblank')) else None,
                    'cnt7': int(rec.get('cnt7')) if pd.notna(rec.get('cnt7')) else None,
                    'cnt30': int(rec.get('cnt30')) if pd.notna(rec.get('cnt30')) else None,
                    'cnt90': int(rec.get('cnt90')) if pd.notna(rec.get('cnt90')) else None
                }
            except Exception:
                continue

        return {
            'anchor_column': candidates[0],
            'min': None, 'max': None, 'anchor': None, 'nonblank': None, 'cnt7': None, 'cnt30': None, 'cnt90': None
        }

    # ---------- Relationship quality checks ----------
    def _relationship_quality_checks(
        self,
        model_name: str,
        workspace: Optional[str],
        md: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        tips: List[Dict[str, Any]] = []
        col_type: Dict[Tuple[str, str], str] = {}
        for c in md.get('columns', []):
            col_type[(c.get('table_name'), c.get('column_name'))] = (c.get('data_type') or '').lower()

        # Lint: example for Queue dimension dual keys
        to_table_groups: Dict[str, set] = {}
        for r in md.get('relationships', []):
            if not self._safe_bool(r.get('is_active')): continue
            to_table_groups.setdefault(r.get('to_table'), set()).add(r.get('to_column'))
        if 'vwpcse_dimqueue' in to_table_groups:
            cols = {c.lower() for c in to_table_groups['vwpcse_dimqueue']}
            if 'queuekey' in cols and 'queueid' in cols:
                tips.append({'type': 'lint', 'message': 'Queue 维度存在 QueueKey 与 QueueID 并行连接；建议统一代理键或加桥表。'})

        # Per-relationship blank/orphan checks
        for r in md.get('relationships', []):
            if not self._safe_bool(r.get('is_active')): continue
            ft, fc = r.get('from_table'), r.get('from_column')
            dt, dc = r.get('to_table'), r.get('to_column')
            if not ft or not fc or not dt or not dc: continue

            blank_fk = None
            try:
                dax_blank = f"""EVALUATE ROW("blank_fk", COUNTROWS(FILTER('{ft}', ISBLANK('{ft}'[{fc}]))) )"""
                dfb = self.runner.evaluate(model_name, dax_blank, workspace)
                if not dfb.empty:
                    blank_fk = int(dfb.iloc[0, 0])
            except Exception:
                pass

            orphan_fk = None
            t_from = (col_type.get((ft, fc)) or '')
            t_to = (col_type.get((dt, dc)) or '')
            if t_from and t_to and t_from.split()[0] == t_to.split()[0]:
                try:
                    dax_orphan = f"""EVALUATE ROW(
                        "orphan_fk",
                        COUNTROWS(
                            EXCEPT( VALUES('{ft}'[{fc}]), VALUES('{dt}'[{dc}]) )
                        )
                    )"""
                    dfo = self.runner.evaluate(model_name, dax_orphan, workspace)
                    if not dfo.empty:
                        orphan_fk = int(dfo.iloc[0, 0])
                except Exception:
                    pass
            else:
                tips.append({'type': 'lint',
                             'message': f"关系 {ft}[{fc}] → {dt}[{dc}] 两端数据类型不同（{t_from} vs {t_to}），无法做孤儿检查，建议统一类型。"})

            tips.append({
                'type': 'rel_quality',
                'from': f"{ft}[{fc}]",
                'to': f"{dt}[{dc}]",
                'blank_fk': blank_fk,
                'orphan_fk': orphan_fk
            })
        return tips

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

        # Example 3: Median CSAT by Queue with data-driven anchor (if survey fact found)
        if 'vwpcse_factcustomersurvey' in table_types:
            # Detect queue text column
            dimq = 'vwpcse_dimqueue' if 'vwpcse_dimqueue' in dim_tables else None
            dimq_text = None
            if dimq:
                for c in md.get('columns', []):
                    if c.get('table_name') == dimq and any(t in (c.get('data_type') or '').lower() for t in ['text','string']):
                        dimq_text = c.get('column_name'); break
            dimq_text = dimq_text or 'Queue Name'

            # Detect submitted/sent date column in fact (date type)
            survey_date_col = None
            survey_cols = [c for c in md.get('columns', []) if c.get('table_name') == 'vwpcse_factcustomersurvey']
            for prefer in ['SubmittedDate', 'SentDate']:
                if any((c.get('column_name') == prefer and 'date' in (c.get('data_type') or '').lower()) for c in survey_cols):
                    survey_date_col = prefer; break

            if survey_date_col:
                dax = f"""EVALUATE
VAR AnchorDate = CALCULATE(MAX('vwpcse_factcustomersurvey'[{survey_date_col}]))
VAR Period = DATESINPERIOD('vwpcse_dimdate'[CalendarDate], AnchorDate, -90, DAY)
VAR Dates = CALCULATETABLE(VALUES('vwpcse_dimdate'[CalendarDate]), Period)
RETURN
TOPN(
  20,
  ADDCOLUMNS(
    VALUES('vwpcse_dimqueue'['{dimq_text}']),
    "Responses",  CALCULATE([# CSAT Response], TREATAS(Dates, 'vwpcse_factcustomersurvey'[{survey_date_col}])),
    "Median CSAT",
      CALCULATE(
        MEDIANX(
          FILTER('vwpcse_factcustomersurvey', NOT ISBLANK('vwpcse_factcustomersurvey'[CsatScore])),
          'vwpcse_factcustomersurvey'[CsatScore]
        ),
        TREATAS(Dates, 'vwpcse_factcustomersurvey'[{survey_date_col}])
      )
  ),
  [Responses], DESC
)"""
                examples.append({
                    'title': '队列的Median CSAT（数据锚点：最近可用日期，窗口90天）',
                    'description': '当 Submitted/Sent 与日期维度无活动关系时使用 TREATAS',
                    'dax': dax,
                    'category': 'time_series'
                })

        # Example 4: Closed cases by country with anchor on closed date
        if 'vwpcse_factincident_closed' in table_types and 'vwpcse_dimgeography' in dim_tables:
            # pick a country column
            country_col = 'Country'
            if not any(c.get('table_name') == 'vwpcse_dimgeography' and c.get('column_name') == country_col for c in md.get('columns', [])):
                # pick any text
                for c in md.get('columns', []):
                    if c.get('table_name') == 'vwpcse_dimgeography' and any(t in (c.get('data_type') or '').lower() for t in ['text','string']):
                        country_col = c.get('column_name'); break
            # find closed date column
            closed_date_col = None
            for c in md.get('columns', []):
                if c.get('table_name') == 'vwpcse_factincident_closed' and 'date' in (c.get('data_type') or '').lower():
                    if 'closed' in (c.get('column_name') or '').lower():
                        closed_date_col = c.get('column_name'); break
            closed_date_col = closed_date_col or 'Case Closed Date'
            dax2 = f"""EVALUATE
VAR AnchorDate = CALCULATE(MAX('vwpcse_factincident_closed'['{closed_date_col}']))
VAR Period = DATESINPERIOD('vwpcse_dimdate'[CalendarDate], AnchorDate, -90, DAY)
RETURN
TOPN(
  10,
  SUMMARIZECOLUMNS(
    'vwpcse_dimgeography'['{country_col}'],
    Period,
    "# Closed", [# Case Closed]
  ),
  [# Closed], DESC
)"""
            examples.append({
                'title': '按国家看关闭量 Top 10（以关闭日期为锚点，90天）',
                'description': '使用活动关系的简单窗口（无需 TREATAS）',
                'dax': dax2,
                'category': 'ranking'
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
        if st.get('date_dimensions'):
            guide['common_patterns'].append("使用日期维度进行时间序列分析")
        return guide

    # ---------- Build Outputs ----------
    def _build_markdown_document(
        self,
        model_name: str,
        md: Dict[str, Any],
        st: Dict[str, Any],
        examples: List[Dict[str, Any]],
        guide: Dict[str, Any],
        profiles: Dict[str, Any] = None,
        rel_quality: List[Dict[str, Any]] = None
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
        parts.append("9. [附录](#附录)\n")

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
            parts.append("| 源(外键) | 目标(主键) | 外键空值 | 孤儿键 |")
            parts.append("|----------|------------|---------|-------|")
            for tip in rel_quality:
                if tip.get('type') != 'rel_quality': continue
                parts.append(
                    f"| {tip.get('from')} | {tip.get('to')} | "
                    f"{'' if tip.get('blank_fk') is None else tip.get('blank_fk')} | "
                    f"{'' if tip.get('orphan_fk') is None else tip.get('orphan_fk')} |"
                )
            lint_msgs = [t['message'] for t in rel_quality if t.get('type') == 'lint']
            if lint_msgs:
                parts.append("\n**模型提示**")
                for m in lint_msgs:
                    parts.append(f"- {m}")
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

        # 附录
        parts.append("## 附录\n")
        if st.get('date_dimensions'):
            parts.append("### 可用的日期维度\n")
            parts.append("| 表 | 列 | 时间智能 |")
            parts.append("|-----|-----|----------|")
            for d in st['date_dimensions']:
                ti = "✅ 通过主日期维度" if d.get('time_intelligence_enabled') else "❌ 未通过主日期维度"
                parts.append(f"| {d.get('table')} | {d.get('column')} | {ti} |")
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
                             profiles: Dict[str, Any] = None, rel_quality: List[Dict[str, Any]] = None) -> str:
        return json.dumps({
            'model_name': model_name,
            'generated_at': self.analysis_timestamp,
            'metadata': md,
            'structure_analysis': st,
            'dax_examples': examples,
            'usage_guide': guide,
            'profiles': profiles or {},
            'relationship_quality': rel_quality or []
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
