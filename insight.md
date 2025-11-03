把每个主题的分析结果映射到叙事实例（SCQA / PIR（Problem‑Insight‑Risk）/ Pyramid）。输出“洞察卡”：结论一句话 → 证据图表 → 归因 → 建议 → 溯源（DAX + 切片 + 时间窗）→ 置信度。




## 5) 直接可得的商业洞察（基于模型能做什么）
1. **客户体验 → 过程诊断**

   * `DSAT%` 按 **根因路径（RootPath）× 产品 × 队列 × 国家 × 语言** 下钻，定位不满意的**结构性来源**。
   * `FCR` / `Resolution Rate` × **产品/队列**：发现一次解决与最终解决的**断点**落在何处（流程/知识/路由）。
   * `CPE CSAT` 与 `Avg IRHours`、`IRHour@P75` 的相关观察：**初响应时效**与满意度之间的弹性。

2. **升级健康（Escalation Health）**

   * `# T3 Bugs` / `# IcM Tickets` × **产品**：定位哪条产品线引发**高强度技术升级**，辅助研发/质量改进闭环。
   * `# T1->IcM Tickets` × **队列/站点**：识别是否存在**越级**或**直达 IcM**的异常路径（培训/流程问题）。

3. **在手治理（WIP Health）**

   * `% Open > 14 days` × **队列/国家**：超龄在手分布，识别“拖单口袋”。
   * `Avg Open Age` × **产品**：长期慢病产品 vs 短平快产品的**运营差异**。

4. **节奏与供需（Rhythm & Capacity）**

   * `# IPD` / `# CPD` × **MonthName/Weekday**：看**需求节律**与**产能节奏**是否匹配。
   * `# Avg Daily Open` 结合 `# Case Created/Closed`：识别**净入库**（Backlog）趋势。

5. **自助分流（Deflection）**

   * `% Self‑Help Deflection` × **产品**：哪些产品的自助能力强/弱；与 `FCR` 对照，评估**知识库/流程编排**价值。
   * `# CreatedTickets` 与 `TotalFlows` 的 **“转人工漏斗”** 形态，指导入口体验调优。




### 故事1｜《满意度的“负片”》：用 DSAT 拍出客户体验的真实形状
* **要回答的问题**：

  1. 最近 3 个月 **% DSAT(1,2)** / **% DSAT(1,2,3)** 的**主贡献因子**是什么？
  2. 哪些 **根因路径（RootPath）× 产品 × 队列 × 国家/语言** 组合抬高了 DSAT？
* **用到的指标**：`% DSAT(1,2)`, `% DSAT(1,2,3)`, `# CSAT Response`, `CPE CSAT`, `% FCR`, `% Resolution Rate`。
* **切片维度**：`RootPath`、`Product Name`、`Queue Name`、`Country`、`Delivery Language`、`MonthName`。
* **关键图表**：

  * 瀑布/帕累托：按 RootPath 对 DSAT% 的贡献排序；
  * 矩阵热力：产品×队列的 DSAT%；
  * 散点：`%FCR` vs `DSAT%`（点大小为 `# CSAT Response`）。
* **典型洞察模板**：

  * “**3 个 RootPath** 贡献了 **68%** 的 DSAT，上榜组合集中在 **产品A×队列Q1** 与 **国家C×语言L**。”
  * “**低 FCR 高 DSAT** 的象限里，**Q1 与 Q3** 占比过高，可能是知识/流程瓶颈。”
* **行动建议**：对 Top 根因组合启动 **知识库Card/流程剧本/复训**；承诺“**两周内**复测 DSAT% 与 FCR 的变化”。
* **常见误区**：只讲均值 `CPE CSAT`，**忽略分布尾部**与样本基数 `# CSAT Response`。

---

### 故事2｜《速度就是口碑》：首响（IRHour）如何撬动 CSAT

* **钩子**：**IRHour@P75** 是“尾部长”的体感指标，比均值更能反映客户不爽。
* **问题**：

  1. 哪些队列/站点的 `IRHour@P75` 明显高于整体？
  2. `IRHour@P75` 与 `CPE CSAT`/`%DSAT` 的关联如何？
* **指标**：`Avg IRHours`, `IRHour@P75`, `CPE CSAT`, `% DSAT(1,2)`, `% FWR`。
* **维度**：`Queue Name`, `SiteName`, `Product Name`, `MonthName`。
* **图表**：

  * 箱线/分位趋势：队列的 IRHour 分布；
  * 双轴：`IRHour@P75` vs `CPE CSAT`（月度）。
* **洞察模板**：某些队列 **P75 明显偏右**，而均值正常 → **容量峰值/排班**问题；**改 P75** 往往比拉均值更快改善口碑。
* **动作**：**高峰时段排班**与**自动应答分流**优化；对 `P75` 设 **SLA 门限**（如 4h）。

---

### 故事4｜《14天的拐点》：超龄在手是怎样“长出”DSAT的

* **钩子**：超过 14 天未结案的在手是**口碑地雷**。
* **问题**：

  1. `% Open > 14 days` 的**主要来源队列/产品**是哪些？
  2. 与 `DSAT%` 的**时滞关系**如何？
* **指标**：`% Open > 14 days`, `# Open Vol`, `Avg Open Age`, `% DSAT(1,2)`, `# Case Created/Closed`。
* **维度**：`Queue Name`, `Product Name`, `RootPath`, `Country`, `MonthName`。
* **图表**：

  * 指标组合卡片：`%>14d`、`Avg Open Age`；
  * 滞后散点：`%>14d(t)` vs `DSAT%(t+1月)`。
* **洞察模板**：**Q3** 队列 `%>14d` 出清不力，**下月 DSAT% 抬升**；根因集中于 **RootPath=等待外部依赖**。
* **动作**：为超龄件设计**快车道**；每周拉链报表，规定**出清率**。
* **超过阈值分层策略**：如 14/30/45 天不同处置。

---

### 故事5｜《升级的回音壁》：T3/IcM 是否在放大客户噪声？

* **钩子**：**T3 Bugs / IcM** 是产品质量与支持边界的“声量放大器”。
* **问题**：

  1. `# T3 Bugs`、`# IcM Tickets` 在**产品×版本**（如可得）上的分布？
  2. `T1->IcM` 直达是否异常集中在某些队列/站点？
* **指标**：`# Escalation Tickets`, `# T3 Bugs`, `# T3 Tickets`, `# IcM Tickets`, `# T1->IcM Tickets`, `# T1->IcM Tickets SAP`。
* **维度**：`Product Name`, `Queue Name`, `SiteName`, `MonthName`, `Partner Name`, `Delivery Language`。
* **图表**：

  * 漏斗：Created → T2 → T3 → IcM；
  * 旭日图/树图：产品→模块（若 RootPath 能映射到模块）。
* **洞察模板**：**产品B** 的 **T3 Bugs/千创建** 值为同类 2.1×；`T1->IcM` 在 **站点S1** 明显偏高，或存**越级路由**。
* **动作**：联合研发设 **Bug 火线清零周**；对越级路由加 **拦截/回流规则**。
* **误区**：盲目压升级量；应关注**单位创建量的升级率**与**修复闭环时效**。

---

### 故事6｜《自助的 ROI》：Deflection 不只是美观曲线

* **钩子**：自助分流提升并非“越高越好”，关键在**良性分流**与**不误杀高价值问题**。
* **问题**：

  1. `% Self‑Help Deflection` 与 `# CreatedTickets/# TotalFlows` 的**弹性区间**？
  2. 哪些产品自助强，但 `DSAT%` 仍高（提示**内容质量/引导**问题）？
* **指标**：`% Self‑Help Deflection`, `# TotalFlows`, `# CreatedTickets`, `% DSAT(1,2)`, `% FCR`。
* **维度**：`Product Name`, `MonthName`。
* **图表**：

  * 漏斗：TotalFlows → CreatedTickets；
  * 四象限：Deflection% vs DSAT%（点大小为 Flows）。
* **洞察模板**：**产品C** 分流率高但 DSAT% 未降，说明**自助误杀**或**内容误导**；应做**内容 A/B** 与**召回率/精确率**监控。
* **动作**：为高流量问题建立**决策树/对话编排**；把低置信度意图**快速转人工**。
* **误区**：只追分流率，不看**人工回流率**与**后续满意度**。

---

### 故事7｜《语言与公平》：本地化不是翻译，是时效与路径公平

* **钩子**：语言与国家是体验落差的“隐形变量”。
* **问题**：

  1. 不同 `Delivery Language` 的 `IRHour@P75`/`%FCR`/`DSAT%` 是否系统性偏差？
  2. 某语言是否被路由到**时效较差的队列/站点**？
* **指标**：`IRHour@P75`, `% FCR`, `% DSAT(1,2)`, `# Case Created`, `Avg DTS`。
* **维度**：`Delivery Language`, `Country`, `Queue Name`, `SiteName`。
* **图表**：

  * 小提琴/箱线：不同语言的 IRHour 分布；
  * Sankey：`Language → Queue → Site` 路由流。
* **洞察模板**：**语言L2** 在 **站点S2** 的首响尾部长 + 低 FCR，建议**路由回平**或**补能**。
* **动作**：路由策略按 SLA 对不同语言设**守门阈**；多语知识库**质量门**。
* **误区**：把语言问题简单归因“沟通效率”；常见根因是**运力配置/路由**。

---

### 故事10｜《首周定生死》：%FWR 的真实商业价值

* **钩子**：**首周解决率（%FWR）** 是“问题是否会发酵”的前置拐点。
* **问题**：

  1. 哪些产品/队列的 `%FWR` 最低？与 `DSAT%`、`升级率` 的相关性？
  2. 提升 `%FWR` 1 个点，对 `DSAT%` 的边际影响？（可做队列内对照）
* **指标**：`% FWR`, `% DSAT(1,2)`, `# T3 Tickets/# Escalation Tickets`, `Avg DTC/DTS`。
* **维度**：`Product Name`, `Queue Name`, `MonthName`。
* **图表**：

  * 漏斗：Created → 首周已解决；
  * 分组条：提升 `%FWR` 前后 `DSAT%/升级率` 对比。
* **洞察模板**：**产品D** 提升 `%FWR` 后，**升级率** 明显下降，**DSAT%** 收敛。
* **动作**：首周**专班/绿色通道**，重点处理**高风险根因**。

---

### 阶段 2: 洞察主题生成 (Thematic Hypothesis Generation)

在理解了“有什么”之后，LLM需要像分析师一样提出“要查什么”。它应该基于领域知识，自动生成几个核心的分析“故事线”或“主题”。

根据你的模型，我建议启动以下三个核心主题：

1.  **主题一：客户体验与满意度 (The Customer Experience Journey)**

      * *假设：* 客户满意度（CSAT）受特定产品、根本原因或队列的影响。
      * *目标：* 找出导致客户不满（DSAT）的关键驱动因素。

2.  **主题二：运营效率与吞吐量 (Operational Efficiency & Throughput)**

      * *假设：* 工单的流入（Created）和流出（Closed）速度不匹配，导致解决时长（DTS）发生变化。
      * *目标：* 分析工单处理漏斗，识别效率瓶颈。

3.  **主题三：积压健康度与升级分析 (Backlog Health & Escalation Analysis)**

      * *假设：* 积压工单（Open Cases）的平均时长（Avg Open Age）正在增长，并且特定问题正导致过多的升级（Escalation）。
      * *目标：* 监控积压风险，并定位导致问题升级（如T3 Bugs）的热点区域。

---

## ① DSAT 降低 30 天冲刺（Stop the Bleeding）— **86%**

**North‑Star KPI**：`% DSAT(1,2)`（或 `% DSAT(1,2,3)`）；**对照**：`CPE CSAT`、`% FCR`、`% Resolution Rate`
**涉及事实**：`vwpcse_factcustomersurvey`（主），联动 `incident_created`（首响）与 `opencasedaily`（超龄）

### 闭环（Sense → Diagnose → Decide → Act → Verify）

* **Sense（高层一屏）**：

  * 月度 `MonthName` 维度：`% DSAT(1,2)`、`# CSAT Response`。
  * 分布对照：`CPE CSAT`、`% FCR`。
* **Diagnose（深度优先路径）**：

  1. `Country` or `Product Name` or Other → 选出 **DSAT 最高的 Top3 国家**；
  2. 进入该国按 `Product Name` → 选 Top3；
  3. 进入产品按 `Queue Name` → 选 Top3；
  4. 进入队列按 `RootPath` → 锁定**贡献 80% DSAT 的根因**；
  5. 对该叶子组合联表：

     * 首响：`IRHour@P75`（`incident_created`）；
     * 在手：`% Open > 14 days`、`Avg Open Age`（`opencasedaily`）；
     * 解决能力：`% FCR`、`% Resolution Rate`（survey）。
       **停止条件**：叶子组合覆盖**≥80%**的 DSAT 超标差额，或样本数量 `# CSAT Response` < 30。
* **Decide（策略模板）**：

  * 若 `IRHour@P75` 超阈 → **排班/首响 SLA 加固**；
  * 若 `%>14d` 高 → **绿色通道 + 周度清零**；
  * 若 `FCR` 低 → **知识卡/决策树补齐**；
  * 若特定 `RootPath` 集中 → **专项缺陷/流程治理**。

* **Verify（验收与守护）**：

  * 30 天后复测：`% DSAT(1,2)`、`CPE CSAT` 改善幅度；
  * 守护指标：`# Case Created` 不显著下滑、`# T3/IcM` 不恶化。

**语义查询示例（可直接提问 LLM‑BI）**

* 「按国家/产品/队列/根因看**近 30 天** `% DSAT(1,2)`，列出 Top 10 组合与 `# CSAT Response`」
* 「对以上 Top 10 组合，显示 `IRHour@P75`、`% Open > 14 days`、`% FCR`」

**数据质量注意**：跨事实钻取时注意 `QueueKey/QueueID` 一致化或桥接。

---

## ④ 首周解决率（%FWR）提升计划：Golden Week — **74%**

**North‑Star KPI**：`% FWR`（首周解决率）
**对照**：`% DSAT(1,2)`、`# T3/IcM`、`Avg DTS/DTC`、`IRHour@P75`
**涉及事实**：`incident_created`（主），联动 survey/escalation/open

### 闭环

* **Sense**：以 `MonthName` × `Product` × `Queue` 看 `% FWR` 的尾部。
* **Diagnose（DFS）**：

  1. `Product Name` → 低 `%FWR` Top3；
  2. → `Queue Name` → 识别**队列差异**；
  3. → `RootPath` → 找到**过慢的根因**；
  4. 联动：`IRHour@P75`、`% Open > 14 days`、`# T3`（时效/复杂度驱动？）。
* **Decide**：对特定根因开 **D+1 复核**、**T+3 绿色通道**、**T+5 升级前置**。
* **Act**：建立**“首周专班”**：固定每日站会，清单化推进。
* **Verify**：两周后 `%FWR` 提升，同时 `DSAT%` 降、`T3/IcM` 降或持平。

**语义查询示例**

* 「按产品/队列/根因看 `% FWR` 的底部组合，附 `IRHour@P75` 与 `%>14d`」

---

## ⑤ 自助分流 ROI：Shift‑Left without Regret — **71%**

**North‑Star KPI**：`% Self‑Help Deflection`（需统一口径），**守护**：`% DSAT(1,2)`、`# CreatedTickets/# TotalFlows`
**涉及事实**：`selfhelpdeflection`（主），联动 survey/incident

### 闭环

* **Sense**：按 `Product` 看 `# TotalFlows`、`# CreatedTickets`、`% Deflection`。
* **Diagnose（DFS）**：

  1. `Product Name` → 高流量产品；
  2. → `MonthName` → 看最近变化；
  3. 叠加 `DSAT%/FCR`：识别**“高分流但体验不升”**的反常区。
* **Decide**：

  * 高置信意图→**强引导自助**；低置信→**快速转人工**；
  * 内容 AB：问题前 10 意图做**知识卡/流程编排**。
* **Act**：上线**对话编排**与**召回/精确率监控**；
* **Verify**：`# CreatedTickets` 降而 `DSAT%` 不升，`FCR` 持平或升。

**语义查询示例**

* 「按产品看 `# TotalFlows/ # CreatedTickets / % Self‑Help Deflection`，并与 `% DSAT(1,2)` 关联」

---

## ⑥ 队列专业化与动态路由：Put Work Where It Wins — **63%**

**North‑Star KPI**：组合目标（`Avg DTS` 下降、`% FCR` 上升、`% DSAT` 下降）
**涉及事实**：`incident_closed`（时效），`customersurvey`（体验），`incident_created`（首响/基数）

### 闭环

* **Sense**：构建**队列×产品**绩效面板：`Avg DTS`、`% FCR`、`% DSAT`。
* **Diagnose（DFS）**：

  1. `Product Name` → 找到**某产品的赢家队列**（`Avg DTS` 低、`%FCR` 高）；
  2. → `Queue Name` → 看该队列在其他产品是否也优；
  3. → `Language/Site` → 判断是否**语言/地域优势**导致。
* **Decide**：制定**专长映射**（Product→Queue），调整**路由权重**。
* **Act**：灰度 20% 流量给“赢家队列”。
* **Verify**：灰度期 `Avg DTS` 下降、`% FCR` 上升、`% DSAT` 不升；升级不恶化。

**语义查询示例**

* 「显示各队列在各产品的 `Avg DTS` 与 `% FCR`，按“低 DTS & 高 FCR”排序列出 Top 10 组合」

---

# 统一的「深度优先」打法（可复用骨架）

**DFS 钻取顺序（推荐）**
`MonthName → Country → Product Name → Queue Name → RootPath → Delivery Language → SiteName`
**停止条件**：

* 当前路径解释**≥80%** 目标差额（如 DSAT 超标 / 净入库）；或
* 样本量不足（如 `# CSAT Response` < 30；或日级窗口 < 14 天）；或
* 继续下钻指标改善**边际<5%**。
  **回溯策略**：若在某层未发现显著差异（p75/均值差<5%），回溯上一层并换维度（如从国家改看语言/伙伴）。
---

# 每个主题的交付件（你可按“极简美学”实现）

1. **一屏总览**：North‑Star + 守护指标（卡片）
2. **三张钻取图**：按 DFS 次序的 Top/Worst 榜（条形/热力/散点）
3. **行动清单**：自动生成（勾选式）
4. **验证页**：Before/After 对比 + 守护指标
5. **口径卡**：度量→语义模型字段映射（出自 `Atlas Measure Mapping`）

---


