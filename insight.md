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

