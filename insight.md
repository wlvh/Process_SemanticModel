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
