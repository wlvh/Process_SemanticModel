# PCSE AI - 完整技术文档

**生成时间**: 2025-10-15T02:31:57.755828
**文档版本**: 1.3

## 目录
1. [模型概述](#模型概述)
2. [数据新鲜度与时间锚点](#数据新鲜度与时间锚点)
3. [数据结构](#数据结构)
4. [度量值参考](#度量值参考)
5. [关系图](#关系图)
6. [关系完整性体检](#关系完整性体检)
7. [DAX查询示例](#dax查询示例)
8. [使用指南](#使用指南)
9. [附录](#附录)

## 模型概述

### 关键统计
- **业务表数量**: 19
- **度量值数量**: 48
- **关系数量**: 51
- **自动日期表**: 38个（已自动创建）

## 数据新鲜度与时间锚点

| 事实表 | 锚点列 | 最小日期 | 最大日期 | 锚点日期 | 非空(锚点列) | 近7天 | 近30天 | 近90天 | 行数 |
|--------|--------|----------|----------|----------|-------------|------|-------|-------|------|
| vwpcse_factcustomersurvey | SubmittedTime |  |  |  |  |  |  |  | 170681 |
| vwpcse_factescalationcase | EscalationCreatedDateTime |  |  |  |  |  |  |  | 65766 |
| vwpcse_factincident_created | Case Closed Date |  |  |  |  |  |  |  | 885078 |
| vwpcse_factopencasedaily | ClosedDate |  |  |  |  |  |  |  | 12965323 |
| vwpcse_factselfhelpdeflection |  |  |  |  |  |  |  |  | 242883 |
| vwpcse_facttask_created | Task Closed Date |  |  |  |  |  |  |  | 265123 |
| vwpcse_factincident_closed | Case Closed Date |  |  |  |  |  |  |  | 885078 |
| vwpcse_facttask_closed | Task Closed Date |  |  |  |  |  |  |  | 265123 |

> **提示**：示例查询默认使用上表的“锚点日期 + 90 天”窗口；若近 90 天为 0，请改用“上月/上季度”等固定窗口。

## 数据结构

### 📊 vwpcse_dimgeography (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `GeographyKey` | Integer |  | ✨唯一 ❗非空 |
| `TimeZone` | Text |  |  |
| `Area` | Text |  |  |
| `Country` | Text |  |  |

### 📊 vwpcse_dimdate (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `DateKey` | Integer |  | ✨唯一 ❗非空 |
| `CalendarDate` | Date |  |  |
| `Year` | Integer |  |  |
| `Month` | Integer |  |  |
| `MonthStartDate` | Date |  |  |
| `MonthNameShort` | Text |  |  |
| `MonthName` | Text |  |  |
| `IsWeekDay` | Integer |  |  |
| `WeekStartDate` | Date |  |  |
| `WeekEndDate` | Date |  |  |
| `MonthEndDate` | Date |  |  |
| `Fiscal Year` | Text |  |  |
| `Fiscal Month` | Text |  |  |
| `Fiscal Week` | Text |  |  |

### 📊 vwpcse_dimpartner (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `PartnerKey` | Integer |  | ✨唯一 ❗非空 |
| `PartnerID` | Integer |  |  |
| `Partner Name` | Text |  |  |
| `IsManaged` | Integer |  |  |
| `Membership Tier` | Text |  |  |
| `Partner Type` | Text |  |  |
| `IsGlobalAlliance` | Text |  |  |
| `VOrgPartnerID` | Integer |  |  |
| `PartnerGlobalID` | Integer |  |  |
| `CapabilityScoreCategory` | Text |  |  |
| `Partner Tier` | Text |  |  |
| `PartnerOneID` | Integer |  |  |
| `CSPType` | Text |  |  |
| `MembershipLevel` | Text |  |  |
| `ProgramTypeLevel` | Text |  |  |

*...还有3个列*

### 📊 vwpcse_dimqueue (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `QueueKey` | Integer |  | ✨唯一 ❗非空 |
| `QueueID` | Text |  | ✨唯一 ❗非空 |
| `Queue Name` | Text |  |  |
| `Case Type` | Text |  |  |
| `Program` | Text |  |  |
| `IsPFSQueue` | Integer |  |  |

### 📊 vwpcse_dimrootcause (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `RootCauseKey` | Integer |  | ✨唯一 ❗非空 |
| `RootPath` | Text |  |  |
| `RootCause1` | Text |  |  |
| `RootCause2` | Text |  |  |
| `RootCause3` | Text |  |  |
| `RootCause4` | Text |  |  |
| `RootCause5` | Text |  |  |
| `IsLeaf` | Integer |  |  |
| `OCP priority` | Integer |  |  |
| `Bucket` | Text |  |  |
| `Bucket Group` | Text |  |  |
| `RootCause6` | Text |  |  |
| `RootCause7` | Text |  |  |

### 📊 vwpcse_dimsap (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `SAPKey` | Integer |  | ✨唯一 ❗非空 |
| `SapId` | Text |  |  |
| `Reporting Program` | Text |  |  |
| `Product Family` | Text |  |  |
| `Product Name` | Text |  |  |
| `Support Topic` | Text |  |  |
| `Support Subtopic` | Text |  |  |
| `WorkspaceId` | Text |  |  |
| `WorkspaceName` | Text |  |  |
| `Delivery Team` | Text |  |  |

### 📊 vwpcse_factcustomersurvey (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `SurveyKey` | Integer |  |  |
| `SurveyResponseId` | Text |  |  |
| `ServiceRequestKey` | Integer |  |  |
| `SentDateKey` | Integer |  |  |
| `SentDate` | Date |  |  |
| `SubmittedDateKey` | Integer |  |  |
| `CsatScore` | Integer |  |  |
| `Verbatim` | Text |  |  |
| `Verbatim Translation` | Text |  |  |
| `IsResolved` | Integer |  |  |
| `ResolvedCount` | Integer |  |  |
| `IsFirstContactResolved` | Text |  |  |
| `FirstContactResolvedCount` | Integer |  |  |
| `IsQualityReviewd` | Integer |  |  |
| `IsResolvedCount` | Integer |  |  |

*...还有14个列*

### 📊 vwpcse_factescalationcase (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `EscalationId` | Integer |  |  |
| `ServiceRequestKey` | Integer |  |  |
| `EscalationState` | Text |  |  |
| `EscalationStatus` | Text |  |  |
| `EscalationCreatedDateTime` | Date |  |  |
| `EscalationResolvedDateTime` | Date |  |  |
| `EscalationClosedDateTime` | Text |  |  |
| `EscalationCreatedDateKey` | Integer |  |  |
| `EscalationResolvedDateKey` | Integer |  |  |
| `EscalationClosedDateKey` | Integer |  |  |
| `EscalationPriority` | Text |  |  |
| `EscalationTier` | Text |  |  |
| `EscalationType` | Text |  |  |
| `ResolvedInDays` | Integer |  |  |
| `SLAMet` | Text |  |  |

*...还有33个列*

### 📊 vwpcse_factincident_created (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `ServiceRequestKey` | Integer |  |  |
| `IncidentID` | Text |  |  |
| `CaseCreatedDateKey` | Integer |  |  |
| `CaseClosedDateKey` | Integer |  |  |
| `InternalTitle` | Text |  |  |
| `Link` | Text |  |  |
| `DTC` | Number |  |  |
| `DTS` | Number |  |  |
| `IRHour` | Number |  |  |
| `CaseAge` | Integer |  |  |
| `CaseAgeWithDecimal` | Number |  |  |
| `TMPI` | Integer |  |  |
| `Case Closed Date` | Date |  |  |
| `Case Closed Time` | Date |  |  |
| `Case Created Date` | Date |  |  |

*...还有29个列*

### 📊 vwpcse_factopencasedaily (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `DateKey` | Integer |  |  |
| `CalendarDate` | Date |  |  |
| `IncidentID` | Text |  |  |
| `ClosedDate` | Date |  |  |
| `CreatedDate` | Date |  |  |
| `IsOpenCase` | Text |  |  |
| `OpenAge` | Integer |  |  |
| `Category_Common` | Text |  |  |
| `AgeingThreshold` | Text |  |  |
| `ServiceRequestKey` | Integer |  |  |
| `Status` | Text |  |  |
| `MaxModifiedDateTimeUTCKey` | Integer |  |  |
| `ServiceRequestClosedDateUTCKey` | Integer |  |  |
| `IsPendingPartnerConfirmation` | Text |  |  |

### 📊 vwpcse_factselfhelpdeflection (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `FactSelfDeflectionKey` | Integer |  |  |
| `DeflectionDateKey` | Integer |  |  |
| `SAPKey` | Integer |  |  |
| `State` | Text |  |  |
| `CreatedTicket` | Integer |  |  |
| `TotalFlows` | Integer |  |  |

### 📊 vwpcse_facttask_created (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `ServiceRequestTaskKey` | Integer |  |  |
| `IncidentTaskId` | Text |  |  |
| `ServiceRequestKey` | Integer |  |  |
| `TaskType` | Text |  |  |
| `State` | Text |  |  |
| `TaskCreatedDateKey` | Integer |  |  |
| `TaskClosedDateKey` | Integer |  |  |
| `TaskQueueId` | Text |  |  |
| `TaskSAPKey` | Integer |  |  |
| `CaseSAPKey` | Integer |  |  |
| `CaseQueueKey` | Integer |  |  |
| `PersonnelNumber` | Text |  |  |
| `SiteName` | Text |  |  |
| `Task Closed Date` | Date |  |  |
| `Task Created Date` | Date |  |  |

*...还有12个列*

### 📊 vwpcse_factincident_closed (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `ServiceRequestKey` | Integer |  |  |
| `IncidentID` | Text |  |  |
| `CaseCreatedDateKey` | Integer |  |  |
| `CaseClosedDateKey` | Integer |  |  |
| `InternalTitle` | Text |  |  |
| `Link` | Text |  |  |
| `DTC` | Number |  |  |
| `DTS` | Number |  |  |
| `IRHour` | Number |  |  |
| `CaseAge` | Integer |  |  |
| `CaseAgeWithDecimal` | Number |  |  |
| `TMPI` | Integer |  |  |
| `Case Closed Date` | Date |  |  |
| `Case Closed Time` | Date |  |  |
| `Case Created Date` | Date |  |  |

*...还有29个列*

### 📊 vwpcse_dimlanguage (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `Delivery Language` | Text |  |  |
| `LanguageKey` | Integer |  | ✨唯一 ❗非空 |

### 📊 vwpcse_dimsite (dimension)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `SiteName` | Text |  | ✨唯一 ❗非空 |
| `SiteTimeZone` | Text |  |  |
| `SiteName_Category` | Text |  |  |
| `SiteGroup` | Text |  |  |

### 📊 vwpcse_facttask_closed (fact)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `ServiceRequestTaskKey` | Integer |  |  |
| `IncidentTaskId` | Text |  |  |
| `ServiceRequestKey` | Integer |  |  |
| `IncidentID` | Text |  |  |
| `TaskType` | Text |  |  |
| `State` | Text |  |  |
| `TaskCreatedDateKey` | Integer |  |  |
| `TaskClosedDateKey` | Integer |  |  |
| `TaskQueueId` | Text |  |  |
| `TaskSAPKey` | Integer |  |  |
| `CaseSAPKey` | Integer |  |  |
| `CaseQueueKey` | Integer |  |  |
| `PersonnelNumber` | Text |  |  |
| `SiteName` | Text |  |  |
| `Task Closed Date` | Date |  |  |

*...还有12个列*

### 📊 Atlas_Dynamic Dim (other)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `Atlas_Dynamic Dim` | Text |  |  |

### 📊 Atlas_Dynamic Metrics (other)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `Atlas_Dynamic Metrics` | Text |  |  |
| `Category` | Text |  |  |
| `Category Order` | Integer |  |  |

### 📊 Atlas Table (other)
| 列名 | 数据类型 | 说明 | 特性 |
|------|----------|------|------|
| `Category` | Text |  |  |
| `Metric` | Text |  |  |
| `MetricOrder` | Integer |  |  |
| `CategoryOrder` | Integer |  |  |

## 度量值参考

### Filtered

#### [# DSAT(1,2)]
```dax
CALCULATE('vwpcse_factcustomersurvey'[# Survey],FILTER('vwpcse_factcustomersurvey','vwpcse_factcustomersurvey'[CsatScore] <=2 &&'vwpcse_factcustomersurvey'[CsatScore] <> BLANK()))
```
#### [# CSAT Response]
```dax
CALCULATE([# Survey],FILTER('vwpcse_factcustomersurvey','vwpcse_factcustomersurvey'[CsatScore] <> BLANK()))
```
#### [# DSAT(1,2,3)]
```dax
CALCULATE('vwpcse_factcustomersurvey'[# Survey],FILTER('vwpcse_factcustomersurvey','vwpcse_factcustomersurvey'[CsatScore] <=3 &&'vwpcse_factcustomersurvey'[CsatScore] <> BLANK()))
```
#### [# T2 Tickets]
```dax
CALCULATE([# Escalation Tickets], FILTER('vwpcse_factescalationcase' ,'vwpcse_factescalationcase'[EscalationTier] ="Tier2"))
```
#### [# T3 Bugs]
```dax
CALCULATE([# Escalation Tickets], FILTER('vwpcse_factescalationcase' ,'vwpcse_factescalationcase'[EscalationTier]="Tier3" && 'vwpcse_factescalationcase'[EscalationType] = "ADO"))
```
#### [# T2 Cases]
```dax
CALCULATE([# Escalation Cases], FILTER('vwpcse_factescalationcase', 'vwpcse_factescalationcase'[EscalationTier] ="Tier2"))
```
#### [# T3 Cases]
```dax
CALCULATE([# Escalation Cases], FILTER('vwpcse_factescalationcase', 'vwpcse_factescalationcase'[EscalationTier] ="Tier3"))
```
#### [# T3 Tickets]
```dax
CALCULATE([# Escalation Tickets], FILTER('vwpcse_factescalationcase' ,'vwpcse_factescalationcase'[EscalationTier]="Tier3"))
```
#### [# T1->IcM Tickets]
```dax
CALCULATE([# T3 Tickets], FILTER('vwpcse_factescalationcase', 'vwpcse_factescalationcase'[IsDirectICM] ="Yes"))
```
#### [# OtherIcM Tickets]
```dax
CALCULATE([# T3 Tickets], FILTER('vwpcse_factescalationcase', 'vwpcse_factescalationcase'[IsDirectICM] = "No" && 'vwpcse_factescalationcase'[EscalationType] = "IcM"))
```

*该类别还有6个度量值*
### Counting

#### [# Survey]
```dax
DISTINCTCOUNT('vwpcse_factcustomersurvey'[SurveyResponseId])
```
#### [# Escalation Tickets]
```dax
IF(DISTINCTCOUNT('vwpcse_factescalationcase'[EscalationId]) = BLANK(), 0, DISTINCTCOUNT('vwpcse_factescalationcase'[EscalationId]))
```
#### [# Escalation Cases]
```dax
IF(DISTINCTCOUNT('vwpcse_factescalationcase'[ServiceRequestKey]) = BLANK(), 0, DISTINCTCOUNT('vwpcse_factescalationcase'[ServiceRequestKey]))
```
#### [# Case Created]
```dax
DISTINCTCOUNT('vwpcse_factincident_created'[IncidentID])
```
#### [# Open Vol]
```dax
CALCULATE(DISTINCTCOUNT('vwpcse_factopencasedaily'[IncidentId]))
```
#### [# Open Case Daily]
```dax
CALCULATE(DISTINCTCOUNT('vwpcse_factopencasedaily'[IncidentId]))
```
#### [# CollabTask Created]
```dax
CALCULATE(DISTINCTCOUNT('vwpcse_facttask_created'[IncidentTaskId]),'vwpcse_facttask_created'[TaskType]="CollaborationTask")
```
#### [# Case Closed]
```dax
CALCULATE(DISTINCTCOUNT('vwpcse_factincident_closed'[IncidentID]),FILTER('vwpcse_factincident_closed','vwpcse_factincident_closed'[Case State] = "Closed"))
```
#### [CPT@P75]
```dax

VAR NoDisconnect = 
    FILTER(
        'vwpcse_factincident_closed',
        NOT 'vwpcse_factincident_closed'[Case Status] IN {"Duplicate", "Disconnect/Hang-up"}
    ) 
RETURN 
    CALCULATE(MAX('vwpcse_factincident_closed'[DTS])*24,TOPN(CEILING(CALCULATE(COUNT('vwpcse_factincident_closed'[DTS]),NoDisconnect)*0.75,1),NoDisconnect, 'vwpcse_factincident_closed'[DTS],ASC))
```
#### [# CollabTask Closed]
```dax
CALCULATE(DISTINCTCOUNT('vwpcse_facttask_closed'[IncidentTaskId]),'vwpcse_facttask_closed'[TaskType]="CollaborationTask")
```
### Calculation

#### [% DSAT(1,2)]
```dax
[# DSAT(1,2)] / [# CSAT Response]
```
#### [% DSAT(1,2,3)]
```dax
[# DSAT(1,2,3)] /[# CSAT Response]
```
### Aggregation

#### [% FCR]
```dax
SUM('vwpcse_factcustomersurvey'[IsFirstContactResolvedCount]) / SUM('vwpcse_factcustomersurvey'[FirstContactResolvedCount])
```
#### [% Resolution Rate]
```dax
SUM('vwpcse_factcustomersurvey'[IsResolvedCount]) / SUM('vwpcse_factcustomersurvey'[ResolvedCount])
```
#### [# IPD]
```dax

VAR var1 = CALCULATE(MAX('vwpcse_factincident_created'[CaseCreatedDateKey]),ALL('vwpcse_dimdate'))
RETURN IF(CALCULATE(SUM('vwpcse_dimdate'[IsWeekDay]),FILTER('vwpcse_dimdate','vwpcse_dimdate'[DateKey] <= var1)) = 0 || ISBLANK(CALCULATE(SUM('vwpcse_dimdate'[IsWeekDay]),FILTER('vwpcse_dimdate','vwpcse_dimdate'[DateKey] <= var1))) , BLANK(), CALCULATE('vwpcse_factincident_created'[# Case Created]/SUM('vwpcse_dimdate'[IsWeekDay]),FILTER('vwpcse_dimdate','vwpcse_dimdate'[DateKey] <= var1)))
```
#### [#  CPD]
```dax
IF(SUM('vwpcse_dimdate'[IsWeekDay]) = 0, 0, CALCULATE(DISTINCTCOUNT('vwpcse_factincident_closed'[IncidentID])/SUM('vwpcse_dimdate'[IsWeekDay])))
```
#### [# Avg Daily Open]
```dax
IF(SUM('vwpcse_dimdate'[IsWeekDay]) = 0, BLANK(), CALCULATE(COUNT('vwpcse_factopencasedaily'[IsOpenCase])/SUM('vwpcse_dimdate'[IsWeekDay]), FILTER('vwpcse_dimdate', 'vwpcse_dimdate'[IsWeekDay] = 1)))
```
#### [% Self-Help Deflection]
```dax
CALCULATE(SUM('vwpcse_factselfhelpdeflection'[TotalFlows]) - SUM('vwpcse_factselfhelpdeflection'[CreatedTicket]))/ SUM('vwpcse_factselfhelpdeflection'[TotalFlows])
```
#### [# TotalFlows]
```dax
SUM('vwpcse_factselfhelpdeflection'[TotalFlows])
```
#### [# CreatedTickets]
```dax
SUM('vwpcse_factselfhelpdeflection'[CreatedTicket])
```
#### [# Self-Help Deflection]
```dax
CALCULATE(SUM('vwpcse_factselfhelpdeflection'[TotalFlows]) - SUM('vwpcse_factselfhelpdeflection'[CreatedTicket]))
```
#### [Avg DTC]
```dax
CALCULATE(SUM('vwpcse_factincident_closed'[DTC])/DISTINCTCOUNT('vwpcse_factincident_closed'[IncidentID]),FILTER('vwpcse_factincident_closed','vwpcse_factincident_closed'[Case State] = "Closed"))
```

*该类别还有3个度量值*
### Statistical

#### [CPE CSAT]
```dax
ROUND(CALCULATE(AVERAGE('vwpcse_factcustomersurvey'[CsatScore])),2)
```
#### [Avg IRHours]
```dax
CALCULATE(AVERAGE('vwpcse_factincident_created'[IRHour]))
```
#### [IRHour@P75]
```dax
PERCENTILE.INC('vwpcse_factincident_created'[IRHour],0.75)
```
#### [Avg Escalation Age]
```dax
CALCULATE(AVERAGE('vwpcse_factescalationcase'[EscalationAge]))
```
#### [Avg Open Age]
```dax
CALCULATE(AVERAGE('vwpcse_factopencasedaily'[OpenAge]))
```
### Other

#### [# T1->IcM Tickets SAP]
```dax
IF([# T1->IcM Tickets]<>0,[# T1->IcM Tickets],BLANK())
```
#### [% Open Vol]
```dax

DIVIDE (
    [# Open Vol],
    CALCULATE ( [# Open Vol], ALL('vwpcse_factopencasedaily'[Category_Common])))
```

## 关系图

### 星型模式结构

**vwpcse_factcustomersurvey** (事实表)
  ├─→ vwpcse_dimdate (SentDateKey → DateKey)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimqueue (QueueKey → QueueKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimsap (SAPKey → SAPKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

**vwpcse_factescalationcase** (事实表)
  ├─→ vwpcse_dimdate (EscalationCreatedDateKey → DateKey)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimqueue (QueueKey → QueueKey)
  ├─→ vwpcse_dimsap (SAPKey → SAPKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

**vwpcse_factincident_created** (事实表)
  ├─→ vwpcse_dimdate (CaseCreatedDateKey → DateKey)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimqueue (QueueKey → QueueKey)
  ├─→ vwpcse_dimsap (SAPKey → SAPKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

**vwpcse_factopencasedaily** (事实表)
  ├─→ vwpcse_dimdate (DateKey → DateKey)

**vwpcse_factselfhelpdeflection** (事实表)
  ├─→ vwpcse_dimsap (SAPKey → SAPKey)
  ├─→ vwpcse_dimdate (DeflectionDateKey → DateKey)

**vwpcse_facttask_created** (事实表)
  ├─→ vwpcse_dimdate (TaskCreatedDateKey → DateKey)
  ├─→ vwpcse_dimsap (TaskSAPKey → SAPKey)
  ├─→ vwpcse_dimqueue (TaskQueueId → QueueID)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

**vwpcse_factincident_closed** (事实表)
  ├─→ vwpcse_dimdate (CaseClosedDateKey → DateKey)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimqueue (QueueKey → QueueKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimsap (SAPKey → SAPKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

**vwpcse_facttask_closed** (事实表)
  ├─→ vwpcse_dimdate (TaskClosedDateKey → DateKey)
  ├─→ vwpcse_dimqueue (TaskQueueId → QueueID)
  ├─→ vwpcse_dimsap (TaskSAPKey → SAPKey)
  ├─→ vwpcse_dimgeography (GeographyKey → GeographyKey)
  ├─→ vwpcse_dimlanguage (LanguageKey → LanguageKey)
  ├─→ vwpcse_dimpartner (PartnerKey → PartnerKey)
  ├─→ vwpcse_dimrootcause (RootCauseKey → RootCauseKey)
  ├─→ vwpcse_dimsite (SiteName → SiteName)

### 关系详情

| 源 | 目标 | 类型 | 筛选方向 |
|-----|------|------|----------|
| vwpcse_factcustomersurvey[SentDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[QueueKey] | vwpcse_dimqueue[QueueKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[SAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_factcustomersurvey[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |
| vwpcse_factescalationcase[EscalationCreatedDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[QueueKey] | vwpcse_dimqueue[QueueKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[SAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_factescalationcase[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |
| vwpcse_factincident_created[CaseCreatedDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[QueueKey] | vwpcse_dimqueue[QueueKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[SAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_factincident_created[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |
| vwpcse_factopencasedaily[DateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_factselfhelpdeflection[SAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_factselfhelpdeflection[DeflectionDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[TaskCreatedDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[TaskSAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[TaskQueueId] | vwpcse_dimqueue[QueueID] | 多对一 | OneDirection |
| vwpcse_facttask_created[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_facttask_created[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |
| vwpcse_factincident_closed[CaseClosedDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[QueueKey] | vwpcse_dimqueue[QueueKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[SAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_factincident_closed[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |
| vwpcse_facttask_closed[TaskClosedDateKey] | vwpcse_dimdate[DateKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[TaskQueueId] | vwpcse_dimqueue[QueueID] | 多对一 | OneDirection |
| vwpcse_facttask_closed[TaskSAPKey] | vwpcse_dimsap[SAPKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[GeographyKey] | vwpcse_dimgeography[GeographyKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[LanguageKey] | vwpcse_dimlanguage[LanguageKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[PartnerKey] | vwpcse_dimpartner[PartnerKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] | 多对一 | OneDirection |
| vwpcse_facttask_closed[SiteName] | vwpcse_dimsite[SiteName] | 多对一 | OneDirection |

## 关系完整性体检

| 源(外键) | 目标(主键) | 外键空值 | 孤儿键 |
|----------|------------|---------|-------|
| vwpcse_dimdate[CalendarDate] | LocalDateTable_c4840c99-4b9c-4a23-b799-18f0fe00fbda[Date] |  |  |
| vwpcse_dimdate[MonthStartDate] | LocalDateTable_ff96679c-f220-4a55-8f47-c1d04123f2fa[Date] |  |  |
| vwpcse_dimdate[WeekStartDate] | LocalDateTable_6ff6dba5-1c95-4a6d-a346-c3e88209045d[Date] |  |  |
| vwpcse_dimdate[WeekEndDate] | LocalDateTable_6507630d-728e-450a-9fc0-d129852cc6fc[Date] |  |  |
| vwpcse_dimdate[MonthEndDate] | LocalDateTable_7f39f39f-a6d0-4847-a679-1d5482445dc3[Date] |  |  |
| vwpcse_factcustomersurvey[SentDate] | LocalDateTable_ec37813c-1c40-4101-88ea-c9dfbcb47599[Date] |  |  |
| vwpcse_factcustomersurvey[SentDateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_factcustomersurvey[SubmittedTime] | LocalDateTable_f50421cd-4584-4a62-ad58-ce6817773dc4[Date] |  | 170439 |
| vwpcse_factcustomersurvey[SubmittedDate] | LocalDateTable_1d754d18-0689-4ef1-b7cb-f5993d4b4ed1[Date] |  |  |
| vwpcse_factcustomersurvey[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_factcustomersurvey[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_factcustomersurvey[QueueKey] | vwpcse_dimqueue[QueueKey] |  |  |
| vwpcse_factcustomersurvey[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_factcustomersurvey[SAPKey] | vwpcse_dimsap[SAPKey] |  |  |
| vwpcse_factcustomersurvey[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_factcustomersurvey[SiteName] | vwpcse_dimsite[SiteName] |  |  |
| vwpcse_factescalationcase[EscalationCreatedDateTime] | LocalDateTable_e2c9b746-d0f0-4b5e-964a-786bcb6bf152[Date] |  | 56262 |
| vwpcse_factescalationcase[EscalationResolvedDateTime] | LocalDateTable_c3d916ab-03db-4afe-b85d-88f4ed2e9b79[Date] | 8205 | 49854 |
| vwpcse_factescalationcase[EscalationCreatedDateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_factescalationcase[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_factescalationcase[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_factescalationcase[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_factescalationcase[QueueKey] | vwpcse_dimqueue[QueueKey] |  |  |
| vwpcse_factescalationcase[SAPKey] | vwpcse_dimsap[SAPKey] |  |  |
| vwpcse_factescalationcase[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_factescalationcase[SiteName] | vwpcse_dimsite[SiteName] |  | 1 |
| vwpcse_factincident_created[CaseCreatedDateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_factincident_created[Case Closed Date] | LocalDateTable_1cfab74e-0576-4a57-a5cd-556be637a584[Date] | 11695 |  |
| vwpcse_factincident_created[Case Closed Time] | LocalDateTable_9438ce73-9bb4-4eba-a880-2a8ed5c6e492[Date] | 11695 | 583065 |
| vwpcse_factincident_created[Case Created Date] | LocalDateTable_6507b33d-6f2d-4e91-b207-07b2e9879538[Date] |  |  |
| vwpcse_factincident_created[Case Created Time] | LocalDateTable_bc903c57-efba-4b62-a48e-0b35e8020718[Date] |  | 610710 |
| vwpcse_factincident_created[Case Resolved Time] | LocalDateTable_9c325ca3-fde9-471d-9b98-9b5540df774a[Date] | 7280 | 600928 |
| vwpcse_factincident_created[CaseClosedDateTimeLT] | LocalDateTable_3e629d6d-eafa-4c59-a663-913b7f1f269b[Date] | 11695 | 864872 |
| vwpcse_factincident_created[CaseCreatedDateTimeLT] | LocalDateTable_5acc2454-6292-4428-8d79-bc8b8ce48d93[Date] |  | 874621 |
| vwpcse_factincident_created[CaseClosedDateLT] | LocalDateTable_25098da8-cd68-4e45-8fa3-24b2410dfb02[Date] | 11695 |  |
| vwpcse_factincident_created[CaseCreatedDateLT] | LocalDateTable_4dae90d8-5b5d-4efd-ae70-71a659423333[Date] |  |  |
| vwpcse_factincident_created[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_factincident_created[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_factincident_created[QueueKey] | vwpcse_dimqueue[QueueKey] |  |  |
| vwpcse_factincident_created[SAPKey] | vwpcse_dimsap[SAPKey] |  |  |
| vwpcse_factincident_created[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_factincident_created[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_factincident_created[SiteName] | vwpcse_dimsite[SiteName] |  | 1 |
| vwpcse_factopencasedaily[CalendarDate] | LocalDateTable_d764f537-bea8-46cf-864b-e1036a41a194[Date] |  |  |
| vwpcse_factopencasedaily[ClosedDate] | LocalDateTable_07eef286-7326-45b1-b56e-793d028e2f0f[Date] | 677158 |  |
| vwpcse_factopencasedaily[CreatedDate] | LocalDateTable_62f503bb-e918-4ea9-8f0d-a29440b48d7c[Date] |  |  |
| vwpcse_factopencasedaily[DateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_factselfhelpdeflection[SAPKey] | vwpcse_dimsap[SAPKey] |  |  |
| vwpcse_factselfhelpdeflection[DeflectionDateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_facttask_created[TaskCreatedDateKey] | vwpcse_dimdate[DateKey] |  |  |
| vwpcse_facttask_created[TaskSAPKey] | vwpcse_dimsap[SAPKey] |  | 825 |
| vwpcse_facttask_created[TaskQueueId] | vwpcse_dimqueue[QueueID] |  | 2 |
| vwpcse_facttask_created[Task Closed Date] | LocalDateTable_532062b3-e63f-4966-911a-a6797dc87948[Date] |  |  |
| vwpcse_facttask_created[Task Created Date] | LocalDateTable_32a3bf22-dd84-41b6-81a3-6475606a141b[Date] |  |  |
| vwpcse_facttask_created[ModifiedDateTime] | LocalDateTable_f315289b-0abc-4613-a832-6d603b01ef4f[Date] |  |  |
| vwpcse_facttask_created[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_facttask_created[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_facttask_created[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_facttask_created[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_facttask_created[SiteName] | vwpcse_dimsite[SiteName] |  | 8 |
| vwpcse_factincident_closed[CaseClosedDateKey] | vwpcse_dimdate[DateKey] |  | 1 |
| vwpcse_factincident_closed[Case Closed Date] | LocalDateTable_cf3fabae-7194-474f-857c-2f8733d41ff0[Date] | 11695 |  |
| vwpcse_factincident_closed[Case Closed Time] | LocalDateTable_eff2363d-0926-468a-9d5b-ed3a65005c96[Date] | 11695 | 583065 |
| vwpcse_factincident_closed[Case Created Date] | LocalDateTable_4fb37ca5-b10d-41af-91b6-4766377fafbb[Date] |  |  |
| vwpcse_factincident_closed[Case Created Time] | LocalDateTable_c6066640-117c-4321-b917-25e99c6bda03[Date] |  | 610710 |
| vwpcse_factincident_closed[Case Resolved Time] | LocalDateTable_b7182c39-0b20-4d9c-ae29-328095ada84d[Date] | 7280 | 600928 |
| vwpcse_factincident_closed[CaseClosedDateTimeLT] | LocalDateTable_868a0aa2-2748-47b3-8346-4d0f7854b2aa[Date] | 11695 | 864872 |
| vwpcse_factincident_closed[CaseCreatedDateTimeLT] | LocalDateTable_79d8cf50-ef37-4cf6-83b6-ae20d03fad48[Date] |  | 874621 |
| vwpcse_factincident_closed[CaseClosedDateLT] | LocalDateTable_a5b5e410-f304-4d87-bf09-065799242625[Date] | 11695 |  |
| vwpcse_factincident_closed[CaseCreatedDateLT] | LocalDateTable_15334c62-bf89-4dbb-8cae-ce3cac193aef[Date] |  |  |
| vwpcse_factincident_closed[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_factincident_closed[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_factincident_closed[QueueKey] | vwpcse_dimqueue[QueueKey] |  |  |
| vwpcse_factincident_closed[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_factincident_closed[SAPKey] | vwpcse_dimsap[SAPKey] |  |  |
| vwpcse_factincident_closed[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_factincident_closed[SiteName] | vwpcse_dimsite[SiteName] |  | 1 |
| vwpcse_facttask_closed[Task Closed Date] | LocalDateTable_d9d11826-be10-4d4e-a5ff-4d40fbdb48f9[Date] |  |  |
| vwpcse_facttask_closed[Task Created Date] | LocalDateTable_aaca5604-5d57-4cca-8564-a289bb15db42[Date] |  |  |
| vwpcse_facttask_closed[ModifiedDateTime] | LocalDateTable_952560ec-311a-49a6-b965-8071dac052d2[Date] |  |  |
| vwpcse_facttask_closed[TaskClosedDateKey] | vwpcse_dimdate[DateKey] |  | 1 |
| vwpcse_facttask_closed[TaskQueueId] | vwpcse_dimqueue[QueueID] |  | 2 |
| vwpcse_facttask_closed[TaskSAPKey] | vwpcse_dimsap[SAPKey] |  | 825 |
| vwpcse_facttask_closed[GeographyKey] | vwpcse_dimgeography[GeographyKey] |  |  |
| vwpcse_facttask_closed[LanguageKey] | vwpcse_dimlanguage[LanguageKey] |  |  |
| vwpcse_facttask_closed[PartnerKey] | vwpcse_dimpartner[PartnerKey] |  | 1 |
| vwpcse_facttask_closed[RootCauseKey] | vwpcse_dimrootcause[RootCauseKey] |  |  |
| vwpcse_facttask_closed[SiteName] | vwpcse_dimsite[SiteName] |  | 8 |

**模型提示**
- Queue 维度存在 QueueKey 与 QueueID 并行连接；建议统一代理键或加桥表。

## DAX查询示例

### 基础查询

#### 获取单个度量值
*查询一个度量值的总值*

```dax
EVALUATE
ROW("结果", [# DSAT(1,2)])
```

#### 查看事实表vwpcse_factcustomersurvey前10行
*获取事实表的前10行数据*

```dax
EVALUATE
TOPN(10, 'vwpcse_factcustomersurvey')
```

### 时间序列

#### 队列的Median CSAT（数据锚点：最近可用日期，窗口90天）
*当 Submitted/Sent 与日期维度无活动关系时使用 TREATAS*

```dax
EVALUATE
VAR AnchorDate = CALCULATE(MAX('vwpcse_factcustomersurvey'[SubmittedDate]))
VAR Period = DATESINPERIOD('vwpcse_dimdate'[CalendarDate], AnchorDate, -90, DAY)
VAR Dates = CALCULATETABLE(VALUES('vwpcse_dimdate'[CalendarDate]), Period)
RETURN
TOPN(
  20,
  ADDCOLUMNS(
    VALUES('vwpcse_dimqueue'['QueueID']),
    "Responses",  CALCULATE([# CSAT Response], TREATAS(Dates, 'vwpcse_factcustomersurvey'[SubmittedDate])),
    "Median CSAT",
      CALCULATE(
        MEDIANX(
          FILTER('vwpcse_factcustomersurvey', NOT ISBLANK('vwpcse_factcustomersurvey'[CsatScore])),
          'vwpcse_factcustomersurvey'[CsatScore]
        ),
        TREATAS(Dates, 'vwpcse_factcustomersurvey'[SubmittedDate])
      )
  ),
  [Responses], DESC
)
```

### 排名分析

#### 按国家看关闭量 Top 10（以关闭日期为锚点，90天）
*使用活动关系的简单窗口（无需 TREATAS）*

```dax
EVALUATE
VAR AnchorDate = CALCULATE(MAX('vwpcse_factincident_closed'['Case Closed Date']))
VAR Period = DATESINPERIOD('vwpcse_dimdate'[CalendarDate], AnchorDate, -90, DAY)
RETURN
TOPN(
  10,
  SUMMARIZECOLUMNS(
    'vwpcse_dimgeography'['Country'],
    Period,
    "# Closed", [# Case Closed]
  ),
  [# Closed], DESC
)
```

### 筛选查询

#### 条件筛选（CALCULATE）
*对事实表文本列做条件筛选*

```dax
EVALUATE
ROW(
    "筛选结果",
    CALCULATE(
        [# DSAT(1,2)],
        'vwpcse_factcustomersurvey'['SurveyResponseId'] = "示例值"
    )
)
```

## 使用指南

### 快速开始
- 1. 连接到Power BI语义模型
- 2. 使用表名和列名时注意大小写
- 3. 度量值使用方括号引用: [度量值名称]
- 4. 表和列使用单引号: '表名'[列名]

### 常见模式
- 主要分析基于事实表: vwpcse_factcustomersurvey, vwpcse_factescalationcase, vwpcse_factincident_created, vwpcse_factopencasedaily, vwpcse_factselfhelpdeflection, vwpcse_facttask_created, vwpcse_factincident_closed, vwpcse_facttask_closed
- 使用日期维度进行时间序列分析

### 最佳实践
- 优先使用已定义的度量值而不是重新计算
- 利用关系进行跨表查询，避免手动JOIN
- 使用CALCULATE进行上下文转换
- 对大数据集使用TOPN限制结果集
- 示例中的时间窗口默认使用数据锚点（最近可用日期），可改为上月/上季等固定窗口

### 故障排除
- 错误: 找不到列 → 检查列名大小写和拼写
- 错误: 循环依赖 → 检查关系设置
- 性能问题 → 考虑使用聚合表或优化度量值
- 窗口内无数据 → 使用数据锚点或放宽时间窗，并检查关系是否为活动/需要TREATAS

## 附录

### 可用的日期维度

| 表 | 列 | 时间智能 |
|-----|-----|----------|
| vwpcse_dimdate | CalendarDate | ❌ 未通过主日期维度 |
| vwpcse_dimdate | MonthStartDate | ❌ 未通过主日期维度 |
| vwpcse_dimdate | WeekStartDate | ❌ 未通过主日期维度 |
| vwpcse_dimdate | WeekEndDate | ❌ 未通过主日期维度 |
| vwpcse_dimdate | MonthEndDate | ❌ 未通过主日期维度 |
| vwpcse_factcustomersurvey | SentDate | ❌ 未通过主日期维度 |
| vwpcse_factcustomersurvey | SubmittedTime | ❌ 未通过主日期维度 |
| vwpcse_factcustomersurvey | SubmittedDate | ❌ 未通过主日期维度 |
| vwpcse_factescalationcase | EscalationCreatedDateTime | ❌ 未通过主日期维度 |
| vwpcse_factescalationcase | EscalationResolvedDateTime | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | Case Closed Date | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | Case Closed Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | Case Created Date | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | Case Created Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | Case Resolved Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | CaseClosedDateTimeLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | CaseCreatedDateTimeLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | CaseClosedDateLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_created | CaseCreatedDateLT | ❌ 未通过主日期维度 |
| vwpcse_factopencasedaily | CalendarDate | ❌ 未通过主日期维度 |
| vwpcse_factopencasedaily | ClosedDate | ❌ 未通过主日期维度 |
| vwpcse_factopencasedaily | CreatedDate | ❌ 未通过主日期维度 |
| vwpcse_facttask_created | Task Closed Date | ❌ 未通过主日期维度 |
| vwpcse_facttask_created | Task Created Date | ❌ 未通过主日期维度 |
| vwpcse_facttask_created | ModifiedDateTime | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | Case Closed Date | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | Case Closed Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | Case Created Date | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | Case Created Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | Case Resolved Time | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | CaseClosedDateTimeLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | CaseCreatedDateTimeLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | CaseClosedDateLT | ❌ 未通过主日期维度 |
| vwpcse_factincident_closed | CaseCreatedDateLT | ❌ 未通过主日期维度 |
| vwpcse_facttask_closed | Task Closed Date | ❌ 未通过主日期维度 |
| vwpcse_facttask_closed | Task Created Date | ❌ 未通过主日期维度 |
| vwpcse_facttask_closed | ModifiedDateTime | ❌ 未通过主日期维度 |

### 自动生成的日期表
Power BI为以下日期列自动创建了时间智能表：

- `DateTableTemplate_13d38c1f-5c9c-4353-9489-a9d7b7d632ad` (hidden)
- `LocalDateTable_c4840c99-4b9c-4a23-b799-18f0fe00fbda` (hidden)
- `LocalDateTable_ff96679c-f220-4a55-8f47-c1d04123f2fa` (hidden)
- `LocalDateTable_6ff6dba5-1c95-4a6d-a346-c3e88209045d` (hidden)
- `LocalDateTable_6507630d-728e-450a-9fc0-d129852cc6fc` (hidden)
- `LocalDateTable_ec37813c-1c40-4101-88ea-c9dfbcb47599` (hidden)
- `LocalDateTable_e2c9b746-d0f0-4b5e-964a-786bcb6bf152` (hidden)
- `LocalDateTable_c3d916ab-03db-4afe-b85d-88f4ed2e9b79` (hidden)
- `LocalDateTable_d764f537-bea8-46cf-864b-e1036a41a194` (hidden)
- `LocalDateTable_07eef286-7326-45b1-b56e-793d028e2f0f` (hidden)
- ...共38个

### 取数提示
- hierarchies not available (INFO.VIEW & TMSCHEMA failed)
- roles not available (INFO.VIEW & TMSCHEMA failed)
