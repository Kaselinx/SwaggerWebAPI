

USE [TCPS2]
GO
/****** Object:  Table [dbo].[PXTSO]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXTSO](
	[PXTSOId] [int] IDENTITY(1,1) NOT NULL,
	[BatchUniqKey] [varchar](64) NOT NULL,
	[Age] [int] NOT NULL,
	[Male] [decimal](10, 7) NOT NULL,
	[Female] [decimal](10, 7) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXTSO] PRIMARY KEY CLUSTERED 
(
	[PXTSOId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwInsuranceCostRate]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[vwInsuranceCostRate]
AS
SELECT  BatchUniqKey, Age, Male, Female,
		ROUND((Male *  10000  * 1)  / 12,2) AS 'MaleCost'
		, ROUND((Female *  10000  * 1)  / 12,2) AS 'FemaleCost'
FROM     dbo.PXTSO
GO
/****** Object:  Table [dbo].[PXATSO]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXATSO](
	[PXATSOId] [int] IDENTITY(1,1) NOT NULL,
	[YearKey] [int] NOT NULL,
	[Age] [int] NOT NULL,
	[Male] [decimal](10, 7) NOT NULL,
	[Female] [decimal](10, 7) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXaTSO] PRIMARY KEY CLUSTERED 
(
	[PXATSOId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vwAnnuityCostRate]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Script for SelectTopNRows command from SSMS  ******/
CREATE VIEW [dbo].[vwAnnuityCostRate]
AS
SELECT YearKey, Age, Male, Female, ROUND(Male * 10000 * 1 / 12, 2) AS 'MaleCost', ROUND(Female * 10000 * 1 / 12, 2) AS 'FemaleCost'
FROM     dbo.PXATSO
GO
/****** Object:  Table [dbo].[PXCorridor]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXCorridor](
	[PXCorridorId] [int] IDENTITY(1,1) NOT NULL,
	[Age] [int] NOT NULL,
	[CorridorRule] [decimal](10, 7) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXCorridor] PRIMARY KEY CLUSTERED 
(
	[PXCorridorId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXLoading]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXLoading](
	[PXLoadingId] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[BandStage] [int] NOT NULL,
	[Limits] [int] NOT NULL,
	[BandValue] [decimal](10, 7) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXLoading] PRIMARY KEY CLUSTERED 
(
	[PXLoadingId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXMaintfee]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXMaintfee](
	[PXMaintfeeId] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[MaintfeeYear] [int] NOT NULL,
	[Maintfee] [int] NOT NULL,
	[Maintfee_discount] [int] NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXMaintfee1] PRIMARY KEY CLUSTERED 
(
	[PXMaintfeeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXMgfee]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXMgfee](
	[PXMgfeeId] [int] NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[MgfeeYear] [int] NOT NULL,
	[MgfeePc] [decimal](5, 4) NOT NULL,
 CONSTRAINT [PK_PXMgfee] PRIMARY KEY CLUSTERED 
(
	[PXMgfeeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXRoyalty]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXRoyalty](
	[PXRoyalty_Id] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [nvarchar](16) NOT NULL,
	[RoyaltyYear] [int] NOT NULL,
	[RoyaltyPc] [decimal](5, 4) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXRoyalty] PRIMARY KEY CLUSTERED 
(
	[PXRoyalty_Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXSubscriptFee]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXSubscriptFee](
	[SubscripFeeId] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[InvestTargets] [int] NOT NULL,
	[Description] [varchar](128) NULL,
	[SubscriptPc] [decimal](7, 4) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXSubscriptFee] PRIMARY KEY CLUSTERED 
(
	[SubscripFeeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXSurrender]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXSurrender](
	[PXSurrenderId] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[SurrenderYear] [int] NOT NULL,
	[SurrenderPc] [decimal](5, 4) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXSurrender] PRIMARY KEY CLUSTERED 
(
	[PXSurrenderId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PXTSOProd_Mapper]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PXTSOProd_Mapper](
	[PXTSOProd_MapperId] [int] IDENTITY(1,1) NOT NULL,
	[PlanCode] [varchar](16) NOT NULL,
	[BatchUniqKey] [varchar](64) NOT NULL,
	[LastModifyTime] [datetime] NOT NULL,
	[LastModifyUser] [varchar](16) NULL,
 CONSTRAINT [PK_PXTSOProd_Mapper] PRIMARY KEY CLUSTERED 
(
	[PXTSOProd_MapperId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PXATSO] ADD  CONSTRAINT [DF_PXATSO_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXCorridor] ADD  CONSTRAINT [DF_PXCorridor_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXLoading] ADD  CONSTRAINT [DF_PXLoading_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXMaintfee] ADD  CONSTRAINT [DF_PXMaintfee1_Maintfee_discount]  DEFAULT ((0)) FOR [Maintfee_discount]
GO
ALTER TABLE [dbo].[PXMaintfee] ADD  CONSTRAINT [DF_PXMaintfee1_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXRoyalty] ADD  CONSTRAINT [DF_PXRoyalty_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXSubscriptFee] ADD  CONSTRAINT [DF_PXSubscriptFee_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXSurrender] ADD  CONSTRAINT [DF_PXSurrender_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXTSO] ADD  CONSTRAINT [DF_PXTSO_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
ALTER TABLE [dbo].[PXTSOProd_Mapper] ADD  CONSTRAINT [DF_XTSOProd_Mapper_LastModifyTime]  DEFAULT (getdate()) FOR [LastModifyTime]
GO
/****** Object:  StoredProcedure [dbo].[QueryCal_cashflow_FGR]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
---- the definition of the procedure.
---- ================================================
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
-- =============================================
-- Author:		Ron, Lin
-- Create date: 2021-12-16
-- Description:	Cal cashflow by 投資報酬率
-- =============================================
CREATE PROCEDURE [dbo].[QueryCal_cashflow_FGR]
--DECLARE 
	@InvestmentReturnRate decimal(5,2), --between 6% to -6%
	@InvestmentAmt decimal(17,4), --保費
	@payOption int,  --彈性繳(1)/躉繳(2)
	@annuityYearCount int,  --年金累積期間
	@proposalType int, --Type 5 = 年金a
	@UserAge	int,	--User age
	@PlanCode		varchar(8), -- plan or PlanCode eg:FVA01N
	@Gender		int, --性別 Male 1, Female 2
	@IncreaseRate   decimal(8,4), --加費比例
	@InventRateONReturn decimal(8,4), --投資報酬率
	@SubSType int, --甲型1, 乙型,
	@Unit_Coverage int, --單位保額
	@InvestTargets int -- 投資標的


AS
BEGIN

	SET NOCOUNT ON;

	--=======================================Create the List of Year-Month Data=============================================================

	 -- Declare months need to be create 
	DECLARE @months     INT, 
        @firstmonth DATE;

	SET @months = (110 - @UserAge) * 12 


	--TEST Setter
	--SET @UserAge = 45
	--SET @Unit_Coverage = 10000
	--SET @InvestmentAmt = 300000
	--SET @PlanCode = 'TVLA1N'
	--SET @SubSType = 1
	--SET @UserAge = 45
	--SET @InvestTargets = 3
	--SET  @Gender = 2
	--SET @IncreaseRate = 0.5
	--SET @InventRateONReturn = 0.06

	SET @firstmonth =  CAST (CAST(YEAR(GETDATE()) AS VARCHAR)  + '-01' + '-01' as datetime)
	

	;WITH x(y, m, s, e) AS 
	(
	  SELECT YEAR(dt), MONTH(dt), dt, DATEADD(MONTH, 1, dt) FROM 
	  ( SELECT dt = DATEADD(MONTH, rn-1, @firstmonth) FROM 
		( SELECT TOP (@months) rn = ROW_NUMBER() OVER (ORDER BY [object_id])
		  FROM sys.all_columns ORDER BY rn
		) AS z
	  ) AS y
	)
	SELECT y - YEAR(GETDATE())  + 1  as 'InsurYearCount' -- count start with 1
			, y as 'cYear'
			, m as 'cMonth'  
			, s as 'FirstDayOfMonth'
			, DATEADD(Day, -1,  e) as 'EndDateOfMonth'
			INTO #TEMP_MONTHDATE
	 FROM x 



	--================================================Create main Output Table==============================================----------
	--保單年度	月份	保險年齡		保險費				保費費用				保險成本						保單管理費			每月扣除額		
	--申購費用					保單價值加值金				保單帳戶價值							解約費用		解約金		保險金額
	--第一期保險費	所繳保險費	累積所繳保險費		第一期保險費	比率	合計		基本保額	Corridor	淨危險保額	標準體費率	次標體費加費點數
	--保險成本	保單維護費用	帳戶管理費用	比例			第一期保險費	保單價值加值金	比例	合計		平均保價	比例	金額		扣除保費費用	再扣除每月扣除額	
	--加上保單價值加值金	再扣除申購費用	投資報酬率	反映投報率後		比率	金額			

	CREATE TABLE #CashFlowOutput
	(
	  InsurYearCount int, --保單年度
	  cMonth		 int, --月份
	  cAge			 int, --保險年齡
	  --保險費 
	  InitialInsurFee		 decimal(16,4), --第一期保險費/"躉繳保險費"  pay_option="躉繳","躉繳保險費",IF(pay_option="彈性繳","第一期保險費"
	  PaidInsurFee			 decimal(16,4), --所繳保險費
	  SumPaidInsurFee		 decimal(16,4), --累積所繳保險費	
	  --保費費用		
	  InitialFee			 decimal(16,4), --第一期保費費用(手續費?)
	  LoadingPercent		 decimal(6,4) , --費率百分比
	  SumFee				 decimal(16,4), --管理費合計
	  --保險成本
	  BasicAssured			 decimal(16,4), --基本保額
	  Corridor				 decimal(6,4), -- Corridor rules ,IF(type="年金",0%)
	  RiskＣoverage			 decimal(16,4), -- 淨危險保額
	  StandRate				 decimal(16,4), -- 標準體費率
	  SubStandIncreaseRate	 decimal(16,4), --次標準體加費比例
	  InsurCost				 decimal(16,4), --保險成本
	  --保單管理費
	  InsurMaintainFee		 decimal(16, 4), --保單維護費用
	  AccountMgrFee			 decimal(16, 4), --帳戶管理費用
	  InsurMgrFeePc			 decimal(6, 4),  --保單管理費比例
	  MonthlyTotalFee		 decimal(16,4),  --每月扣除額
	  --申購費用
	  InitialSubscriptionFee decimal(16, 4), --申購第一期保險費
	  SubscriptionAddons	 decimal(16, 4),--申購保單價值加值金
	  SubscriptionFeeRate	 decimal(6, 4), -- 申購費率
	  SubscriptionFeeSum	 decimal(16, 4), --申購費合計
	  --保單價值加值金
	  AvgInsurCost			 decimal(16, 4), -- 平均保價
	  AddOnCostRate			 decimal(6,4), -- 保單價值加值金比例
	  AddOnCost				 decimal(16, 4), -- 保單價值加值金
	  --保單帳戶價值
	  WorthAfterSumFee		 decimal(16, 4), --扣除保費費用
	  WorthAferMonthlyDeduction	decimal(16, 4), --再扣除每月扣除額
	  WorthAfterReserve		 decimal(16, 4), 	--加上保單價值加值金
	  WorthAfterSubscriptionFee decimal(16, 4), --再扣除申購費用
	  ReturnRateOnInvestment decimal(6,4), -- 投資報酬率
	  ReturnOnInvestment decimal(16,4), --反映投報率後
	  --解約費用
	  SurrenderRate		 decimal(6,4), --解約費用比例
	  SurrenderCharge	 decimal(16,4), --解約費用金額
	  SurrenderReturn	 decimal(16,4),
	  --保險金額
	  InsurenceAmt		 decimal(16,4) --=IF(OR(B36="",type="年金"),"",IF(AM36<=0,0,IF(type="甲型",MAX(N36,AM36),N36+AM36)))
	)


	INSERT INTO #CashFlowOutput(InsurYearCount, cMonth, cAge)
	SELECT InsurYearCount, cMonth ,  (@UserAge + InsurYearCount - 1 ) as 'cAge'
	FROM #TEMP_MONTHDATE


	--===============================update第一次保險費(彈性繳)/躉繳保險費("躉繳")===============================
	UPDATE #CashFlowOutput SET InitialInsurFee = @InvestmentAmt 
			WHERE InsurYearCount = 1 AND cMonth = 1
	UPDATE #CashFlowOutput SET InitialInsurFee  = 0 where InitialInsurFee IS NULL


	--========================== update 所繳保險費　因為彈性繳的性質是同於躉繳　所以只取第一筆============================================================--
	UPDATE #CashFlowOutput SET PaidInsurFee = InitialInsurFee
	--========================== update 累積所繳保險費　因為彈性繳的性質是同於躉繳　所以只取第一筆============================================================--
	UPDATE #CashFlowOutput SET SumPaidInsurFee =  (SELECT  MAX(InitialInsurFee) FROM #CashFlowOutput)
	
	--==================================update 保費費用 =============================================================================---
	DECLARE @Loading decimal(8,6)
	SET @Loading =								(SELECT  TOP 1 BandValue 
												  FROM  PXLoading 
												  WHERE PlanCode = @PlanCode AND  30000 <= Limits
												  Order by Limits ASC)
	UPDATE #CashFlowOutput SET LoadingPercent =  @Loading  --update 保費費用比率 
	UPDATE #CashFlowOutput SET InitialFee = InitialInsurFee * @Loading　--update 保費費用第一期保險費
	UPDATE #CashFlowOutput SET SumFee =   InitialFee　--update 保費費用合計
	--=================================update 保險成本===============================================================================---				
	
	--DECLARE @SubSType int --甲型1, 乙型2
	--IF(type="甲型",ROUNDUP(C9*C10,0),IF(type="乙型",ROUNDUP(C9*(C10-1),0),0))
	UPDATE #CashFlowOutput
	SET #CashFlowOutput.Corridor = B.CorridorRule,　-- update 保險成本 Corridor
		#CashFlowOutput.BasicAssured =  CASE WHEN @SubSType = 1 AND @proposalType != 5 THEN  B.CorridorRule * A.SumPaidInsurFee       --update'甲型' 基本保額
											 WHEN @SubSType = 2 AND @proposalType != 5 THEN  ROUND(A.SumPaidInsurFee * (B.CorridorRule - 1), 0)	 --update'乙型' 基本保額
			                            ELSE 0 END  --年金 :todo 先給0 後面在參考年金規則
	FROM #CashFlowOutput A INNER JOIN PXCorridor B ON A.cAge = B.Age


	--===========================================update 標準體費率============================================-------
	UPDATE #CashFlowOutput
	SET #CashFlowOutput.StandRate =  CASE WHEN @Gender = '1' THEN B.MaleCost ELSE B.FemaleCost END 
	FROM #CashFlowOutput A INNER JOIN 
			(
			SELECT  B.*
			FROM [dbo].[PXTSOProd_Mapper] A INNER JOIN [dbo].[vwInsuranceCostRate] B ON A.BatchUniqKey = B.BatchUniqKey
			AND A.[PlanCode] = @PlanCode
			) B ON A.cAge = B.Age 

	--DECLARE @IncreaseRate   decimal(8,4)

	UPDATE #CashFlowOutput SET #CashFlowOutput.SubStandIncreaseRate = @IncreaseRate --次標體費加費點數



	--DECLARE @InventRateONReturn decimal(6, 4)
	---===============================update 投資報酬率 ReturnRateOnInvestment ==============================================
	UPDATE #CashFlowOutput SET ReturnRateOnInvestment = @InventRateONReturn  --update 投資報酬率

	--================================update 保單維護費用比例 HLOOKUP(B10,Mgfee,2,FALSE)======================================
	UPDATE #CashFlowOutput SET  InsurMgrFeePc	= B.Maintfee   --保單維護費
	FROM #CashFlowOutput A INNER JOIN PXMaintfee B ON A.InsurYearCount = B.MaintfeeYear
	WHERE B.PlanCode = @PlanCode AND B.MaintfeeYear = A.InsurYearCount

	--===============================update保單價值加值金比例AddOnCostRate=======================================================
	UPDATE #CashFlowOutput SET  AddOnCostRate	= B.RoyaltyPc   --保單維護費用比
	FROM #CashFlowOutput A INNER JOIN PXRoyalty B ON A.InsurYearCount = B.RoyaltyYear
	WHERE B.PlanCode = @PlanCode AND B.RoyaltyYear = A.InsurYearCount


	--===============================update 解約費用 SurrenderRate=======================================================================
	
	UPDATE #CashFlowOutput SET SurrenderRate = B.SurrenderPc
	FROM #CashFlowOutput A LEFT JOIN PXSurrender B ON B.SurrenderYear = A.InsurYearCount
	WHERE B.PlanCode = @PlanCode AND B.SurrenderYear = A.InsurYearCount


	--========================================update 申購費用比例====================================
	UPDATE #CashFlowOutput SET SubscriptionFeeRate =  (SELECT SubscriptPc FROM PXSubscriptFee WHERE PlanCode = @PlanCode AND InvestTargets = @InvestTargets)

	--===============================update 保單帳戶價值	 ===================================================================
	DECLARE	  @cInitialInsurFee		decimal(16,4) 
	DECLARE	  @kMonth int			--monthy key
	DECLARE   @kInsurYearCount int  --year key


	DECLARE	  @cWorthAfterSumFee	decimal(16, 4)	  --Current扣除保費費用
	DECLARE	  @cWorthAferMonthlyDeduction	decimal(16, 4) --Current再扣除每月扣除額
	DECLARE	  @cWorthAfterReserve		 decimal(16, 4) 	--Current加上保單價值加值金
	DECLARE	  @cWorthAfterSubscriptionFee decimal(16, 4) --Current再扣除申購費用
	DECLARE	  @cReturnRateOnInvestment decimal(16,4) --Current 投資報酬率
	--most important key 
	DECLARE	  @cReturnOnInvestment decimal(16,4) --Current反映投報率後
	DECLARE   @cInvestmentAmt decimal(16,4) 
	DECLARE   @cSumFee		 decimal(16,4)
	SET @cReturnOnInvestment = 0

	--Declcare Cursor to update 扣除保費費用/再扣除每月扣除額/加上保單價值加值金

	DECLARE cashFlow_cursor CURSOR FOR
		  SELECT InsurYearCount, cMonth, SumFee --,  WorthAferMonthlyDeduction , WorthAfterReserve,WorthAfterSubscriptionFee
		  FROM  #CashFlowOutput

	-- Open the Cursor
		OPEN cashFlow_cursor
		-- 3 - Fetch the next record from the cursor
		FETCH NEXT FROM cashFlow_cursor INTO @kInsurYearCount, @kMonth, @cSumFee --, @cWorthAferMonthlyDeduction, @cWorthAfterReserve,@cWorthAfterSubscriptionFee
		-- Set the status for the cursor
		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			
			--UPDATE AH 扣除保費費用 rule:上一期AM9反映投報率後+G10所繳保險費-L10保費費用合計
			SET @cInvestmentAmt = (SELECT  InitialInsurFee FROM #CashFlowOutput A WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth	)	
			SET @cWorthAfterSumFee =  @cInvestmentAmt + @cReturnOnInvestment -  @cSumFee  --=上一期AM9反映投報率後+G10所繳保險費-L10保費費用合計

			UPDATE #CashFlowOutput SET WorthAfterSumFee = @cWorthAfterSumFee where InsurYearCount = @kInsurYearCount and cMonth = @kMonth
			
			--PRINT CAST( @cWorthAfterSumFee as VARCHAR )　+ ':WorthAfterSumFee'
			--PRINT CAST( @cReturnOnInvestment as VARCHAR) + ':ReturnOnInvestment'
			--PRINT CAST( @cInvestmentAmt as VARCHAR) + ':cInvestmentAmt'
			--PRINT CAST( @cSumFee as VARCHAR) + ':cSumFee'


			--update  淨危險保額 rule: IF(type="甲型",MAX(N10-AH10,0),N10)
			IF (@SubSType = 1 AND @proposalType != 5  ) --甲型1,
			BEGIN
			--PRINT　@cWorthAfterSumFee 
			--PRINT 128535
	
			UPDATE #CashFlowOutput SET RiskＣoverage  = CASE WHEN (BasicAssured  - @cWorthAfterSumFee >=0) 
															THEN BasicAssured  - @cWorthAfterSumFee  ELSE 0 END
										WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth
		    --SELECT  BasicAssured ,@cWorthAfterSumFee, (BasicAssured  - @cWorthAfterSumFee ) AS 'RiskＣoverage' FROM  #CashFlowOutput WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth

			END
			Else If( @SubSType = 2 AND @proposalType != 5 ) --乙型2 IF(type="甲型",MAX(N10-AH10,0),N10) -- N11
			BEGIN
				UPDATE #CashFlowOutput SET RiskＣoverage  =  CASE WHEN (BasicAssured  - @cWorthAfterSumFee >=0) 
															THEN BasicAssured  - @cWorthAfterSumFee  ELSE 0 END
										WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth
			END
			ELSE --年金 IF(type="年金",0)
				BEGIN 
					UPDATE #CashFlowOutput SET RiskＣoverage  = 0  
										WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth
			END 
			


			--update 保險成本 rule:IF(type="年金" 0)  ELSE  MIN(ROUND(P10淨危險保額/Unit_Coverage*Q10標準體費率*(1+R10次標體費加費點數),Decimal),AH10扣除保費費用)
			UPDATE #CashFlowOutput SET InsurCost = 
											CASE WHEN ROUND(RiskＣoverage/@Unit_Coverage * StandRate * ( 1 +  @IncreaseRate ),0) <= @cWorthAfterSumFee 
															THEN ROUND(RiskＣoverage/@Unit_Coverage * StandRate * ( 1 +  @IncreaseRate ),0)
												 ELSE  @cWorthAfterSumFee END
			FROM #CashFlowOutput
			WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth

			--=====update 帳戶管理費用 rule:IF((扣除保費費用-保險成本)>ROUND(扣除保費費用*保單管理費比例,Decimal),ROUND(扣除保費費用*保單管理費比例,Decimal),扣除保費費用-保險成本))
			--SELECT @cWorthAfterSumFee AS  '扣除保費費用', InsurCost AS '保險成本' , InsurMgrFeePc AS '保單管理費比例' 
			--FROM #CashFlowOutput A
			--WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth
			UPDATE #CashFlowOutput SET AccountMgrFee = CASE WHEN (@cWorthAfterSumFee - A.InsurCost) > ROUND(@cWorthAfterSumFee * InsurMgrFeePc, 4) 
															THEN ROUND(@cWorthAfterSumFee * A.InsurMgrFeePc, 2)  
															ELSE @cWorthAfterSumFee - A.InsurCost END 
																	
			FROM #CashFlowOutput A
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth		


			--=======update 保單維護費用 IF(H10>Maintfee_discount,0,IF((AH10-S10-U10)>HLOOKUP(B10,Maintfee,2),HLOOKUP(B10,Maintfee,2),AH10-S10-U10))

			DECLARE @Maintfee_discount int
			DECLARE @Maintfee int 
			SET @Maintfee_discount = (SELECT Maintfee_discount FROM [dbo].[PXMaintfee] WHERE MaintfeeYear = @kInsurYearCount AND PlanCode = @PlanCode)
			SET @Maintfee = (SELECT Maintfee FROM [dbo].[PXMaintfee] WHERE MaintfeeYear = @kInsurYearCount AND PlanCode = @PlanCode)

			--SELECT SumPaidInsurFee , @Maintfee_discount , @cWorthAfterSumFee , InsurCost
			--FROM #CashFlowOutput A
			--WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth

			UPDATE #CashFlowOutput SET InsurMaintainFee = CASE WHEN SumPaidInsurFee > @Maintfee_discount THEN 0
														       WHEN (@cWorthAfterSumFee  -  @Maintfee - A.InsurCost )>  @Maintfee THEN @Maintfee 
														  ELSE  @cWorthAfterSumFee - A.InsurCost -  @Maintfee END
						
			FROM #CashFlowOutput A
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth


			--====================================update W每月扣除額 S保險成本+T保單維護費用+U帳戶管理費用)
			UPDATE #CashFlowOutput SET MonthlyTotalFee = A.InsurCost + A.InsurMaintainFee  + A.AccountMgrFee
			FROM #CashFlowOutput A
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth

		    --===================================update AI再扣除每月扣除額 rule: AH10扣除保費費用-W10
			UPDATE #CashFlowOutput SET WorthAferMonthlyDeduction = @cWorthAfterSumFee - A.MonthlyTotalFee
			FROM #CashFlowOutput A
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth



			--===================================update AD保單價值加值金平均保價 rule:B10="","",IF(B10=1,0,IF(AND(AI10>0,C10=1),ROUND(SUM(OFFSET($AI$10,12*(B10-2)+1,0,12,1))/12,Decimal),0))===================================================
			UPDATE #CashFlowOutput SET AvgInsurCost =  CASE WHEN @kInsurYearCount = 1 THEN 0
															WHEN A.cMonth = 1 AND A.WorthAferMonthlyDeduction > 0 AND @kInsurYearCount > 1 
																THEN 0 --:todo 這是算年度平均保價  先給0
															ELSE 0 
														END
			FROM #CashFlowOutput A
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth



			---====================================update 保單價值加值金金額rule: ROUND(AD 平均保價 *AE 比例 ,Decimal))=================================================
			UPDATE #CashFlowOutput SET AddOnCost = AvgInsurCost * AddOnCostRate
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth

			--===================================update　加上保單價值加值金 AJ　rule: AI11+AF11=====================
			UPDATE #CashFlowOutput SET WorthAfterReserve = WorthAferMonthlyDeduction + AddOnCost
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth

			

			--==================================update AM 反映投報率後 rule:ROUND(AJ10*(1+AL10)^(1/12),Decimal)
			
			--DECLARE @TestNum1 AS decimal(16,11)
			--DECLARE @TestNum2 AS decimal(16,7)
			--DECLARE @TestNum3 AS decimal(16,9)

			SELECT @cReturnOnInvestment = ROUND(A.WorthAfterReserve * POWER(( cast(1 as decimal(17,11)) + cast(@InventRateONReturn as decimal(17,11)) ), (cast(1 as decimal(17,11))/ (cast(12 as decimal(17,11))))),0)
			--, @TestNum2 = A.WorthAfterReserve * POWER(( cast(1 as decimal(17,6)) + cast(@InventRateONReturn as decimal(17,6)) ), (cast(1 as decimal(17,6))/ (cast(12 as decimal(17,6)))))
			--, @TestNum3 = ROUND(A.WorthAfterReserve * POWER(( cast(1 as decimal(17,9)) + cast(@InventRateONReturn as decimal(17,9)) ), (cast(1 as decimal(17,9))/ (cast(12 as decimal(17,9))))),0)
			--, @TestNum1 = POWER(( cast(1 as decimal(17,11)) + cast(@InventRateONReturn as decimal(17,11)) ), (cast(1 as decimal(17,11))/ (cast(12 as decimal(17,11))))) 
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth
			--PRINT CONVERT(VARCHAR,@cReturnOnInvestment) + ',' + CONVERT(VARCHAR,@TestNum2)  + ',' + CONVERT(VARCHAR,@TestNum3)  + ',' + CONVERT(VARCHAR,@TestNum1)  

			UPDATE  #CashFlowOutput SET ReturnOnInvestment = @cReturnOnInvestment
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth

			

			SET @cReturnOnInvestment = ( SELECT ReturnOnInvestment FROM #CashFlowOutput A WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth )
			
			--PRINT @cReturnOnInvestment
			--=========================update 申購費用第一期保險費 InitialSubscriptionFee =IF(AND(B10=1,C10=1),ROUND(AI10*AA10,Decimal),0)==================================================================
			UPDATE #CashFlowOutput SET InitialSubscriptionFee = ISNULL(A.WorthAferMonthlyDeduction * A.SubscriptionFeeRate, 0)
			FROM #CashFlowOutput  A 
			WHERE A.InsurYearCount = 1 AND A.cMonth = 1 


			--=========================update  申購費用保單價值加值金= ROUND(AF10*AA10,Decimal)=================================================================

			UPDATE #CashFlowOutput SET SubscriptionAddons  = AddOnCost * SubscriptionFeeRate
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth


			--=========================update 申購費用合計SubscriptionFeeSum Y18+Z18)
			
			UPDATE #CashFlowOutput SET SubscriptionFeeSum  = ISNULL(SubscriptionAddons + InitialSubscriptionFee, 0)
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth


			--=============================================update 再扣除申購費用 WorthAfterSubscriptionFee (AJ18-AB18)===============================
			UPDATE #CashFlowOutput SET WorthAfterSubscriptionFee = WorthAfterReserve - SubscriptionFeeSum
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth


			--==================================update解約費用 金額 ROUND(AM10*AO10,Decimal)解約金========================================================================================
			--============解約費用 金額
			UPDATE  #CashFlowOutput SET SurrenderCharge = ROUND(@cReturnOnInvestment * A.SurrenderRate,0)
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth
		


			--===========update 解約費用 解約金AM10-AP10=================================================================================================
			UPDATE  #CashFlowOutput SET SurrenderReturn = ROUND(@cReturnOnInvestment - A.SurrenderCharge,0,1)
			FROM #CashFlowOutput A 
			WHERE A.InsurYearCount = @kInsurYearCount AND A.cMonth = @kMonth
			

			--===========update 保險金額  =IF(OR(B10="",type="年金"),"",IF(AM(ReturnOnInvestment)<=0,0,IF(type="甲型",MAX(N10,AM10),N10+AM10)))=============================
			IF (@SubSType = 1  AND @proposalType != 5 ) --甲型1,
			BEGIN
				UPDATE #CashFlowOutput SET InsurenceAmt  = CASE WHEN (ReturnOnInvestment  <= 0)  THEN 0 
																WHEN BasicAssured > WorthAfterSumFee THEN BasicAssured
																ELSE WorthAfterSumFee
															END
				WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth


			END
			Else If( @SubSType = 2 AND @proposalType != 5 ) --乙型2 IF(type="甲型",MAX(N10-AH10,0),N10) -- N11
			BEGIN
				UPDATE #CashFlowOutput SET InsurenceAmt  = CASE WHEN (ReturnOnInvestment  <= 0)  THEN 0 
																ELSE WorthAfterSumFee + BasicAssured
															END
				WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth	
			END
			ELSE --年金 IF(type="年金",0)
				BEGIN 
					UPDATE #CashFlowOutput SET InsurenceAmt  = 0  
										WHERE InsurYearCount = @kInsurYearCount and cMonth = @kMonth
			END 
			
 			FETCH NEXT FROM cashFlow_cursor INTO @kInsurYearCount, @kMonth, @cSumFee --, @cWorthAferMonthlyDeduction, @cWorthAfterReserve,@cWorthAfterSubscriptionFee
		END 
		-- 6 - Close the cursor
		CLOSE cashFlow_cursor  

		-- 7 - Deallocate the cursor
		DEALLOCATE cashFlow_cursor 

		SELECT * FROM #CashFlowOutput
	
	 DROP TABLE #CashFlowOutput
	 DROP TABLE #TEMP_MONTHDATE
  

END
GO
/****** Object:  StoredProcedure [dbo].[QueryInvestment_CDMN_Info]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
---- =============================================
---- Author:		Ron, Lin
---- Create date: 2021-12-15
---- Description:	Create proposal Data by Excel rules. CDMN_1 = @Rate = 0.01 , CDMN_2 = @Rate = 0.02
---- =============================================
CREATE PROCEDURE [dbo].[QueryInvestment_CDMN_Info]
	-- Add the parameters for the stored procedure here
	--DECLARE
	@Rate  decimal(8,4), -- 年金預定利率
	@Gender	 VARCHAR(1) -- 性別 F AS Female OR M as Male
	--@Age INT, --投保年齡
	--@Amt decimal (17,6) -- 試算金額 不輸入則為100000
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @vRate decimal (17,9)
	DECLARE @Year int
	DECLARE @Age int 
	DECLARE @Amt decimal (16,2)
	
	DECLARE @IYearCount int
	DECLARE @CAge int
	DECLARE @qx  decimal(17,8)

	--Test set
	SET @Age = 0


	SET @Amt = 100000  -- default 設定10萬

	if(@Rate IS null OR @Rate <= 0)
	BEGIN
		SET @Rate = 0.01 -- 1%
	END 
		SET @vRate = Power((Cast(1 as float)+ Cast(@Rate as float))　, -1)


		--Create temp table for the data--------
		;with cteNums(age) AS
		(
			SELECT 0
			UNION ALL
			SELECT age + 1
			FROM cteNums WHERE age <= 110 -- how many times to iterate
		)
		SELECT ROW_NUMBER() OVER (Order by A.[Age] ASC) AS Num, [YearKey],	A.[Age],[Male],[Female] 
		INTO #CurrATSORate
		FROM PXATSO A INNER JOIN  cteNums B
		ON A.Age = B.age
		WHERE B.Age <= 110
		OPTION  (MAXRECURSION 0)

		--select * from #CurrATSORate
	--DROP TABLE #ResultTable 
	--DROP TABLE #CurrATSORate

		--保單年度	保險年齡	D(x+year)	N(x+year)	D(x+year)	N(x+year)
		CREATE TABLE #ResultTable 
		(
			IYearCount int,
			Age		   int,
			qx		   decimal(17,8),
			Ix		   decimal(17,2),
			Cx		   decimal(17,2),		
			Dx		   decimal(17,2),		
			Mx		   decimal(17,2),	
			Nx		   decimal(17,2)
		)


		--Create basic table
		INSERT INTO #ResultTable (IYearCount, Age , qx)
		SELECT Num
			  ,Age
			  ,CASE WHEN @Gender = 'F' THEN  Female ELSE Male END AS 'qx' 
		FROM #CurrATSORate

		--DECLARE @Amt decimal(17,4) 
		--SET @Amt= 100000
		--DECLARE @vRate decimal (17,9)
		--SET @vRate = POWER(CAST(1.01 as float), (-1))


		--Declcare Cursor to update lx, Cx, Dx
		DECLARE atso_cursor CURSOR FOR
		  SELECT IYearCount, Age , qx FROM  #ResultTable

		-- Open the Cursor
		OPEN atso_cursor
		-- 3 - Fetch the next record from the cursor
		FETCH NEXT FROM atso_cursor INTO @IYearCount , @CAge, @qx
		-- Set the status for the cursor
		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			-- 4 - Begin the custom business logic
   			--PRINT @IYearCount + @CAge +  @qx
			UPDATE #ResultTable SET Ix = @Amt 
								WHERE IYearCount  = @IYearCount
			UPDATE #ResultTable SET Cx = ROUND(@qx  * @Amt  *  (POWER( @vRate, (Cast(@CAge as float) + 0.5))), 4)
								WHERE IYearCount  = @IYearCount
			UPDATE #ResultTable SET Dx = @Amt * (Power(@vRate , @CAge))
								WHERE IYearCount  = @IYearCount		

			--SELECT @qx, @Amt, @vRate,@CAge
			SET @Amt = @AMT -  (@qx * @Amt * 1 )
			-- 5 - Fetch the next record from the cursor
 			FETCH NEXT FROM atso_cursor INTO @IYearCount , @CAge, @qx
		END 
		-- 6 - Close the cursor
		CLOSE atso_cursor  

		-- 7 - Deallocate the cursor
		DEALLOCATE atso_cursor 


		--Declcare Cursor to update mx, Nx
		DECLARE atsoSum_cursor CURSOR FOR
		  SELECT IYearCount FROM  #ResultTable

		-- Open the Cursor
		OPEN atsoSum_cursor
		-- 3 - Fetch the next record from the cursor
		FETCH NEXT FROM atsoSum_cursor INTO @IYearCount 
		-- Set the status for the cursor
		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			-- 4 - Begin the custom business logic
   			--PRINT @IYearCount + @CAge +  @qx
			UPDATE #ResultTable SET  Mx = (SELECT SUM(cx) FROM #ResultTable WHERE IYearCount  >= @IYearCount)
									,Nx =  (SELECT SUM(dx) FROM #ResultTable WHERE IYearCount  >= @IYearCount)
			WHERE IYearCount = @IYearCount
			-- 5 - Fetch the next record from the cursor
 			FETCH NEXT FROM atsoSum_cursor INTO @IYearCount 
		END 
		-- 6 - Close the cursor
		CLOSE atsoSum_cursor  

		-- 7 - Deallocate the cursor
		DEALLOCATE atsoSum_cursor 


	    SELECT * FROM #ResultTable

		DROP TABLE #ResultTable
		DROP TABLE #CurrATSORate


END
GO
/****** Object:  StoredProcedure [dbo].[QueryYearly_Cashflow]    Script Date: 2021/12/24 下午 04:40:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Ron, Lin
-- Create date:  2021 12 24
-- Description:	年報資料
-- =============================================
CREATE PROCEDURE [dbo].[QueryYearly_Cashflow]
	-- Add the parameters for the stored procedure here
	@InvestmentReturnRate decimal(5,2), --between 6% to -6%
	@InvestmentAmt decimal(17,4), --保費
	@payOption int,  --彈性繳(1)/躉繳(2)
	@annuityYearCount int,  --年金累積期間
	@proposalType int, --Type 5 = 年金
	@UserAge	int,	--User age
	@PlanCode		varchar(8), -- plan or PlanCode eg:FVA01N
	@Gender		int, --性別 Male 1, Female 2
	@IncreaseRate   decimal(8,4), --加費比例
	@InventRateONReturn decimal(8,4), --投資報酬率
	@SubSType int, --甲型1, 乙型,
	@Unit_Coverage int, --單位保額
	@InvestTargets int -- 投資標的

AS
BEGIN
	SET NOCOUNT ON;

	CREATE TABLE #CashFlowOutput
	(
	  InsurYearCount int, --保單年度
	  cMonth		 int, --月份
	  cAge			 int, --保險年齡
	  --保險費 
	  InitialInsurFee		 decimal(16,4), --第一期保險費/"躉繳保險費"  pay_option="躉繳","躉繳保險費",IF(pay_option="彈性繳","第一期保險費"
	  PaidInsurFee			 decimal(16,4), --所繳保險費
	  SumPaidInsurFee		 decimal(16,4), --累積所繳保險費	
	  --保費費用		
	  InitialFee			 decimal(16,4), --第一期保費費用(手續費?)
	  LoadingPercent		 decimal(6,4) , --費率百分比
	  SumFee				 decimal(16,4), --管理費合計
	  --保險成本
	  BasicAssured			 decimal(16,4), --基本保額
	  Corridor				 decimal(6,4), -- Corridor rules ,IF(type="年金",0%)
	  RiskＣoverage			 decimal(16,4), -- 淨危險保額
	  StandRate				 decimal(16,4), -- 標準體費率
	  SubStandIncreaseRate	 decimal(16,4), --次標準體加費比例
	  InsurCost				 decimal(16,4), --保險成本
	  --保單管理費
	  InsurMaintainFee		 decimal(16, 4), --保單維護費用
	  AccountMgrFee			 decimal(16, 4), --帳戶管理費用
	  InsurMgrFeePc			 decimal(6, 4),  --保單管理費比例
	  MonthlyTotalFee		 decimal(16,4),  --每月扣除額
	  --申購費用
	  InitialSubscriptionFee decimal(16, 4), --申購第一期保險費
	  SubscriptionAddons	 decimal(16, 4),--申購保單價值加值金
	  SubscriptionFeeRate	 decimal(6, 4), -- 申購費率
	  SubscriptionFeeSum	 decimal(16, 4), --申購費合計
	  --保單價值加值金
	  AvgInsurCost			 decimal(16, 4), -- 平均保價
	  AddOnCostRate			 decimal(6,4), -- 保單價值加值金比例
	  AddOnCost				 decimal(16, 4), -- 保單價值加值金
	  --保單帳戶價值
	  WorthAfterSumFee		 decimal(16, 4), --扣除保費費用
	  WorthAferMonthlyDeduction	decimal(16, 4), --再扣除每月扣除額
	  WorthAfterReserve		 decimal(16, 4), 	--加上保單價值加值金
	  WorthAfterSubscriptionFee decimal(16, 4), --再扣除申購費用
	  ReturnRateOnInvestment decimal(6,4), -- 投資報酬率
	  ReturnOnInvestment decimal(16,4), --反映投報率後
	  --解約費用
	  SurrenderRate		 decimal(6,4), --解約費用比例
	  SurrenderCharge	 decimal(16,4), --解約費用金額
	  SurrenderReturn	 decimal(16,4),
	  --保險金額
	  InsurenceAmt		 decimal(16,4) --=IF(OR(B36="",type="年金"),"",IF(AM36<=0,0,IF(type="甲型",MAX(N36,AM36),N36+AM36)))
	)

	INSERT INTO #CashFlowOutput
    EXEC [dbo].[QueryCal_cashflow_FGR]
		@InvestmentReturnRate, --投資報酬率range 目前沒用到  
		@InvestmentAmt, --@InvestmentAmt
		@payOption, --彈性繳(1)/躉繳(2)目前沒用到
		@annuityYearCount, --目前沒用到
		@proposalType,
		@UserAge,
		@PlanCode,
		@Gender,
		@IncreaseRate, 
		@InventRateONReturn, --@InventRateONReturn
		@SubSType,
		@Unit_Coverage,
		@InvestTargets

			--"保單年度"	"保險年齡"	第一期保險費	保費費用 每月扣除額	申購費用	保單價值加值金	保單帳戶價值	解約費用	解約金	"保險金額(身故/完全失能保險金) "
			SELECT A.InsurYearCount					AS '保單年度'
				   ,A.cAge  						AS '保險年齡'
				   ,SUM(A.InitialInsurFee)			AS '第一期保險費' 
				   ,SUM(A.InitialFee)				AS '保費費用'
				   ,SUM(A.InsurCost)				AS '每月扣除額'
				   ,SUM(A.SubscriptionFeeSum)		AS '申購費用'
				   ,SUM(A.AddOnCost)				AS '保單價值加值金'
				   ,MAX(B.ReturnOnInvestment)		AS '保單帳戶價值'
				   ,MAX(B.SurrenderCharge)			AS '解約費用'
				   ,MAX(B.SurrenderReturn)			AS '解約金'
				   ,MAX(B.InsurenceAmt)				AS '保險金額(身故/完全失能保險金)' 
				   , @InventRateONReturn			AS '投資報酬率'
			FROM #CashFlowOutput A LEFT JOIN #CashFlowOutput B ON A.InsurYearCount = B.InsurYearCount AND A.cMonth = B.cMonth AND B.cMonth = 12
			GROUP BY A.InsurYearCount ,A.cAge ,A.InsurYearCount 

		DROP TABLE #CashFlowOutput
END
GO
EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPane1', @value=N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[40] 4[20] 2[20] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1 [50] 4 [25] 3))"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1 [50] 2 [25] 3))"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4 [30] 2 [40] 3))"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1 [56] 3))"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 0
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
         Begin Table = "PXATSO"
            Begin Extent = 
               Top = 7
               Left = 48
               Bottom = 170
               Right = 249
            End
            DisplayFlags = 280
            TopColumn = 0
         End
      End
   End
   Begin SQLPane = 
   End
   Begin DataPane = 
      Begin ParameterDefaults = ""
      End
      Begin ColumnWidths = 9
         Width = 284
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
      End
   End
   Begin CriteriaPane = 
      Begin ColumnWidths = 11
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1350
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vwAnnuityCostRate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vwAnnuityCostRate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPane1', @value=N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[40] 4[20] 2[20] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1 [50] 4 [25] 3))"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1 [50] 2 [25] 3))"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4 [30] 2 [40] 3))"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1 [56] 3))"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 0
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
         Begin Table = "PXTSO"
            Begin Extent = 
               Top = 7
               Left = 46
               Bottom = 170
               Right = 249
            End
            DisplayFlags = 280
            TopColumn = 0
         End
      End
   End
   Begin SQLPane = 
   End
   Begin DataPane = 
      Begin ParameterDefaults = ""
      End
      Begin ColumnWidths = 9
         Width = 284
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
         Width = 1200
      End
   End
   Begin CriteriaPane = 
      Begin ColumnWidths = 11
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1350
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vwInsuranceCostRate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vwInsuranceCostRate'
GO
s