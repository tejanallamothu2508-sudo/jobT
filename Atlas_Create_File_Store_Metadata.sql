-- Atlas Enhancement: Create file_store_metadata table
-- Tracks all Excel file + sheet combinations loaded by Atlas pipelines
-- Run this in Databricks SQL

-- Step 1: Create the metadata table
CREATE TABLE IF NOT EXISTS development_021_bronze_finance.atlas.file_store_metadata (
  metadata_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  pipeline_name STRING NOT NULL,
  file_name STRING NOT NULL,
  sheet_name STRING NOT NULL,
  destination_table STRING NOT NULL,
  load_year INT,
  load_month INT,
  loaded_date TIMESTAMP DEFAULT current_timestamp(),
  IS_DELETED BOOLEAN DEFAULT false
);

-- Step 2: Insert all 22 sheet mappings across 15 pipelines
INSERT INTO development_021_bronze_finance.atlas.file_store_metadata
  (pipeline_name, file_name, sheet_name, destination_table, load_year, load_month)
VALUES
  -- 1. CDS CII Billing Manual (1 sheet)
  ('Atlas_Bronze_CDS_CII_Billing_Manual',
   'SQL Upload - CII Manual Billing 11-2025.xlsx',
   'WP',
   'development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II_ManualBilling',
   2025, 11),

  -- 2. ZRBINQ Billing Plans (1 sheet)
  ('Atlas_Bronze_ZRBINQ_Billing_Plans',
   'SQL Upload - Billing Plans 11-2025.xlsx',
   'Upload',
   'development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans',
   2025, 11),

  -- 3. Alliant Royalties - Finance 12_2025 (1 sheet)
  ('Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_12_2025',
   'Alliant Queries_ Finance 12_2025.xlsx',
   'GL529050 & 547000',
   'development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant',
   2025, 12),

  -- 4. Alliant Royalties - Finance GL 113015 (1 sheet)
  ('Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_GL_113015_12_2025',
   'Alliant Queries_Finance GL 113015_12 2025.xlsx',
   '113015',
   'development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant',
   2025, 12),

  -- 5. Depreciation Schedule (1 sheet)
  ('Atlas_Bronze_Depreciation_Schedule',
   '2612 Combined NA Scheduled Depreciation-USD.xlsx',
   'NA Combine DEPRSCH',
   'development_021_bronze_finance.atlas.RPM_Exp_Depreciation',
   2025, 12),

  -- 6. Daily Fee Poker Install Base (1 sheet)
  ('Atlas_Bronze_Daily_Fee_Poker_Install_Base',
   'December 2025 Action Poker Unit Count Data.xlsx',
   'December 2025',
   'development_021_bronze_finance.atlas.BW_COPA_DailyFeePoker_Units',
   2025, 12),

  -- 7. Machine Sales Revenue Plugs (1 sheet)
  ('Atlas_Bronze_Machine_Sales_Revenue_Plugs',
   'BPC Adhoc Machine Revenue CY25 December Actuals File_New Structure.xlsx',
   'For Garrett',
   'development_021_bronze_finance.atlas.BPC_Plug_Financials',
   2025, 12),

  -- 8. Machine Sales Cost Plugs (1 sheet)
  ('Atlas_Bronze_Machine_Sales_Cost_Plugs',
   'BPC Adhoc Machine Cost for GBDB Working - 2025 December Actuals.xlsx',
   'Total Cost by Region by CC',
   'development_021_bronze_finance.atlas.BPC_Plug_Financials_MachineCosts',
   2025, 12),

  -- 9. Flat Fee Billing (2 sheets)
  ('Atlas_Bronze_Flat_Fee_Billing',
   'Hybris Billing File December 2025.xlsx',
   'Hybris Billing December 2025',
   'development_021_bronze_finance.atlas.RPM_Peformance_FF_MJP',
   2025, 12),

  ('Atlas_Bronze_Flat_Fee_Billing',
   'Hybris Billing File December 2025.xlsx',
   'Prior Hybris Billing',
   'development_021_bronze_finance.atlas.RPM_Peformance_FF_MJP_PriorMonth',
   2025, 12),

  -- 10. Lottery Billing (4 sheets)
  ('Atlas_Bronze_Lottery_Billing',
   'MJP Reclass 12 -DECEMBER 2025.xlsx',
   'DEL PREMIUM',
   'development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_DE',
   2025, 12),

  ('Atlas_Bronze_Lottery_Billing',
   'MJP Reclass 12 -DECEMBER 2025.xlsx',
   'NYL PREM',
   'development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_NY',
   2025, 12),

  ('Atlas_Bronze_Lottery_Billing',
   'MJP Reclass 12 -DECEMBER 2025.xlsx',
   'RIL PREMIUM',
   'development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_RI',
   2025, 12),

  ('Atlas_Bronze_Lottery_Billing',
   'MJP Reclass 12 -DECEMBER 2025.xlsx',
   'Non-Premium_11_23 to 12_27',
   'development_021_bronze_finance.atlas.RPM_Peformance_NonPremLotto_RI',
   2025, 12),

  -- 11. Africa Billing (1 sheet)
  ('Atlas_Bronze_Africa_Billing',
   '12 Africa 3600 ZRBINQ.xlsx',
   'CC3600 ZRBINQ Dec''25',
   'development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Africa',
   2025, 12),

  -- 12. EMEA Fixed Fee Billing (1 sheet)
  ('Atlas_Bronze_EMEA_Fixed_Fee_Billing',
   'Fixed Fee Install-based overview EMEA 2025-12 v2.xlsx',
   'Active',
   'development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_FixedFee',
   2025, 12),

  -- 13. Greece WLA Billing (1 sheet)
  ('Atlas_Bronze_Greece_WLA_Billing',
   'Assets_Revenue_November 25.xlsx',
   'Asset Data_Initial Contract',
   'development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_GreeceWLA',
   2025, 11),

  -- 14. Iceland Billing (1 sheet)
  ('Atlas_Bronze_Iceland_Billing',
   'Oct 1stOfMonth_UIL.xlsx',
   '20251101',
   'development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Iceland',
   2025, 10),

  -- 15. LAC Billing (1 sheet)
  ('Atlas_Bronze_LAC_Billing',
   'Noviembre Upload .xlsx',
   'Num',
   'development_021_bronze_finance.atlas.RPM_Intl_Performance_LAC',
   2025, 11);

-- Step 3: Verify
SELECT * FROM development_021_bronze_finance.atlas.file_store_metadata
ORDER BY metadata_id;
