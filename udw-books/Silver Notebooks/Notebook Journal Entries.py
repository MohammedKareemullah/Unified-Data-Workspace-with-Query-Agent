# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview actual field names
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC SELECT * FROM udw_procurement.bronze.sap_journal_entries_custom LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Silver Journal Entries
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.journal_entries
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/journal_entries/'
# MAGIC AS
# MAGIC WITH fx AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN target_currency = 'INR'
# MAGIC         THEN exchange_rate END)                     AS inr_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'EUR'
# MAGIC         THEN exchange_rate END)                     AS eur_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'SAR'
# MAGIC         THEN exchange_rate END)                     AS sar_per_usd
# MAGIC   FROM udw_procurement.bronze.fx_rates
# MAGIC   WHERE base_currency = 'USD'
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Document identity
# MAGIC   j.Ledger                                          AS ledger,
# MAGIC   j.CompanyCode                                     AS company_code,
# MAGIC   j.FiscalYear                                      AS fiscal_year,
# MAGIC   j.AccountingDocument                              AS document_number,
# MAGIC   j.LedgerGLLineItem                                AS line_item,
# MAGIC
# MAGIC   -- GL and cost objects
# MAGIC   j.GLAccount                                       AS gl_account,
# MAGIC   j.CostCenter                                      AS cost_center,
# MAGIC   j.ProfitCenter                                    AS profit_center,
# MAGIC   j.Segment                                         AS segment,
# MAGIC   j.BusinessArea                                    AS business_area,
# MAGIC   j.ControllingArea                                 AS controlling_area,
# MAGIC   j.FunctionalArea                                  AS functional_area,
# MAGIC   j.WBSElement                                      AS wbs_element,
# MAGIC   j.OrderID                                         AS internal_order,
# MAGIC
# MAGIC   -- Business partner
# MAGIC   j.Supplier                                        AS vendor_id,
# MAGIC   j.Customer                                        AS customer_id,
# MAGIC
# MAGIC   -- Posting metadata
# MAGIC   j.AccountingDocumentType                          AS document_type,
# MAGIC   j.DebitCreditCode                                 AS debit_credit,
# MAGIC   j.TaxCode                                         AS tax_code,
# MAGIC   j.AssignmentReference                             AS assignment_ref,
# MAGIC   j.DocumentItemText                                AS item_text,
# MAGIC   j.CreatedByUser                                   AS created_by,
# MAGIC
# MAGIC   -- Fiscal period
# MAGIC   CAST(j.FiscalPeriod AS INT)                       AS fiscal_period,
# MAGIC   j.FiscalYearVariant                               AS fiscal_year_variant,
# MAGIC
# MAGIC   -- Dates
# MAGIC --   CAST(j.PostingDate AS DATE)                       AS posting_date,
# MAGIC --   CAST(j.DocumentDate AS DATE)                      AS document_date,
# MAGIC
# MAGIC
# MAGIC   CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(j.PostingDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC   )                                                     AS posting_date,
# MAGIC
# MAGIC   CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(j.DocumentDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC   )                                                     AS document_date,
# MAGIC   
# MAGIC --   CAST(j.EntryDate AS DATE)                         AS entry_date,
# MAGIC
# MAGIC   -- Amounts in transaction currency
# MAGIC   j.TransactionCurrency                             AS transaction_currency,
# MAGIC   CAST(j.AmountInTransactionCurrency AS DOUBLE)     AS amount_transaction_curr,
# MAGIC
# MAGIC   -- Amounts in company code currency
# MAGIC   j.CompanyCodeCurrency                             AS company_code_currency,
# MAGIC   CAST(j.CompanyCodeCurrencyAmount AS DOUBLE)       AS amount_company_curr,
# MAGIC
# MAGIC   -- Normalize to USD
# MAGIC   ROUND(
# MAGIC     CASE UPPER(j.TransactionCurrency)
# MAGIC       WHEN 'USD' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC       WHEN 'INR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.sar_per_usd
# MAGIC       ELSE CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC     END, 2
# MAGIC   )                                                 AS amount_usd,
# MAGIC
# MAGIC   -- Amount sign for analytics
# MAGIC   -- S = debit (cost), H = credit (revenue/offset)
# MAGIC   CASE j.DebitCreditCode
# MAGIC     WHEN 'S' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC     WHEN 'H' THEN -CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC     ELSE CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC   END                                               AS signed_amount_transaction,
# MAGIC
# MAGIC   CASE j.DebitCreditCode
# MAGIC     WHEN 'S' THEN ROUND(
# MAGIC       CASE UPPER(j.TransactionCurrency)
# MAGIC         WHEN 'USD' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC         WHEN 'INR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC         WHEN 'EUR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.eur_per_usd
# MAGIC         WHEN 'SAR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.sar_per_usd
# MAGIC         ELSE CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC       END, 2)
# MAGIC     WHEN 'H' THEN -ROUND(
# MAGIC       CASE UPPER(j.TransactionCurrency)
# MAGIC         WHEN 'USD' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE)
# MAGIC         WHEN 'INR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC         WHEN 'EUR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.eur_per_usd
# MAGIC         WHEN 'SAR' THEN CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.sar_per_usd
# MAGIC         ELSE CAST(j.AmountInTransactionCurrency AS DOUBLE) / fx.inr_per_usd
# MAGIC       END, 2)
# MAGIC     ELSE 0
# MAGIC   END                                               AS signed_amount_usd,
# MAGIC
# MAGIC   -- Quantity
# MAGIC   CAST(j.Quantity AS DOUBLE)                        AS quantity,
# MAGIC   j.BaseUnit                                        AS base_unit,
# MAGIC   j.Material                                        AS material_number,
# MAGIC
# MAGIC   -- Reference documents
# MAGIC   j.PurchaseOrder                                   AS po_number,
# MAGIC   j.PurchaseOrderItem                               AS po_item,
# MAGIC   j.SalesDocument                                   AS sales_document,
# MAGIC   j.SalesDocumentItem                               AS sales_document_item,
# MAGIC   j.TransactionType                                 AS transaction_type,
# MAGIC
# MAGIC   -- Time dimensions
# MAGIC
# MAGIC   
# MAGIC   YEAR(posting_date)                 AS posting_year,
# MAGIC   MONTH(posting_date)                AS posting_month,
# MAGIC   DATE_TRUNC('MONTH',
# MAGIC     posting_date)                    AS posting_month_date,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_ACDOCA_CUSTOM'                               AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_journal_entries_custom j
# MAGIC CROSS JOIN fx
# MAGIC WHERE j.AccountingDocument IS NOT NULL
# MAGIC   AND j.LedgerGLLineItem IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY j.CompanyCode, j.FiscalYear,
# MAGIC                j.AccountingDocument, j.LedgerGLLineItem
# MAGIC   ORDER BY j.AccountingDocument
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Validate Silver Journal Entries
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS total_line_items,
# MAGIC   COUNT(DISTINCT document_number)       AS distinct_documents,
# MAGIC   COUNT(DISTINCT company_code)          AS company_codes,
# MAGIC   COUNT(DISTINCT cost_center)           AS cost_centers,
# MAGIC   COUNT(DISTINCT gl_account)            AS gl_accounts,
# MAGIC   COUNT(DISTINCT fiscal_year)           AS fiscal_years,
# MAGIC   MIN(posting_date)                     AS earliest_posting,
# MAGIC   MAX(posting_date)                     AS latest_posting,
# MAGIC   ROUND(SUM(CASE WHEN debit_credit = 'S'
# MAGIC       THEN amount_usd ELSE 0 END), 2)   AS total_debits_usd,
# MAGIC   ROUND(SUM(CASE WHEN debit_credit = 'H'
# MAGIC       THEN amount_usd ELSE 0 END), 2)   AS total_credits_usd
# MAGIC FROM udw_procurement.silver.journal_entries;