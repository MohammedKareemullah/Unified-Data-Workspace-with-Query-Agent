# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview raw data to confirm joins work
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Quick join check: how many suppliers have addresses?
# MAGIC SELECT COUNT(DISTINCT s.Supplier) AS suppliers_with_addresses
# MAGIC FROM udw_procurement.bronze.sap_suppliers s
# MAGIC INNER JOIN udw_procurement.bronze.sap_bp_addresses a
# MAGIC   ON s.Supplier = a.BusinessPartner;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many suppliers have company data?
# MAGIC SELECT COUNT(DISTINCT s.Supplier) AS suppliers_with_company_data
# MAGIC FROM udw_procurement.bronze.sap_suppliers s
# MAGIC INNER JOIN udw_procurement.bronze.sap_supplier_company sc
# MAGIC   ON s.Supplier = sc.Supplier;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many suppliers have purchasing org data?
# MAGIC SELECT COUNT(DISTINCT s.Supplier) AS suppliers_with_purch_org
# MAGIC FROM udw_procurement.bronze.sap_suppliers s
# MAGIC INNER JOIN udw_procurement.bronze.sap_supplier_purchasing_org po
# MAGIC   ON s.Supplier = po.Supplier;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Create Silver Vendors Delta table
# MAGIC -- Sources:
# MAGIC --   A_Supplier           → identity, blocking flags, tax, industry
# MAGIC --   A_SupplierCompany    → payment terms, reconciliation, currency
# MAGIC --   A_SupplierPurchOrg   → incoterms, lead time, min order amount
# MAGIC --   A_BusinessPartnerAddress → city, country, postal code, street
# MAGIC --   pg_vendor_master     → category, active flag, contract data
# MAGIC --   pg_vendor_contracts  → aggregated contract metrics
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.vendors
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/vendors/'
# MAGIC AS
# MAGIC WITH sap_sup AS (
# MAGIC   SELECT
# MAGIC     Supplier                          AS vendor_id,
# MAGIC     COALESCE(SupplierFullName,
# MAGIC              SupplierName)            AS vendor_name_sap,
# MAGIC     SupplierCorporateGroup            AS corporate_group,
# MAGIC     SupplierAccountGroup              AS account_group,
# MAGIC     Industry                          AS industry_code,
# MAGIC     VATRegistration                   AS vat_number,
# MAGIC     TaxNumber1                        AS tax_number_1,
# MAGIC     TaxNumber2                        AS tax_number_2,
# MAGIC     CAST(DeletionIndicator
# MAGIC          AS BOOLEAN)                  AS is_deleted,
# MAGIC     CAST(PaymentIsBlockedForSupplier
# MAGIC          AS BOOLEAN)                  AS payment_blocked,
# MAGIC     CAST(PurchasingIsBlocked
# MAGIC          AS BOOLEAN)                  AS purchasing_blocked,
# MAGIC     CAST(PostingIsBlocked
# MAGIC          AS BOOLEAN)                  AS posting_blocked,
# MAGIC     SuplrQualityManagementSystem      AS quality_mgmt_system,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(SuplrQltyInProcmtCertfnValidTo,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                 AS quality_cert_valid_to,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                 AS sap_creation_date
# MAGIC   FROM udw_procurement.bronze.sap_suppliers
# MAGIC   WHERE Supplier IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY Supplier ORDER BY Supplier
# MAGIC   ) = 1
# MAGIC ),
# MAGIC sap_company AS (
# MAGIC   -- Financial/accounting data per company code
# MAGIC   SELECT
# MAGIC     Supplier                          AS vendor_id,
# MAGIC     CompanyCode                       AS company_code,
# MAGIC     CompanyCodeName                   AS company_code_name,
# MAGIC     Currency                          AS company_currency,
# MAGIC     PaymentTerms                      AS payment_terms,
# MAGIC     ReconciliationAccount             AS recon_account,
# MAGIC     AccountingClerk                   AS accounting_clerk,
# MAGIC     HouseBank                         AS house_bank,
# MAGIC     CAST(SupplierIsBlockedForPosting
# MAGIC          AS BOOLEAN)                  AS blocked_for_posting,
# MAGIC     CAST(IsToBeCheckedForDuplicates
# MAGIC          AS BOOLEAN)                  AS check_for_duplicates
# MAGIC   FROM udw_procurement.bronze.sap_supplier_company
# MAGIC   WHERE Supplier IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY Supplier ORDER BY CompanyCode
# MAGIC   ) = 1
# MAGIC ),
# MAGIC sap_purch_org AS (
# MAGIC   -- Purchasing configuration per purchasing org
# MAGIC   SELECT
# MAGIC     Supplier                          AS vendor_id,
# MAGIC     PurchasingOrganization            AS purchasing_org,
# MAGIC     PurchaseOrderCurrency             AS po_currency,
# MAGIC     PaymentTerms                      AS purch_payment_terms,
# MAGIC     IncotermsClassification           AS incoterms,
# MAGIC     IncotermsLocation1                AS incoterms_location,
# MAGIC     CAST(MinimumOrderAmount
# MAGIC          AS DOUBLE)                   AS min_order_amount,
# MAGIC     CAST(MaterialPlannedDeliveryDurn
# MAGIC          AS INT)                      AS planned_delivery_days,
# MAGIC     SupplierPhoneNumber               AS purchasing_phone,
# MAGIC     SupplierRespSalesPersonName       AS sales_person,
# MAGIC     SupplierABCClassificationCode     AS abc_classification,
# MAGIC     CAST(IsOrderAcknRqd
# MAGIC          AS BOOLEAN)                  AS order_ack_required,
# MAGIC     CAST(PurchasingIsBlockedForSupplier
# MAGIC          AS BOOLEAN)                  AS purch_blocked,
# MAGIC     CAST(EvaldReceiptSettlementIsActive
# MAGIC          AS BOOLEAN)                  AS evaluated_receipt_active
# MAGIC   FROM udw_procurement.bronze.sap_supplier_purchasing_org
# MAGIC   WHERE Supplier IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY Supplier ORDER BY PurchasingOrganization
# MAGIC   ) = 1
# MAGIC ),
# MAGIC sap_addr AS (
# MAGIC   -- Primary address from Business Partner Address entity
# MAGIC   -- BusinessPartner ID = Supplier ID
# MAGIC   SELECT
# MAGIC     BusinessPartner                   AS vendor_id,
# MAGIC     FullName                          AS address_full_name,
# MAGIC     CityName                          AS city,
# MAGIC     PostalCode                        AS postal_code,
# MAGIC     StreetName                        AS street,
# MAGIC     HouseNumber                       AS house_number,
# MAGIC     Region                            AS region,
# MAGIC     Country                           AS country,
# MAGIC     District                          AS district,
# MAGIC     County                            AS county,
# MAGIC     AddressTimeZone                   AS timezone,
# MAGIC     TransportZone                     AS transport_zone
# MAGIC   FROM udw_procurement.bronze.sap_bp_addresses
# MAGIC   WHERE BusinessPartner IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY BusinessPartner ORDER BY BusinessPartner
# MAGIC   ) = 1
# MAGIC ),
# MAGIC pg_ven AS (
# MAGIC   -- PostgreSQL enrichment: category, active status
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     vendor_name                       AS vendor_name_pg,
# MAGIC     country                           AS country_pg,
# MAGIC     city                              AS city_pg,
# MAGIC     payment_terms                     AS payment_terms_pg,
# MAGIC     currency                          AS pg_currency,
# MAGIC     vendor_category,
# MAGIC     CAST(is_active AS BOOLEAN)        AS is_active,
# MAGIC     created_date
# MAGIC   FROM udw_procurement.bronze.pg_vendor_master
# MAGIC   WHERE vendor_id IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY vendor_id ORDER BY vendor_id
# MAGIC   ) = 1
# MAGIC ),
# MAGIC pg_contracts_agg AS (
# MAGIC   -- Contract aggregates per vendor
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     COUNT(contract_id)                AS contract_count,
# MAGIC     SUM(CAST(contract_value
# MAGIC              AS DOUBLE))              AS total_contract_value,
# MAGIC     MAX(end_date)                     AS latest_contract_end,
# MAGIC     MIN(start_date)                   AS earliest_contract_start,
# MAGIC     COUNT(DISTINCT category)          AS contract_categories
# MAGIC   FROM udw_procurement.bronze.pg_vendor_contracts
# MAGIC   GROUP BY vendor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Primary key
# MAGIC   COALESCE(s.vendor_id, p.vendor_id) AS vendor_id,
# MAGIC
# MAGIC   -- Name: prefer SAP full name, fall back to PostgreSQL
# MAGIC   COALESCE(s.vendor_name_sap,
# MAGIC            p.vendor_name_pg)          AS vendor_name,
# MAGIC
# MAGIC   -- Address: prefer SAP BP Address entity
# MAGIC   COALESCE(a.city, p.city_pg)         AS city,
# MAGIC   COALESCE(a.country, p.country_pg)   AS country,
# MAGIC   a.region,
# MAGIC   a.postal_code,
# MAGIC   a.street,
# MAGIC   a.house_number,
# MAGIC   a.district,
# MAGIC   a.transport_zone,
# MAGIC   a.timezone,
# MAGIC
# MAGIC   -- Commercial/financial data
# MAGIC   COALESCE(
# MAGIC     p.payment_terms_pg,
# MAGIC     sc.payment_terms,
# MAGIC     po.purch_payment_terms
# MAGIC   )                                   AS payment_terms,
# MAGIC   COALESCE(
# MAGIC     p.pg_currency,
# MAGIC     sc.company_currency,
# MAGIC     po.po_currency
# MAGIC   )                                   AS currency,
# MAGIC   sc.company_code,
# MAGIC   sc.company_code_name,
# MAGIC   sc.recon_account,
# MAGIC   sc.accounting_clerk,
# MAGIC   sc.house_bank,
# MAGIC
# MAGIC   -- Purchasing configuration
# MAGIC   po.purchasing_org,
# MAGIC   po.incoterms,
# MAGIC   po.incoterms_location,
# MAGIC   po.min_order_amount,
# MAGIC   po.planned_delivery_days,
# MAGIC   po.sales_person,
# MAGIC   po.abc_classification,
# MAGIC   po.order_ack_required,
# MAGIC   po.evaluated_receipt_active,
# MAGIC
# MAGIC   -- Identity and classification
# MAGIC   p.vendor_category,
# MAGIC   s.industry_code,
# MAGIC   s.corporate_group,
# MAGIC   s.account_group,
# MAGIC   s.vat_number,
# MAGIC   s.tax_number_1,
# MAGIC   s.quality_mgmt_system,
# MAGIC   s.quality_cert_valid_to,
# MAGIC   s.sap_creation_date,
# MAGIC   p.created_date                      AS pg_created_date,
# MAGIC
# MAGIC   -- Status flags
# MAGIC   COALESCE(p.is_active,
# MAGIC            NOT COALESCE(s.is_deleted, FALSE)) AS is_active,
# MAGIC   COALESCE(s.is_deleted, FALSE)       AS is_deleted,
# MAGIC   COALESCE(s.payment_blocked, FALSE)  AS payment_blocked,
# MAGIC   COALESCE(s.purchasing_blocked, FALSE) AS purchasing_blocked,
# MAGIC   COALESCE(s.posting_blocked, FALSE)  AS posting_blocked,
# MAGIC   COALESCE(sc.blocked_for_posting,
# MAGIC            FALSE)                     AS company_blocked,
# MAGIC
# MAGIC   -- Contract metrics
# MAGIC   c.contract_count,
# MAGIC   c.total_contract_value,
# MAGIC   c.latest_contract_end,
# MAGIC   c.earliest_contract_start,
# MAGIC   c.contract_categories,
# MAGIC
# MAGIC   -- Metadata
# MAGIC   CURRENT_DATE()                      AS ingestion_date,
# MAGIC   'SAP_S4H+PostgreSQL'               AS source_system
# MAGIC
# MAGIC FROM sap_sup s
# MAGIC FULL OUTER JOIN pg_ven p
# MAGIC   ON  s.vendor_id = p.vendor_id
# MAGIC LEFT JOIN sap_addr a
# MAGIC   ON  COALESCE(s.vendor_id, p.vendor_id) = a.vendor_id
# MAGIC LEFT JOIN sap_company sc
# MAGIC   ON  COALESCE(s.vendor_id, p.vendor_id) = sc.vendor_id
# MAGIC LEFT JOIN sap_purch_org po
# MAGIC   ON  COALESCE(s.vendor_id, p.vendor_id) = po.vendor_id
# MAGIC LEFT JOIN pg_contracts_agg c
# MAGIC   ON  COALESCE(s.vendor_id, p.vendor_id) = c.vendor_id
# MAGIC WHERE COALESCE(s.vendor_id, p.vendor_id) IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_id from udw_procurement.silver.vendors;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Validate Silver Vendors
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS total_vendors,
# MAGIC   COUNT(DISTINCT country)               AS countries,
# MAGIC   COUNT(DISTINCT vendor_category)       AS categories,
# MAGIC   SUM(CASE WHEN is_active
# MAGIC       THEN 1 ELSE 0 END)                AS active_vendors,
# MAGIC   SUM(CASE WHEN purchasing_blocked
# MAGIC       THEN 1 ELSE 0 END)                AS purchasing_blocked,
# MAGIC   SUM(CASE WHEN contract_count > 0
# MAGIC       THEN 1 ELSE 0 END)                AS vendors_with_contracts,
# MAGIC   ROUND(SUM(total_contract_value), 0)   AS total_contract_value,
# MAGIC   AVG(planned_delivery_days)            AS avg_lead_time_days
# MAGIC FROM udw_procurement.silver.vendors;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 4 — Vendor breakdown by country and category
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   country,
# MAGIC   vendor_category,
# MAGIC   abc_classification,
# MAGIC   COUNT(*)                              AS vendor_count,
# MAGIC   ROUND(AVG(planned_delivery_days), 1)  AS avg_lead_time_days,
# MAGIC   ROUND(SUM(total_contract_value), 0)   AS total_contract_value
# MAGIC FROM udw_procurement.silver.vendors
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY total_contract_value DESC NULLS LAST
# MAGIC LIMIT 20;