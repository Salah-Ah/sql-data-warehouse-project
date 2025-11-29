-- Creating cust_info table in the silver schema
IF  OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
Create table silver.crm_cust_info(
cst_id				INT,
cst_key				NVARCHAR(50),
cst_firstname		NVARCHAR(50),
cst_lastname		NVARCHAR(50),
cst_marital_status	NVARCHAR(50), 
cst_gndr			NVARCHAR(50),
cst_create_date		Date,
dwh_create_Date		Datetime2 default  getdate()
)
GO

-- Creating prd_info table in the silver schema
IF  OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
prd_id				INT,
cat_id				NVARCHAR(50),
prd_key				NVARCHAR(50),
prd_nm				NVARCHAR(50),
prd_cost			DECIMAL(6,2),
prd_line			NVARCHAR(50),
prd_start_dt		DATE,
prd_end_dt			DATE,
dwh_create_Date		Datetime2 default  getdate()
)
GO

-- Creating sales_details table in the silver schema
IF  OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
sls_ord_num		    NVARCHAR(50),
sls_prd_key			NVARCHAR(50),
sls_cust_id			INT,
sls_order_dt		DATE,
sls_ship_dt			DATE,
sls_due_dt			DATE,
sls_price			INT,
sls_quantity		INT,
sls_sales			INT,
dwh_create_Date		Datetime2 default  getdate()
)
GO

-- Creating cust_az12 table in the silver schema
IF  OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
cid					NVARCHAR(50),
bdate				DATE,
gen					NVARCHAR(50),
dwh_create_Date		Datetime2 default  getdate()
)
GO

-- Creating loc_a101 table in the silver schema
IF  OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
cid					NVARCHAR(50),
cntry				NVARCHAR(50),
dwh_create_Date		Datetime2 default  getdate()
)
GO

-- Creating px_cat_g1v2 table in the silver schema
IF  OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
id					NVARCHAR(50),
cat					NVARCHAR(50),
subcat				NVARCHAR(50),
maintenance			NVARCHAR(50),
dwh_create_Date		Datetime2 default  getdate()
)
GO

Select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
Case When sls_order_dt <= 0
	 or len(sls_order_dt) != 8 Then null 
	 else cast(cast(sls_order_dt as varchar) as date)
End as sls_order_dt,
Case When sls_ship_dt <= 0 
	 or len(sls_ship_dt) != 8 Then null
	 else cast(cast(sls_ship_dt as varchar) as date)
End as sls_ship_dt,
Case When sls_due_dt <= 0 
	 or len(sls_due_dt) != 8 Then null 
	 else cast(cast(sls_due_dt as varchar) as date)
End as sls_due_dt,

Case when sls_price is null or sls_price <= 0 
	 Then sls_sales / nullif(sls_quantity,0)
	 else sls_price
End as sls_price,
	sls_quantity,
Case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
	 Then sls_quantity * ABS(sls_price)
	 else sls_sales
End as sls_sales
From 
	bronze.crm_sales_details
