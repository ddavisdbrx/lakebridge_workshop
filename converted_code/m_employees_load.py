# Databricks notebook source
# Code converted on 2025-12-10 13:01:35
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from delta.tables import DeltaTable
from databricks_conversion_supplements import DatabricksConversionSupplements



# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script
# COMMAND ----------
# Variable_declaration_commentdbutils.widgets.text(name = "VAR100", defaultValue = 'ABCDE')
VAR100 = dbutils.widgets.get("VAR100")

dbutils.widgets.text(name = "VAR200", defaultValue = 200)
VAR200 = dbutils.widgets.get("VAR200")

# COMMAND ----------
# Processing node SQ_Employees, type SOURCE 
# COLUMN COUNT: 7
SQ_Employees = spark.sql(f"""SELECT
Employees.Emp_id,
Employees.first_name,
Employees.last_name,
Employees.hired_date,
Employees.last_upd_date,
Employees.salary,
Employees.dept_name
FROM Employees
WHERE Employees.hired_date > '2020-01-01'""").withColumn('sys_row_id',xxhash64(concat_ws('||', 'Emp_id','first_name','last_name','hired_date','last_upd_date','salary','dept_name')))
# Conforming fields names to the component layout
SQ_Employees_conformed_cols = ["Emp_id", "first_name", "last_name", "hired_date", "last_upd_date", "salary", "dept_name"]
SQ_Employees = DatabricksConversionSupplements.conform_df_columns(SQ_Employees,SQ_Employees_conformed_cols)


# COMMAND ----------
# Processing node filter_active_dept, type FILTER 
# COLUMN COUNT: 7
filter_active_dept = SQ_Employees.select( \
	SQ_Employees.Emp_id.alias('Emp_id'), \
	SQ_Employees.first_name.alias('first_name'), \
	SQ_Employees.last_name.alias('last_name'), \
	SQ_Employees.hired_date.alias('hired_date'), \
	SQ_Employees.last_upd_date.alias('last_upd_date'), \
	SQ_Employees.salary.alias('salary'), \
	SQ_Employees.dept_name.alias('dept_name')).filter(expr(f"""SUBSTR ( dept_name , 1 , 3 ) != \"000\" and first_name != '{'{VAR100}'}'""")).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node Exp_employee_attr, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7
SQ_Employees = SQ_Employees.withColumn("v_full_name", concat(col('first_name') , lit(" ") , col('last_name2')))

Exp_employee_attr = SQ_Employees.select( \
	SQ_Employees.sys_row_id.alias('sys_row_id'), \
	SQ_Employees.first_name.alias('first_name'), \
	SQ_Employees.last_name.alias('last_name2'), \
	SQ_Employees.hired_date.alias('hired_date')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	col('first_name'), \
	col('last_name2'), \
	(col('v_full_name')).alias('out_full_name'), \
	col('hired_date'), \
	(datediff(current_date() , col('hired_date'))).alias('out_days_worked'), \
	(lit('Y')).alias('active'), \
	(current_date()).alias('curr_dt') \
)


# COMMAND ----------
# Processing node filter_2, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3
filter_2 = filter_active_dept.select( \
	filter_active_dept.sys_row_id.alias('sys_row_id'), \
	filter_active_dept.dept_name.alias('dept_name_filtered'), \
	filter_active_dept.first_name.alias('first_name'), \
	filter_active_dept.last_name.alias('last_name')).filter(expr(f"""dept_name_filtered != \"ZZZ\" and last_name != \"YYY\"""")).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node AGGTRANS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2
AGGTRANS = filter_2.select( \
	filter_2.sys_row_id.alias('sys_row_id'), \
	filter_2.dept_name_filtered.alias('dpt_nm')) \
	.groupBy("dpt_nm") \
	.agg( \
	count(col('dpt_nm')).alias('out_count') \
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node Exp_2, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6
Exp_2 = Exp_employee_attr.select( \
	Exp_employee_attr.sys_row_id.alias('sys_row_id'), \
	Exp_employee_attr.out_full_name.alias('full_name2'), \
	Exp_employee_attr.out_days_worked.alias('days_worked'), \
	Exp_employee_attr.curr_dt.alias('curr_dt2'), \
	Exp_employee_attr.active.alias('active'), \
	Exp_employee_attr.first_name.alias('first_name')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	col('full_name2'), \
	col('days_worked'), \
	(lit('Y')).alias('active_flag'), \
	col('curr_dt2'), \
	col('active'), \
	col('first_name') \
)


# COMMAND ----------
# Processing node dim_party, type TARGET 
# COLUMN COUNT: 14
# Joining dataframes SQ_Employees, Exp_2 to form dim_party
dim_party_joined = SQ_Employees.join(Exp_2, SQ_Employees.sys_row_id == Exp_2.sys_row_id, 'inner')

dim_party = dim_party_joined.select( \
	lit(None).cast(LongType()).alias('PARTY_ID'), \
	Exp_2.full_name2.cast(StringType()).alias('FULL_NAME'), \
	SQ_Employees.hired_date.cast(TimestampType()).alias('dated_hired'), \
	Exp_2.days_worked.cast(LongType()).alias('days_employeed'), \
	Exp_2.curr_dt2.cast(TimestampType()).alias('created_ts'), \
	Exp_2.active_flag.cast(StringType()).alias('active_ind'), \
	lit(None).cast(StringType()).alias('location_name') \
)
dim_party.write.saveAsTable('dim_party', mode = 'append')

dim_party = dim_party_joined.select( \
	lit(None).cast(LongType()).alias('PARTY_ID'), \
	Exp_2.full_name2.cast(StringType()).alias('FULL_NAME'), \
	SQ_Employees.hired_date.cast(TimestampType()).alias('dated_hired'), \
	Exp_2.days_worked.cast(LongType()).alias('days_employeed'), \
	Exp_2.curr_dt2.cast(TimestampType()).alias('created_ts'), \
	Exp_2.active_flag.cast(StringType()).alias('active_ind'), \
	lit(None).cast(StringType()).alias('location_name') \
)
dim_party.write.saveAsTable('dim_party', mode = 'append')


# COMMAND ----------
# Processing node exp_finalize, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3
exp_finalize = AGGTRANS.select( \
	AGGTRANS.sys_row_id.alias('sys_row_id'), \
	AGGTRANS.dpt_nm.alias('department_nm'), \
	AGGTRANS.out_count.alias('COUNT_RENAMED_1')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	col('department_nm'), \
	col('COUNT_RENAMED_1'), \
	(md5(col('department_nm'))).alias('dept_hash') \
)


# COMMAND ----------
# Processing node dim_departments_INS, type TARGET 
# COLUMN COUNT: 3

dim_departments_INS = exp_finalize.select( \
	exp_finalize.dept_hash.cast(LongType()).alias('dept_id'), \
	exp_finalize.department_nm.cast(StringType()).alias('dept_name'), \
	exp_finalize.COUNT_RENAMED_1.cast(LongType()).alias('cnt') \
)
dim_departments_INS.write.saveAsTable('dim_departments', mode = 'append')

quit()
