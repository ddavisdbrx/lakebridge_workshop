# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name = '$Start.StartTime', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.StartTime', value = dbutils.widgets.get("$Start.StartTime"))


dbutils.widgets.text(name = '$Start.EndTime', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.EndTime', value = dbutils.widgets.get("$Start.EndTime"))


dbutils.widgets.text(name = '$Start.Status', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.Status', value = dbutils.widgets.get("$Start.Status"))


dbutils.widgets.text(name = '$Start.PrevTaskStatus', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.PrevTaskStatus', value = dbutils.widgets.get("$Start.PrevTaskStatus"))


dbutils.widgets.text(name = '$Start.ErrorCode', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.ErrorCode', value = dbutils.widgets.get("$Start.ErrorCode"))


dbutils.widgets.text(name = '$Start.ErrorMsg', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$Start.ErrorMsg', value = dbutils.widgets.get("$Start.ErrorMsg"))


dbutils.widgets.text(name = '$s_m_employees_load.StartTime', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.StartTime', value = dbutils.widgets.get("$s_m_employees_load.StartTime"))


dbutils.widgets.text(name = '$s_m_employees_load.EndTime', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.EndTime', value = dbutils.widgets.get("$s_m_employees_load.EndTime"))


dbutils.widgets.text(name = '$s_m_employees_load.Status', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.Status', value = dbutils.widgets.get("$s_m_employees_load.Status"))


dbutils.widgets.text(name = '$s_m_employees_load.PrevTaskStatus', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.PrevTaskStatus', value = dbutils.widgets.get("$s_m_employees_load.PrevTaskStatus"))


dbutils.widgets.text(name = '$s_m_employees_load.ErrorCode', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.ErrorCode', value = dbutils.widgets.get("$s_m_employees_load.ErrorCode"))


dbutils.widgets.text(name = '$s_m_employees_load.ErrorMsg', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.ErrorMsg', value = dbutils.widgets.get("$s_m_employees_load.ErrorMsg"))


dbutils.widgets.text(name = '$s_m_employees_load.SrcSuccessRows', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.SrcSuccessRows', value = dbutils.widgets.get("$s_m_employees_load.SrcSuccessRows"))


dbutils.widgets.text(name = '$s_m_employees_load.SrcFailedRows', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.SrcFailedRows', value = dbutils.widgets.get("$s_m_employees_load.SrcFailedRows"))


dbutils.widgets.text(name = '$s_m_employees_load.TgtSuccessRows', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.TgtSuccessRows', value = dbutils.widgets.get("$s_m_employees_load.TgtSuccessRows"))


dbutils.widgets.text(name = '$s_m_employees_load.TgtFailedRows', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.TgtFailedRows', value = dbutils.widgets.get("$s_m_employees_load.TgtFailedRows"))


dbutils.widgets.text(name = '$s_m_employees_load.TotalTransErrors', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.TotalTransErrors', value = dbutils.widgets.get("$s_m_employees_load.TotalTransErrors"))


dbutils.widgets.text(name = '$s_m_employees_load.FirstErrorCode', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.FirstErrorCode', value = dbutils.widgets.get("$s_m_employees_load.FirstErrorCode"))


dbutils.widgets.text(name = '$s_m_employees_load.FirstErrorMsg', defaultValue = '')
dbutils.jobs.taskValues.set(key = '$s_m_employees_load.FirstErrorMsg', value = dbutils.widgets.get("$s_m_employees_load.FirstErrorMsg"))

