for file in *.json; do
  echo "Creating job from $file ..."

  # Replace all placeholders in-memory
  json_payload=$(sed \
      -e 's/%USER_NAME%/dan.davis@databricks.com/g' \
      -e 's/%FAILURE_EMAIL_ADDRESS%/dan.davis@databricks.com/g' \
      -e 's/%SUCCESS_EMAIL_ADDRESS%/dan.davis@databricks.com/g' \
      -e 's/%EMAIL_ADDRESS%/dan.davis@databricks.com/g' \
      "$file")

  # Create Databricks job
  databricks jobs create --json "$json_payload"
done
