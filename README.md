# Lakebridge Enablement

This repository contains sample scripts and files used as part of a hands-on **Lakebridge Migration Enablement**. The enablement is designed for both Databricks employees and customers to gain practical experience using [Lakebridge](https://databrickslabs.github.io/lakebridge/docs/overview/), Databricks' code migration toolkit.

## What is Lakebridge?

Lakebridge is a comprehensive toolkit designed to help you manage all phases of your SQL migration, from initially surveying your existing landscape through to translation of SQL and final data reconciliation.
Lakebridge provides three primary capabilities:

- **Pre-Migration Assessment** — Analyzes existing SQL workloads through profiling and code analysis. The profiler examines workload size, complexity, and features to estimate savings, while the analyzer identifies potential migration issues and effort required.
- **SQL Workload Conversion** — Provides multiple transpiler options: **Bladebridge** (deterministic transpiler handling multiple source dialects), **Morpheus** (next-generation with dbt support), and **Switch** (LLM-powered conversion for SQL and other formats using Mosaic AI Model Serving).
- **Post-Migration Reconciliation** — Ensures data transferred from source platforms to Databricks matches the original system, accounting for live environments.

## Enablement Overview

This enablement is a half-day, hands-on session where participants install Lakebridge, analyze sample workloads, and convert code using both deterministic and AI-based transpilers. By the end, participants will have practical experience with:

1. Installing and configuring Lakebridge via the Databricks CLI
2. Running the **Analyzer** to assess workload complexity
3. Converting code using the **Bladebridge** deterministic transpiler
4. Converting code using **Switch**, the LLM-powered transpiler
5. Customizing Switch with custom prompt configurations
6. Using the **Genie Code** to convert code

## Repository Structure

```
lakebridge_workshop/
├── sample_synapse/              # Sample Synapse SQL files (source code to convert)
│   ├── DimCustomer_Query.sql
│   ├── DimGeography_DDL.sql
│   ├── DimProduct_Query.sql
│   ├── DropTableSelectInto_Example.sql
│   ├── FactTransaction_Query.sql
│   ├── LoadEntityName_StoredProcedure.sql
│   └── TempTable_Example.sql
├── sample_informatica/          # Sample Informatica workflow (XML source)
│   └── wf_m_employees_load.XML
├── converted_code/              # Example converted output files
│   ├── DimCustomer_Query.sql
│   ├── DimGeography_DDL.sql
│   ├── DimProduct_Query.sql
│   ├── DropTableSelectInto_Example.sql
│   ├── FactTransaction_Query.sql
│   ├── LoadEntityName_StoredProcedure.sql
│   ├── TempTable_Example.sql
│   ├── databricks_conversion_supplements.py
│   ├── m_employees_load.py
│   ├── upload_job.sh
│   ├── wf_m_employees_load.json
│   └── wf_m_employees_load_params.py
├── custom_configs/              # Custom configuration examples
│   ├── bladebridge/             # Bladebridge custom config and handlers
│   │   ├── custom_config.json
│   │   └── temp_table_to_cte_handler.pm
│   └── switch/                  # Switch custom prompt and assistant instructions
│       ├── switch_custom_prompt.yml
│       └── .assistant_instructions.md
├── analyzer_report/             # Output directory for analyzer results
└── errors/                      # Output directory for error logs
```

## Prerequisites

Before starting, ensure the following are installed and configured:

### Databricks Environment

- Access to a **Databricks workspace** (production, development, or free trial)
- **Databricks CLI** installed and configured with either Personal Access Token (PAT) or Service Principal authentication — [Installation Instructions](https://docs.databricks.com/aws/en/dev-tools/cli/install#install-or-update-the-databricks-cli)
- A configured **cluster ID** in your Databricks CLI profile
- Verified connectivity via `databricks clusters list`

### Software Requirements

- **Python** 3.10.1 – 3.13.x (Python 3.14 is not currently supported)
- **Java** 11 or higher (OpenJDK 11+ recommended) — required for the Morpheus transpiler component

### Network Access

The installation environment must have internet connectivity to:

- [GitHub](https://github.com) (`github.com`, `raw.githubusercontent.com`)
- [Maven Central](https://central.sonatype.com) (`repo1.maven.org`, `central.sonatype.com`)
- [PyPI](https://pypi.org) (`pypi.org`, `files.pythonhosted.org`)

> **Note:** Organizations with firewall restrictions must whitelist these resources or configure private repository mirrors (e.g., Artifactory, Nexus) before installation.

### Additional Prerequisites for Switch (AI-Powered Conversion)

- [Databricks Foundation Models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) or [External Models in Mosaic AI Model Serving](https://docs.databricks.com/aws/en/generative-ai/external-models/#supported-models) with [custom model endpoints](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/create-manage-serving-endpoints)
- Serverless job compute (or classic compute with DBR 14.3 LTS+)
- Unity Catalog with a catalog, schema, and volume for state management
- SQL warehouse access (minimum `CAN_USE` permissions) for reconcile configuration

---

## Enablement Steps

To participate in the hands-on Lakebridge enablement using the files in this repository, please reach out to [dan.davis@databricks.com](mailto:dan.davis@databricks.com) for instructions and scheduling.

### Part 1: Installing Lakebridge and the CLI

#### 1.1 Review the Lakebridge Documentation

The Lakebridge documentation can be [found here](https://databrickslabs.github.io/lakebridge/docs/overview/).

#### 1.2 Ensure the Databricks CLI is Installed

The Databricks CLI is the primary prerequisite for using Lakebridge. To confirm if the CLI is installed, run:

```bash
databricks -v
```

Example output: `v0.278.0`

It is recommended to have the latest version of the CLI installed. Refer to the installation instructions provided for Linux, MacOS, and Windows, [available here](https://docs.databricks.com/aws/en/dev-tools/cli/install#install-or-update-the-databricks-cli).

#### 1.3 Configure Authentication

You will need your workspace host URL in order to authenticate to a workspace using the Databricks CLI. Run the command below when you have the workspace URL:

```bash
databricks auth login --host <workspace-url>
```

When prompted for the Databricks Profile Name, enter a profile name called **lakebridge** (all lowercase) so that you can reference it later:

```
✔ Databricks profile name [DEFAULT]: lakebridge
```

> **⚠️ Important:** Use the profile name `lakebridge` because all of the code snippets in the enablement use this profile name, making it easy to copy and paste commands.

After setting up the profile, you will be brought to a browser to authenticate. Once you have authenticated, test connectivity by running:

```bash
databricks clusters list --profile lakebridge
```

> **Note:** This may return a large list of clusters. Cancel the command after it starts returning results using `Ctrl + C`.

> **ℹ️** The profile name is used when you have references to multiple Databricks workspaces or service principals. For example, you might have a development workspace and a separate `PROD` profile for production. You can use `databricks auth profiles` to view all configured profiles.

#### 1.4 Verify your Python Version

Lakebridge requires Python 3.10 or higher, but less than Python 3.14. Check your version:

```bash
# If the first command fails, try the second one
python --version
python3 --version
```

> **⚠️** Ensure the version is ≥ 3.10.0 and < 3.14.0

#### 1.5 Install Lakebridge

Run the installer:

```bash
databricks labs install lakebridge --profile lakebridge
```

Confirm installation:

```bash
databricks labs lakebridge --help
```

---

### Part 2: Running the Analyzer

#### 2.1 Download and Prepare the Enablement Files

1. Download the [enablement files from this repository](https://github.com/ddavisdbrx/lakebridge_workshop)
   - Click **Code** and then **Download Zip** to download the repository
2. Go to your Downloads and extract the contents of the ZIP file
3. Rename the extracted folder to `lakebridge`
4. Move the entire folder to your **Documents** directory

> **⚠️ Important:** All code snippets in the enablement use relative paths based on `~/Documents/lakebridge`. Ensure the folder is in this location for easy copy-paste of commands.

You should see the following folders:
- `sample_synapse`
- `sample_informatica`
- `errors`
- `analyzer_report`
- `custom_configs`
- `converted_code`

#### 2.2 Set Up Your Command Line Session

1. Open the command line interface:
   - **MacOS/Linux:** Open Terminal
   - **Windows:** Open Command Prompt or PowerShell
2. Check your current working directory:

```bash
pwd
```

3. Change to the lakebridge folder:

```bash
cd ./Documents/lakebridge
```

4. Run `pwd` again to confirm you are inside the `lakebridge` directory.

#### 2.3 Run the Analyzer

Run the following command to analyze the sample Synapse source files:

```bash
databricks labs lakebridge analyze \
  --source-directory ./sample_synapse \
  --report-file ./analyzer_report/sample_analyzer_report.xlsx \
  --source-tech synapse
```

> **⚠️** If prompted, enter the number corresponding to **Synapse** as the source technology.

#### 2.4 Review the Analyzer Output

1. Open your file explorer and navigate to `./analyzer_report`
2. Open the generated Excel file (`sample_analyzer_report.xlsx`) to review:
   - **Job Complexity Assessment**
   - **Comprehensive Job Inventory**
   - **Cross-System Interdependency Mapping**

> **ℹ️** Learn more about complexity scoring in the [documentation](https://databrickslabs.github.io/lakebridge/docs/assessment/analyzer/complexity_scoring/).

> **Note:** Complexity scoring can vary between source technologies (e.g., Synapse, Teradata, Oracle, SQL Server). Lakebridge scoring logic is tailored to match the syntax and workload patterns of each system.

---

### Part 3: Running Lakebridge with the Bladebridge Converter

Lakebridge has deterministic transpilers used for converting code. This section walks through how to use Lakebridge with the Bladebridge transpiler.

#### 3.1 Authenticate to your Databricks Workspace

If you have not already, authenticate using the Databricks CLI:

```bash
databricks auth login --host <workspace-url>
```

Verify with:

```bash
databricks clusters list --profile lakebridge
```

#### 3.2 Install the Transpiler

```bash
databricks labs lakebridge install-transpile --profile lakebridge
```

> **Note:** You may see the following error — this is expected and can be safely ignored:
> ```
> ERROR [d.l.l.transpiler.installers] The morpheus transpiler requires Java 11 or above.
> ```
> ✅ We are not using the Morpheus transpiler.

Follow the interactive prompts:

| Prompt | Enter |
|--------|-------|
| Do you want to override the existing installation? | `yes` |
| Select the source dialect | `6` (Synapse) |
| Specify the config file to override the default [Bladebridge] config | Press `Enter` |
| Enter input SQL path (directory/file) | Press `Enter` |
| Enter output directory | Press `Enter` |
| Enter error file path | `/Users/<your-username>/Documents/lakebridge/errors/errors.log` |
| Would you like to validate the syntax and semantics of the transpiled queries? | Press `Enter` |
| Open config file in the browser? | `yes` |

Navigate to the Databricks Workspace UI and confirm the configuration was created under:
`/Workspace/Users/<your email address>/.lakebridge`

#### 3.3 Run the Transpiler

```bash
databricks labs lakebridge transpile \
  --profile lakebridge \
  --input-source ./sample_synapse \
  --output-folder ./converted_code
```

> **ℹ️ Optional: Validate converted code syntax**
>
> You can include the argument `--skip-validation false` if you configured a catalog, schema, and a cluster-id during installation. This enables the transpiler to automatically validate the syntax of the converted code. To add this, either re-install the transpiler or add the following parameters to your `config.yml` file in your workspace:
> ```yaml
> catalog_name: <catalog_name>
> schema_name: <schema_name>
> sdk_config:
>   warehouse_id: <sql_warehouse_id>
> ```

#### 3.4 Review the Converted Code

1. Navigate to the `./converted_code` folder on your local machine
2. Browse the generated files to review how Lakebridge has rewritten the SQL and logic
3. *(Optional)* Use a file comparison tool (e.g., VS Code, Beyond Compare, Meld, WinMerge) to inspect differences
4. Open both the original and converted versions of a sample file (e.g., `LoadEntityName_StoredProcedure.sql`)
5. Compare them side by side and observe:
   - How the code structure was transformed
   - Databricks-compatible patterns introduced
   - Function and syntax adjustments
   - Schema rewrites, naming normalizations, or temp table conversions

---

### Part 4: Running Lakebridge Switch (AI-Powered Conversion)

> **⚠️ Switch requires the following:**
> - [Databricks Foundation Models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) or [External Models in Mosaic AI Model Serving](https://docs.databricks.com/aws/en/generative-ai/external-models/#supported-models)
> - Serverless Jobs

Switch is a Lakebridge transpiler plugin that uses Large Language Models (LLMs) to convert SQL and other source formats into Databricks notebooks or generic files. Switch leverages Mosaic AI Model Serving to understand code intent and semantics, generating equivalent Python notebooks with Spark SQL or other target formats.

This LLM-powered approach excels at converting complex SQL code and business logic where context and intent matter more than syntactic transformation. While generated notebooks may require manual adjustments, they provide a valuable foundation for Databricks migration.

#### 4.1 Install the Transpiler

```bash
databricks labs lakebridge install-transpile --include-llm-transpiler true --profile lakebridge
```

> **Note:** You may see the Morpheus/Java warning — this is expected and can be safely ignored.

#### 4.2 Run the Transpiler

```bash
databricks labs lakebridge llm-transpile \
  --accept-terms=true \
  --input-source ./sample_synapse \
  --output-ws-folder /Workspace/Users/<your-email>/lakebridge/converted_code_switch \
  --source-dialect=synapse \
  --catalog-name <catalog_name> \
  --schema-name <schema_name> \
  --volume <volume_name> \
  --profile lakebridge
```

Follow the interactive prompts:
- **Prompt:** Select a Foundation Model serving endpoint → Enter: `0`
- Wait for the command to finish. Lakebridge will automatically create and start a job in your workspace.

#### 4.3 Review the Generated Job

1. In the Databricks Workspace UI, navigate to the job named **Lakebridge_Switch**
2. Wait for the job to complete (10–30 minutes)
3. Once complete, open the output folder you specified with `--output-ws-folder` — this is where all completed scripts will be located

Switch is intended to be customized based on your frequently used patterns. [Here is the documentation](https://databrickslabs.github.io/lakebridge/docs/transpile/pluggable_transpilers/switch/customizing_switch/) on how to customize with custom prompts.

#### 4.4 Upload Your Custom Prompt File

1. Locate the custom prompt file on your machine at `./custom_configs/switch/switch_custom_prompt.yml`
2. In your workspace, create a folder: `lakebridge_switch/custom_prompts` in your user directory
3. Upload the custom prompt file to: `/Workspace/Users/<your username>/lakebridge_switch/custom_prompts/`
4. Open the file and review it. Notice how it defines:
   - Table naming conventions (e.g., camelCase → snake_case)
   - Schema changes
   - Temp table handling
   - Few-shot examples
   - Additional conversion logic

#### 4.5 Update the Switch Config to Reference Your Custom Prompt

1. Navigate to the Switch config at: `/Workspace/Users/<your_user_name>/.lakebridge/switch/resources/switch_config.yml`
2. Open the config and add the file path to your custom prompt file:

```yaml
# Optional Parameters
conversion_prompt_yaml: /Workspace/Users/<your_user_name>/lakebridge_switch/custom_prompts/switch_custom_prompt.yml
```

#### 4.6 Run the Transpiler Again with Custom Prompts

```bash
databricks labs lakebridge llm-transpile \
  --accept-terms=true \
  --input-source ./sample_synapse \
  --output-ws-folder /Workspace/Users/<your-email>/lakebridge_switch/converted_code_switch \
  --source-dialect=synapse \
  --catalog-name <catalog_name> \
  --schema-name <schema_name> \
  --volume <volume_name> \
  --profile lakebridge
```

Follow the interactive prompts:
- **Prompt:** Select a Foundation Model serving endpoint → Enter: `0`
- Wait for the command to finish

#### 4.7 Review the Generated Job and Output

1. In the Databricks Workspace UI, navigate to the job named **Lakebridge_Switch**
2. Wait for the job to complete
3. Open the output folder you specified with `--output-ws-folder` to review the completed scripts

---

### Part 5: Use Genie Code to Convert Code

The [Genie Code](https://docs.databricks.com/aws/en/notebooks/databricks-assistant-faq) is an AI-based pair-programmer that can help you generate, optimize, complete, explain, and fix code and queries. It can also automatically translate your code into Databricks SQL dialect. You can improve accuracy and conversion quality by adding [custom instructions](https://docs.databricks.com/aws/en/notebooks/assistant-tips).

#### 5.1 Add User Instructions

1. Navigate to your Databricks workspace
2. Open the Assistant pane by clicking the Assistant icon in the upper-right corner
3. In the Assistant pane, click the settings icon to open Assistant settings
4. Under **User instructions**, click **Add instructions file** — this creates a `.assistant_instructions.md` file in your default user workspace directory
5. Copy the contents from the example instructions file located at `./custom_configs/switch/.assistant_instructions.md` in this repository
6. Paste the instructions into the file in your workspace

#### 5.2 Migrate Code Using Genie Code

1. Navigate to the **SQL Editor**
2. Copy and paste the following sample code into the editor:

```sql
-- Example Synapse SQL Script Using a Temp Table

-- 1. Create a temp table from FactInternetSales
SELECT
    fs.ProductKey,
    fs.OrderDateKey,
    fs.SalesOrderNumber,
    fs.OrderQuantity,
    fs.UnitPrice,
    fs.ExtendedAmount
INTO #TempSales
FROM [EDW].FactInternetSales fs
WHERE fs.OrderQuantity > 1;

-- 2. Enrich temp table with product details
SELECT
    ts.ProductKey,
    p.ProductAlternateKey,
    p.EnglishProductName,
    ts.OrderQuantity,
    ts.UnitPrice,
    ts.ExtendedAmount,
    p.Color,
    p.Size
INTO #TempSalesWithProduct
FROM #TempSales ts
JOIN dbo.DimProduct p
    ON ts.ProductKey = p.ProductKey;

-- 3. Use second temp table in a CTE
WITH SalesSummary AS (
    SELECT
        ProductKey,
        SUM(OrderQuantity) AS TotalQty,
        SUM(ExtendedAmount) AS TotalSales
    FROM #TempSalesWithProduct
    GROUP BY ProductKey
)

-- 4. Join CTE to DimProduct to produce final report
SELECT
    ss.ProductKey,
    p.EnglishProductName,
    ss.TotalQty,
    ss.TotalSales
FROM SalesSummary ss
JOIN dbo.DimProduct p
    ON ss.ProductKey = p.ProductKey
ORDER BY ss.TotalSales DESC;

-- 5. Cleanup
DROP TABLE IF EXISTS #TempSales;
DROP TABLE IF EXISTS #TempSalesWithProduct;
```

3. Open the Assistant by clicking the Assistant icon in the upper-right corner
4. With the code open in the SQL Editor, type `/migrate` into the Assistant and press Enter
5. The Assistant will generate code using your custom instructions that fits Databricks SQL syntax
6. Click **Accept** to replace the current contents with the generated code

> **Note:** Genie Code is non-deterministic, so you are not guaranteed the same response every time.

---

### Part 6 (Optional): Customize the Lakebridge Bladebridge Converter

> **⚠️** This section is not part of the scheduled enablement and is provided as an additional reference for customizing Lakebridge using the Bladebridge converter by extending the config file. See the [documentation here](https://databrickslabs.github.io/lakebridge/docs/transpile/pluggable_transpilers/bladebridge_configuration/).

#### 6.1 Extending the Config File

Based on your analyzer results and the most frequently used patterns in your code, you can customize your config file to add specific handlers.

1. **Open your Workspace Configuration:**
   - In the Databricks Workspace UI, navigate to: `/Workspace/Users/<your name>/.lakebridge`
   - Open `config.yml`
   - Review the key arguments: `transpiler_config_path` and `overrides_file`

2. **Add your custom config file** to `overrides_file`:

```yaml
transpiler_options:
  overrides-file: /Users/<user_name>/Documents/lakebridge/custom_configs/bladebridge/custom_config.json
```

3. **Update the `custom_config.json` file:**
   - On your local machine, navigate to: `./custom_configs/bladebridge/custom_config.json`
   - Open the file and update any hardcoded references:
     - Update the username in file paths
     - Update the Python version in the file path
     - Update the prefix of the `/Users` path if applicable
   - **Save the file when done**

> **ℹ️** In your custom configuration file, you need to specify that it inherits from the default configuration. This enables layered rule definitions and promotes reuse. The default configuration files are stored at:
> ```
> <user_home_directory>/.databricks/labs/remorph-transpilers/bladebridge/lib/.venv/lib/<python_version>/site-packages/databricks/labs/bladebridge/Converter/Configs/
> ```
> The `inherit_from` field is an array pointing to JSON filenames that the current file inherits from.

#### 6.2 Review the Custom Config File

Navigate to `./custom_configs/bladebridge/custom_config.json` and inspect the customized rules:
- Transformations
- Override patterns
- Inherited patterns
- Custom converter modules

#### 6.3 Review the `temp_table_to_cte_handler.pm` File

Open the file and review the Perl logic that handles common SQL-to-Databricks conversions, such as:

**Before (Synapse SQL):**
```sql
DROP TABLE IF EXISTS dbo.SalesSummary;
SELECT
    ProductKey,
    SUM(OrderQuantity) AS TotalQty,
    SUM(ExtendedAmount) AS TotalSales,
    GETUTCDATE() AS CurrentTimestamp
INTO dbo.SalesSummary
FROM dbo.FactInternetSales
GROUP BY ProductKey;
```

**After (Databricks SQL):**
```sql
CREATE OR REPLACE TABLE dbo.sales_summary AS
SELECT
    ProductKey,
    SUM(OrderQuantity) AS TotalQty,
    SUM(ExtendedAmount) AS TotalSales,
    current_timestamp() AS Current_Timestamp
FROM dbo.fact_internet_sales
GROUP BY ProductKey;
```

The handler also converts:
- `DROP TABLE` / `TRUNCATE TABLE` into Databricks-friendly equivalents
- Temp tables into CTEs or temp views
- Custom syntax transformations

#### 6.4 Run the Transpiler with Custom Configs

```bash
databricks labs lakebridge transpile \
  --profile lakebridge \
  --input-source ./sample_synapse \
  --source-dialect synapse \
  --output-folder ./converted_code
```

#### 6.5 Review the Converted Code

1. Navigate to the `./converted_code` folder
2. Browse the generated files to review how Lakebridge has rewritten the SQL and logic
3. *(Optional)* Use a file comparison tool to inspect differences
4. Open both the original and converted versions of a sample file (e.g., `TempTable_Example.sql`)
5. Compare them side by side and observe:
   - How the code structure was transformed, including DROP statements and temp tables
   - Databricks-compatible patterns introduced
   - Function and syntax adjustments
   - Schema rewrites, naming normalizations, or temp table conversions

---

## Resources

- [Lakebridge Documentation](https://databrickslabs.github.io/lakebridge/docs/overview/)
- [Lakebridge GitHub Repository](https://github.com/databrickslabs/lakebridge)
- [Lakebridge Switch (LLM Transpiler) Documentation](https://databrickslabs.github.io/lakebridge/docs/transpile/pluggable_transpilers/switch/customizing_switch/)
- [Bladebridge Configuration Documentation](https://databrickslabs.github.io/lakebridge/docs/transpile/pluggable_transpilers/bladebridge_configuration/)
- [Complexity Scoring Documentation](https://databrickslabs.github.io/lakebridge/docs/assessment/analyzer/complexity_scoring/)
- [Databricks CLI Installation](https://docs.databricks.com/aws/en/dev-tools/cli/install#install-or-update-the-databricks-cli)
- [Genie Code](https://docs.databricks.com/aws/en/notebooks/databricks-assistant-faq)
- [Genie Code Custom Instructions](https://docs.databricks.com/aws/en/notebooks/assistant-tips)
