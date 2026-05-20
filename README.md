# DHS-NFHS Analytics Pipeline

## Overview

**DHS-NFHS** is a Databricks-based health analytics system that processes National Family Health Survey (NFHS) data to analyze diabetes prevalence and treatment patterns across Indian states. The project identifies diagnosis gaps, tracks health metrics, and generates comprehensive dashboards and reports for health officials and researchers.

## Project Purpose

This project leverages survey data from the NFHS to:
- Analyze diabetes prevalence across different Indian states
- Identify gaps between measured and diagnosed diabetes cases
- Track treatment patterns and medication usage
- Generate demographic-specific health insights (age, sex, BMI, wealth, residence)
- Support evidence-based public health decision-making

## Architecture Overview

The project follows the **Medallion Architecture** (Bronze-Silver-Gold layers) on Databricks Lakehouse platform for progressive data refinement and analytics.

### Workflow Architecture

![Workflow Architecture](/Project%20Files/architecture_design.png)

The workflow processes raw NFHS survey data through three distinct layers:

1. **Bronze Layer** - Data Ingestion
   - Unzips NFHS survey data (ZIP format)
   - Extracts and converts DTA files to CSV format using chunking
   - Merges CSV chunks into single consolidated files
   - Converts CSVs to Delta Lake Tables

2. **Silver Layer** - Data Transformation & Cleaning
   - **Merging**: Combines data from multiple sources
   - **Cleaning**: Standardizes formats, handles missing values, data validation
   - **Anthropometry Scaling**: Normalizes health measurements (height, weight, BMI)
   - **Glucose Processing**: Cleans, validates, and standardizes glucose readings
   - **Diabetes Indicators**: Creates binary flags based on medical thresholds:
     - Fasting glucose ≥ 126 mg/dL = diabetic
     - Non-fasting glucose ≥ 200 mg/dL = diabetic
   - **Feature Engineering**: Builds composite health metrics and derived features

3. **Gold Layer** - Analytics & Business Intelligence
   - State-level health summaries and KPIs
   - Demographic-specific analyses (age, sex, BMI, lifestyle, wealth, residence)
   - Care cascade and treatment path analysis
   - Disease reversal probability modeling
   - Heatmaps and distribution visualizations
   - Composite health index by state

## Data Pipeline Execution

### Job Execution Flow

![Job Execution Flow](/Project%20Files/dhs-nfhs/Job_%20Orchestion.png)

The pipeline executes as a coordinated workflow:
- **cpr_a**: Unzips and extracts raw data (24s execution time)
- **cpr_b**: Converts DTA to CSV using chunking for large files (24s execution time)
- **cpr_c**: Merges CSV chunks into consolidated files (13s execution time)
- **BSG**: Generates state-level business summaries and gold layer analytics (5m 52s execution time)

All jobs run on Databricks Serverless infrastructure with distributed processing.

## Data Lineage

![Data Lineage DAG](/Project%20Files/dhs-nfhs/Medallion_BSG_Pipeline.png)

The data lineage shows the complete transformation journey:
- **Input Sources**: Individual Record (IR), Men Record (MR), Couple Record (PR) datasets
- **Bronze Layer**: Creates base delta tables (k_brz, mr_brz, pr_brz)
- **Silver Layer**: Progressively transforms data through cleaning, scaling, glucose processing, and diabetes indicator creation
- **Gold Layer**: Produces 15+ analytical tables for dashboards, analysis, and reporting

## Directory Structure

```
dhs-nfhs/
├── brz/                          # Bronze Layer - Data Ingestion
│   └── ba_delta_tables.py       # Creates bronze delta tables from CSVs
├── cpr/                          # Data Processing & Preparation
│   ├── ca_unzipping_vol.py      # Unzips NFHS ZIP files
│   ├── cb_dta2csv_chunking.py   # Converts DTA to CSV with chunking
│   └── cc_merging_chunks.py     # Merges CSV chunks
├── sil/                          # Silver Layer - Transformations
│   ├── sa_merging.py            # Merges data sources
│   ├── sb_cleaning.py           # Data cleaning and validation
│   ├── sc_anthropometry_scaling.py # Normalizes measurements
│   ├── sd_glucose_processing.py # Cleans glucose data
│   ├── se_diabetes_indicators.py # Creates diabetes flags
│   └── sf_feature_engineering.py # Builds derived features
└── gol/                          # Gold Layer - Analytics
    ├── dim_states.py            # Dimension table for states
    ├── ga_dashboard_kpi.py      # Main KPI metrics dashboard
    ├── gb_state_summary.py      # State-level summaries
    ├── gc_age_analysis.py       # Age-based analysis
    ├── gd_sex_analysis.py       # Sex/gender analysis
    ├── ge_bmi_analysis.py       # BMI-based analysis
    ├── gf_lifestyle_analysis.py # Lifestyle factors analysis
    ├── gg_residence_analysis.py # Urban/rural residence analysis
    ├── gh_wealth_analysis.py    # Wealth quintile analysis
    ├── gi_care_cascade.py       # Treatment pathway analysis
    ├── gj_reversal_probability.py # Diabetes reversal modeling
    ├── gk_heatmap_age_bmi.py    # Age-BMI intersection analysis
    ├── gl_glucose_distribution.py # Glucose level distributions
    └── gold_state_health_index.py # Composite state health index
```

## Key Metrics & Indicators

The system tracks and calculates:

- **Diabetes Prevalence**: Percentage of population with measured diabetes
- **Diagnosis Rate**: Percentage with reported/diagnosed diabetes
- **Diagnosis Gap**: Difference between measured and diagnosed cases (undiagnosed diabetes)
- **Medication Coverage**: Percentage of diabetics on treatment
- **Remission Rate**: Percentage achieving diabetes remission
- **State Health Index**: Composite metric combining multiple health factors
- **Demographic Breakdowns**: All metrics stratified by age, sex, BMI, wealth, and residence

## Technology Stack

- **Databricks**: Unified data and AI platform
- **PySpark**: Distributed data processing framework
- **Delta Lake**: ACID-compliant data lake format
- **SQL & Python**: Data transformation and analysis languages
- **Data Lineage Tracking**: Built-in Databricks workflow monitoring

## Data Processing Flow

1. **Raw Data** → NFHS survey ZIP files containing DTA (Stata) files
2. **Extraction** → Unzip and convert DTA to CSV format using chunking for efficiency
3. **Bronze** → Load raw CSVs into Delta tables
4. **Silver** → Clean, standardize, and enrich data with derived metrics
5. **Gold** → Create aggregated, analysis-ready datasets for dashboards and reports

## Use Cases

- Public health officials monitoring diabetes burden by state
- Epidemiologists studying disease prevalence patterns
- Policy makers identifying regions with high diagnosis gaps
- Healthcare planners allocating resources for diabetes management
- Researchers analyzing demographic risk factors for diabetes
- Dashboard users tracking KPIs and health trends

## Governance & Security

The platform includes:
- Unity Catalog for data governance
- Access controls for data security
- Compliance tracking for regulatory requirements
- Dashboards and alerts for monitoring and notifications

## Future Enhancements

Potential areas for expansion:
- Integration with real-time clinical data
- Predictive modeling for diabetes risk assessment
- Machine learning-based treatment recommendations
- Interactive dashboards for stakeholders
- Mobile app for field data collection

