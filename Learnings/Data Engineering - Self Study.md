
# Data Engineering – Self Study

## Table of Contents

1. [Omnia Learning](#omnia-learning)
2. [Houston Integration with SQL Developer](#houston-integration-with-sql-developer)
3. [src/main/resources](#srcmainresources)
4. [src/main/run](#srcmainrun)
5. [Omnia to Cloud (OTC) Migration](#omnia-to-cloud-otc-migration)
6. [Execution Flow of Repository](#execution-flow-of-repository)
7. [Debugging Workflow](#debugging-workflow)
8. [Complete Debugging and Troubleshooting Guide](#complete-debugging-and-troubleshooting-guide)
9. [TeamCity - Continuous Integration and Deployment](#teamcity---continuous-integration-and-deployment)
10. [JFrog Artifactory & RPM Package Generation](#jfrog-artifactory--rpm-package-generation)
11. [SBT (Simple Build Tool)](#sbt-simple-build-tool)
12. [Houston - CBA's Enterprise Job Management](#houston---cbas-enterprise-job-management)
13. [Stream Key in Omnia Environment](#stream-key-in-omnia-environment)
14. [GDW (Group Data Warehouse)](#gdw-group-data-warehouse)
15. [Autosys - Complete Interview Preparation Guide](#autosys---complete-interview-preparation-guide)
16. [XLR (XL Release)](#xlr-xl-release)
17. [OPS LOGICAL](#ops-logical)
18. [OBL Files](#obl-files)
19. [Platform Overview](#platform-overview)

---

## Omnia Learning

Omnia is Commonwealth Bank of Australia's (CBA) enterprise-scale big data platform - a comprehensive data processing and analytics ecosystem built on modern distributed computing technologies. Think of Omnia as CBA's "data factory" that ingests, processes, transforms, and serves data across the entire organization.

### Key Features

• Centralized data platform with consistent processing
• Automated data pipelines running 24/7
• Self-service analytics for business users
• Real-time and batch processing capabilities
• Regulatory compliance automation
• Scalable infrastructure supporting petabytes of data

# 🔍 Comprehensive Repository Comparison (Continued)

| **Comparison Aspect** | **etl.oal-ifrs9** | **etl.oal-airb** | **etl.ifrs9-export** |
|----------------------|-------------------|------------------|----------------------|
| **🔗 DEPENDENCIES** | **Multiple Upstream Systems** - Complex dependencies on SAP, HLS, FARLO, and other source systems | **OAL Tables** - Dependent on successful completion of etl.oal-ifrs9 processing | **OAL Risk Tables** - Dependent on risk attribute tables from OAL layer |
| **🚨 PRIORITY LEVEL** | **P1-P2 (Critical)** - Mission-critical for financial reporting and regulatory compliance | **P3 (Standard)** - Standard priority for regulatory export processes | **P3 (Standard)** - Standard priority for external vendor integration |
| **📅 SLA REQUIREMENTS** | **Strict (Regulatory Deadlines)** - Tight deadlines for month-end financial reporting and regulatory submissions | **Standard** - Monthly regulatory reporting windows with standard SLAs | **Standard** - Monthly delivery schedules aligned with vendor requirements |
| **🏦 BANKING PRODUCTS** | **All Products (CC, HL, PL, VLOC, POD, BNPL)** - Comprehensive coverage of all lending products with full processing | **Core Products (CC, HL, PL, VLOC, POD)** - Risk export for main lending products excluding BNPL | **Core Products (CC, HL, PL, VLOC)** - CommScore export for primary risk modeling products |
| **📋 DATA ATTRIBUTES** | **Comprehensive** - Full account details, risk calculations, financial metrics, and regulatory fields | **Summary Level** - Export-ready risk indicators and formatted financial metrics | **Key Identifiers** - Essential risk attributes formatted for external vendor consumption |
| **🔧 TECHNICAL SKILLS** | **Advanced SQL, YAML, Shell Scripting** - Complex joins, CTEs, window functions, and advanced data processing | **Intermediate SQL, YAML, Shell Scripting** - Standard SQL with formatting focus and export preparation | **Scala, SQL, JSON Configuration** - Functional programming, data extraction, and security protocols |
| **📚 BUSINESS KNOWLEDGE** | **Deep IFRS9, Banking Products, Risk Models** - Comprehensive understanding of accounting standards and risk management | **Basel III AIRB, Risk-Weighted Assets** - Regulatory capital and prudential requirements knowledge | **Vendor Requirements, External Modeling** - Understanding of external partnerships and model validation |
| **🎓 LEARNING COMPLEXITY** | **High (⭐⭐⭐⭐⭐)** - Complex business logic, regulatory requirements, and technical implementation | **Medium (⭐⭐⭐)** - Standard export processes with regulatory format requirements | **Medium-High (⭐⭐⭐⭐)** - Scala programming, security protocols, and vendor integration |
| **💰 BUSINESS VALUE** | **Very High (⭐⭐⭐⭐⭐)** - Critical for financial reporting, regulatory compliance, and business operations | **High (⭐⭐⭐⭐)** - Important for regulatory capital management and compliance reporting | **Medium-High (⭐⭐⭐)** - Valuable for advanced modeling and external validation capabilities |
| **🚀 CAREER GROWTH** | **Very High (⭐⭐⭐⭐⭐)** - Comprehensive skills in banking, regulation, and complex data engineering | **High (⭐⭐⭐⭐)** - Regulatory expertise and export system management | **High (⭐⭐⭐⭐)** - Advanced programming skills and external integration experience |
| **🔒 SECURITY LEVEL** | **High (Internal)** - Standard Omnia security with internal data protection protocols | **High (Regulatory)** - Regulatory-grade security for prudential reporting requirements | **Very High (External)** - Enhanced security with 7Z encryption for external vendor data transfer |
| **📊 MONITORING TOOLS** | **Houston, TeamCity, Comprehensive Dashboards** - Extensive monitoring with detailed performance metrics | **Houston, Autosys, Standard Monitoring** - Standard monitoring for export processes and job scheduling | **Houston, Load Assurance, Security Monitoring** - Focused monitoring for file generation and security validation |
| **🎯 SUCCESS METRICS** | **Data Quality, Processing Time, Business Rule Compliance** - Comprehensive metrics for accuracy and performance | **Export Success Rate, Format Compliance, Timeliness** - Standard metrics for export processes | **File Generation Success, Encryption Validation, Delivery Confirmation** - Security and delivery-focused metrics |

---

# 📂 Repository Structure Analysis

## etl.oal-ifrs9

```
etl.oal-ifrs9/
├── .gitignore                 # Git ignore patterns
├── build.sbt                  # SBT build configuration (Scala Build Tool)
├── README.md                  # Repository documentation
├── sbt                        # SBT launcher script
├── version.sbt                # Version management
├── project/                   # SBT project configuration
├── src/                       # Source code and configurations
    ├── main/autosys/
    ├── main/hive/
    ├── main/houston/
    ├── main/resources/
    ├── main/run/
```

## ROOT LEVEL FILES:

### **build.sbt**
- Scala Build Tool configuration file
- Defines project dependencies, Scala version, and build settings
- Specifies JAR packaging and library versions
- Critical for compiling the Spark application

### **version.sbt**
- Contains the current version of the application
- Used for deployment tracking and rollback capabilities
- Format: version := "5.43.3-SNAPSHOT"

### **README.md**
- Project documentation and setup instructions
- Contains deployment procedures and troubleshooting guides
- Essential for new team members and maintenance

### **.gitignore**
- Specifies files and directories to exclude from version control
- Prevents sensitive configuration and build artifacts from being committed
- Includes IDE files, logs, and temporary directories

### **sbt**
- SBT launcher script for building the project
- Ensures consistent build environment across different machines
- Used by CI/CD pipelines for automated builds

## SRC/MAIN DIRECTORY STRUCTURE:

### AUTOSYS FOLDER - JOB SCHEDULING DEFINITIONS:

**What is Autosys?**
Autosys is an enterprise job scheduling system that manages when and how your ETL jobs run.

**JIL Files Explained:**
JIL (Job Information Language) files define job schedules, dependencies, and execution parameters.

**File Naming Convention:**
- Numbers (8267, 8268, etc.): Autosys job IDs
- _insert.jil: Creates new job definition
- _delete.jil: Removes existing job definition
- _rollback.jil: Reverts to previous job version

**Example JIL File Analysis:**
```jil
/* Job: au_cba_hado_p01_16058_mlt_enr_oal_omn_mth_tfm_cmd */
insert_job: au_cba_hado_p01_16058_mlt_enr_oal_omn_mth_tfm_cmd
job_type: CMD
command: ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/vloc_pipeline.sh
machine: hado-omnia-p01
owner: omnia_prod
permission: gx,wx
date_conditions: 1
days_of_week: mo,tu,we,th,fr,sa,su
start_times: "20:30"
condition: s(au_cba_hado_p01_upstream_job_success)
alarm_if_fail: 1
```

**JIL Components Explained:**
- **insert_job**: Unique job identifier
- **job_type: CMD**: Command execution job
- **command**: Shell script to execute
- **machine**: Target server for execution
- **owner**: User account for job execution
- **date_conditions**: Calendar-based scheduling
- **start_times**: When job should start
- **condition**: Dependencies on other jobs
- **alarm_if_fail**: Send alerts on failure

### HIVE FOLDER - DATABASE SCHEMA DEFINITIONS:

**What is Hive?**
Apache Hive provides SQL-like interface to query data stored in HDFS. It acts as a data warehouse infrastructure.

**DDL Files (Data Definition Language):**
These files create table structures in Hive.

**Key DDL Files:**
- **bv_rbs_vloc_acct_modl_inpt_mnly_ddl.sql**: Your main VLOC table
- **rbs_home_loan_risk_ddl.sql**: Home loan risk tables
- **rbs_prsl_loan_risk_ddl.sql**: Personal loan risk tables
- **rbs_cr_card_risk_ddl.sql**: Credit card risk tables

**Shell Scripts in Hive Folder:**
- **create_rbs_risk_tbls.sh**: Batch creation of all risk tables
- **PM0049380.sh**: Specific deployment script for production

**CSV Files:**
- **map_l2_risk_acct_feat_derv_rul_*.csv**: Feature derivation rules
- These define business logic for risk calculations
- Different versions for different time periods

**History Correction Folder:**
Contains DDL files for fixing historical data issues:
- **rbs_home_loan_risk_bugfix.sql**: Fixes for home loan data
- **rbs_prsl_loan_risk_history_fixed.sql**: Personal loan corrections

### HOUSTON FOLDER - STREAM PROCESSING ORCHESTRATION:

**What is Houston?**
Houston is CBA's internal framework for managing data streams and dependencies between different data processing jobs.

**Houston Components:**

#### **deploy-stream-dependency.sh**
- Registers dependencies between data streams
- Ensures upstream data is available before processing
- Critical for data consistency and timing

#### **deploy-stream-register.sh**
- Registers new data streams in Houston catalog
- Makes streams discoverable by downstream consumers
- Enables data lineage tracking

#### **houston-deploy.sh**
- Main deployment script for Houston configurations
- Coordinates stream registration and dependency setup
- Used during production deployments

#### **stream-register.sh**
- Individual stream registration utility
- Used for adding new streams or updating existing ones
- Supports both batch and streaming data sources

**Why Houston is Critical:**
- **Data Lineage**: Tracks where data comes from and goes to
- **Dependency Management**: Ensures correct processing order
- **Stream Discovery**: Enables teams to find and use data streams
- **Quality Monitoring**: Tracks data quality metrics across streams

### RESOURCES FOLDER - CONFIGURATION AND SQL:

**YAML Configuration Files:**
Each YAML file defines a complete ETL pipeline configuration.

**Key YAML Files:**
- **bv_rbs_vloc_acct_modl_inpt_mnly_ongoing.yaml**: Your main VLOC pipeline
- **cc_features.yaml**: Credit card feature engineering
- **hl_al_ongoing.yaml**: Home loan ongoing processing
- **pl_ongoing.yaml**: Personal loan ongoing processing

**SQL Subfolder Structure:**
The SQL folder contains business logic organized by product:

- **vloc_modl_input/**: VLOC-specific SQL transformations
- **hl/**: Home loan transformations
- **pl/**: Personal loan transformations
- **cc/**: Credit card transformations
- **commons/**: Shared utility functions
- **history_correction/**: Data correction scripts

**SQL File Organization:**
Each product folder contains:
- **initload.sql**: Initial data loading logic
- **ongoing.sql**: Regular processing logic
- **feature_*.sql**: Specific feature calculations
- **validation.sql**: Data quality checks

### RUN FOLDER - EXECUTION SCRIPTS:

**Shell Scripts by Category:**

#### **Main Pipeline Scripts:**
- **bv_csc_hl_acct_airb_modl_oal.sh**: Home loan AIRB model
- **csc_bnpl_enr_oal.sh**: BNPL enrichment
- **csc_vloc_sas_omn_oal.sh**: VLOC SAS processing
- **ifrs9_vloc_omnia_to_sas.sh**: VLOC to SAS export

#### **Utility Scripts:**
- **history_catchup.sh**: Processes historical data gaps
- **history_load.sh**: Loads historical data for new implementations
- **hl_load_assurance_python.sh**: Data quality validation

#### **Configuration Files:**
- **log4j.properties**: Logging configuration
- Controls log levels, output formats, and destinations
- Critical for debugging and monitoring

**CONF Subfolder:**
Contains environment-specific configurations:
- **dev.conf**: Development environment settings
- **test.conf**: Test environment settings
- **prod.conf**: Production environment settings
- **badev.conf**: Business Acceptance environment

**Product-Specific Subfolders:**
- **cc-file-to-sas/**: Credit card to SAS processing
- **hl-file-to-sas/**: Home loan to SAS processing
- **pl-file-to-sas/**: Personal loan to SAS processing
- **oal-to-obl/**: OAL to OBL data movement
- **vloc-file-to-sas/**: VLOC to SAS processing

Each subfolder contains:
- Processing scripts for that product
- Custom data watchers
- Product-specific configurations

## UNDERSTANDING THE DATA FLOW ARCHITECTURE:

### **Data Layer Architecture:**

#### **ENR (Enriched Layer):**
- Raw data from source systems
- Basic cleansing and standardization
- Source: SAP, OSCA, MEP systems

#### **OAL (Omnia Access Layer):**
- Business logic applied
- Feature engineering
- Risk calculations
- Your VLOC pipeline outputs here

#### **OBL (Omnia Business Layer):**
- Aggregated business views
- Reporting-ready data
- Regulatory reporting formats

#### **SAS Layer:**
- Statistical analysis and modeling
- IFRS9 model execution
- Provision calculations

### **Data Movement Patterns:**
- **ENR → OAL:** Your main pipeline (Spark processing)
- **OAL → OBL:** Aggregation and business views
- **OAL → SAS:** Model input preparation
- **SAS → OAL:** Model results integration

## DEPLOYMENT AND BUILD PROCESS:

### **SBT Build Process:**
1. **Compile**: Scala code compilation
2. **Test**: Unit and integration tests
3. **Package**: JAR file creation
4. **Deploy**: Artifact deployment to cluster

### **Build Artifacts:**
- **etl-oal-ifrs9_2.12-5.43.3-thin.jar**: Main application
- **etl-oal-ifrs9_2.12-5.43.3-deps.jar**: Dependencies

### **Deployment Flow:**
1. Code commit to Git repository
2. TeamCity build trigger
3. SBT compilation and testing
4. JAR packaging
5. RPM package creation
6. Deployment to target environment
7. Autosys job updates
8. Houston stream registration

## CONFIGURATION MANAGEMENT:

### **Environment-Specific Configurations:**

#### **Development (dev.conf):**
- Small data samples
- Reduced resource allocation
- Debug logging enabled
- Fast feedback cycles

#### **Test (test.conf):**
- Production-like data volumes
- Full validation enabled
- Performance testing
- Integration testing

#### **Production (prod.conf):**
- Full data processing
- Optimized resource allocation
- Error logging only
- High availability configuration

### **Configuration Hierarchy:**
1. Default values in YAML
2. Environment-specific overrides in .conf files
3. Runtime parameters from shell scripts
4. System environment variables

## MONITORING AND LOGGING:

### **Log4j Configuration:**
```properties
log4j.rootLogger=INFO, stdout, file
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=/var/log/etl-oal-ifrs9/application.log
```

### **Logging Levels:**
- **ERROR**: System failures and critical issues
- **WARN**: Potential problems and data quality issues
- **INFO**: Normal processing information
- **DEBUG**: Detailed execution information

### **Monitoring Components:**
- **Autosys Monitoring**: Job execution status
- **Spark UI**: Application performance metrics
- **HDFS Monitoring**: Storage utilization
- **Houston Monitoring**: Stream health and dependencies

## SECURITY AND ACCESS CONTROL:

### **File Permissions:**
- Scripts: 755 (executable by owner, readable by group)
- Configuration: 644 (readable by owner and group)
- Logs: 640 (readable by owner, writable by group)

### **User Accounts:**
- **omnia_prod**: Production job execution
- **omnia_dev**: Development environment
- **omnia_test**: Test environment

### **Data Security:**
- Kerberos authentication for Hadoop access
- Encrypted data transmission
- Row-level security in Hive tables
- Audit logging for data access

## OVERVIEW - THE BIG PICTURE:

When you run your VLOC pipeline, it's like conducting an orchestra where multiple systems work together in perfect harmony. Let me explain exactly what happens from the moment you press enter until your data appears in the target table.

## REAL WORLD ANALOGY:

Think of your pipeline like a restaurant preparing a complex meal:
- Autosys is the head waiter who takes the order and schedules everything
- Houston is the restaurant manager who coordinates resources
- Spark is the kitchen with multiple chefs working simultaneously
- Hive is the dining room where the final meal is served
- HDFS is the pantry where all ingredients are stored

### Data Layers and Processing

Omnia implements a medallion architecture with clearly defined data layers, each serving specific purposes:

1. **DIL (Data Ingestion Layer)**: Raw data landing zone with minimal processing
2. **ENR (Enriched Layer)**: Cleaned and standardized data with basic business rules applied
3. **OAL (Operational Access Layer)**: Business-ready data with complex business logic and risk calculations
4. **OBL (Operational Business Layer)**: Aggregated business views optimized for reporting

## Houston Integration with SQL Developer

### Why SQL Developer is Used

**1. DIRECT DATABASE ACCESS**
```sql
-- Operations teams query Houston tables directly
SELECT ETL_ID, BUSINESS_DATE, STATUS, START_TIME, END_TIME
FROM HOUSTON.LOAD_RUNS
WHERE ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND BUSINESS_DATE >= '2024-06-01'
ORDER BY BUSINESS_DATE DESC;
```

**2. MANUAL INTERVENTIONS**
```sql
-- Reset failed job for retry
UPDATE HOUSTON.LOAD_RUNS
SET STATUS = 'NOT STARTED'
WHERE ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND BUSINESS_DATE = '2024-06-30'
  AND STATUS = 'FAILED';
```

**3. OPERATIONAL QUERIES**
```sql
-- Check all failed jobs for a date
SELECT ETL_ID, START_TIME, END_TIME
FROM HOUSTON.LOAD_RUNS
WHERE BUSINESS_DATE = '2024-06-30'
  AND STATUS = 'FAILED'
ORDER BY START_TIME;

-- Get job execution metrics
SELECT METRIC_TYPE, OBSERVED, EXPECTED, STATUS
FROM HOUSTON.LOAD_METRICS m
JOIN HOUSTON.LOAD_RUNS r ON m.RUN_ID = r.RUN_ID
WHERE r.ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND r.BUSINESS_DATE = '2024-06-30';
```

**4. TROUBLESHOOTING SUPPORT**
- Real-time job status monitoring
- Historical performance analysis
- Dependency chain investigation
- Data quality issue diagnosis

### Houston Data Flow for VLOC Pipeline - Actual Implementation

**COMPLETE HOUSTON FLOW:**

### Houston Dependency Management - Actual Stream Dependencies

**DEPENDENCY CHAIN FOR VLOC (from deploy-stream-dependency.sh):**
```bash
# Your actual dependency registrations
./stream-register.sh "${ENV}" CREATE_DEPENDENCY 5071 15625 #OMN_BV_RBS_ACCT_LVR_SUMY_TRANSFORM_OAL
./stream-register.sh "${ENV}" CREATE_DEPENDENCY 5071 13304 #PCC source tables
./stream-register.sh "${ENV}" CREATE_DEPENDENCY 5071 11311 #CMS_ASSET_TRANSFORM_ENR
```

**Houston Dependency Checks:**
```bash
# Before VLOC job starts, Houston checks these actual dependencies
curl "${HOUSTON_URL}/houston/api/v2/streams/15625/status"  # Account LVR Summary ready?
curl "${HOUSTON_URL}/houston/api/v2/streams/13304/status"  # PCC source tables ready?
curl "${HOUSTON_URL}/houston/api/v2/streams/11311/status"  # CMS Asset Transform ready?

# Only if all upstream jobs are SUCCESS, VLOC job can start
```

### Houston Troubleshooting and Operations - Actual Queries

**COMMON HOUSTON OPERATIONS:**

**1. Check Job Status:**
```sql
-- In SQL Developer - Query your actual VLOC job
SELECT 
    lr.ETL_ID,
    lr.BUSINESS_DATE,
    lr.STATUS,
    lr.START_TIME,
    lr.END_TIME,
    ROUND((lr.END_TIME - lr.START_TIME) * 24 * 60, 2) as DURATION_MINUTES
FROM HOUSTON.LOAD_RUNS lr
WHERE lr.ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND lr.BUSINESS_DATE >= TRUNC(SYSDATE) - 30
ORDER BY lr.BUSINESS_DATE DESC;
```

**2. Reset Failed Job:**
```sql
-- Reset your VLOC job for retry
UPDATE HOUSTON.LOAD_RUNS
SET STATUS = 'NOT STARTED',
    START_TIME = NULL,
    END_TIME = NULL
WHERE ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND BUSINESS_DATE = TO_DATE('2024-06-30', 'YYYY-MM-DD')
  AND STATUS = 'FAILED';
COMMIT;
```

**3. Check Dependencies:**
```sql
-- Find upstream dependencies for your VLOC stream 17998
SELECT 
    sd.PARENT_STREAM_KEY,
    s.STREAM_NAME as PARENT_STREAM_NAME,
    lr.STATUS as PARENT_STATUS
FROM HOUSTON.STREAM_DEPENDENCIES sd
JOIN HOUSTON.STREAMS s ON sd.PARENT_STREAM_KEY = s.STREAM_KEY
JOIN HOUSTON.LOAD_RUNS lr ON s.ETL_ID = lr.ETL_ID
WHERE sd.STREAM_KEY = 17998  -- Your VLOC stream
  AND lr.BUSINESS_DATE = TO_DATE('2024-06-30', 'YYYY-MM-DD');
```

**4. Monitor Performance:**
```sql
-- Job performance over time for your VLOC job
SELECT 
    BUSINESS_DATE,
    ROUND((END_TIME - START_TIME) * 24 * 60, 2) as DURATION_MINUTES,
    CASE 
        WHEN (END_TIME - START_TIME) * 24 * 60 > 120 THEN 'SLOW'
        WHEN (END_TIME - START_TIME) * 24 * 60 > 90 THEN 'NORMAL'
        ELSE 'FAST'
    END as PERFORMANCE_CATEGORY
FROM HOUSTON.LOAD_RUNS
WHERE ETL_ID = 'OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL'
  AND STATUS = 'SUCCESS'
  AND BUSINESS_DATE >= TRUNC(SYSDATE) - 90
ORDER BY BUSINESS_DATE DESC;
```

### Houston Best Practices for Your VLOC Project - Actual Implementation

**1. ETL_ID NAMING CONVENTION (from your actual jobs):**
```
Pattern: {SYSTEM}_{PRODUCT}_{LAYER}_{OPERATION}_{FREQUENCY}_{LAYER}
Example: OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL
OMN = Omnia system
VLOC = Variable Line of Credit product
RBS = Risk-Based System
RISK_ATTR = Risk Attributes operation
TRANSFORM = Transformation type
OAL = Operational Analytics Layer
```

**2. STREAM KEY MANAGEMENT (from your actual registrations):**
- Your VLOC stream key: 17998
- Home Loan AIRB Model: 15317
- Personal Loan IFRS9 Model: 15571
- Personal Loan AIRB Model: 16058
- Document stream key assignments in Houston

**3. DEPENDENCY CONFIGURATION (from deploy-stream-dependency.sh):**
- Map all upstream data sources
- Configure dependency checks in Houston
- Test dependency validation in lower environments
- Document dependency rationale

**4. MONITORING SETUP (from hl_load_assurance_python.sh):**
- Configure load assurance metrics
- Set up alerting for failures
- Monitor job performance trends
- Establish SLA thresholds

**5. ERROR HANDLING (from your actual scripts):**
- Implement retry mechanisms
- Configure failure notifications
- Document troubleshooting procedures
- Maintain operational runbooks

## src/main/resources

**YAML DEFINITION:**
YAML (YAML Ain't Markup Language) is a human-readable data serialization standard that defines how your ETL pipeline should behave.

**YOUR PROJECT CONTEXT:**
The YAML file `bv_rbs_vloc_acct_modl_inpt_mnly_ongoing.yaml` is the master configuration that tells Spark exactly how to process your VLOC data. It's like the conductor's sheet music that orchestrates the entire data pipeline.

### Pipeline Metadata Section
```yaml
name: bv_rbs_vloc_acct_modl_inpt_mnly_ongoing
description: "VLOC Account Model Input Monthly - Ongoing Processing"
version: "5.43.3"
```

**WHAT THIS SECTION DOES:**
- name: Unique identifier for this pipeline configuration
- description: Human-readable explanation of pipeline purpose
- version: Tracks configuration changes and compatibility

**BUSINESS IMPORTANCE:**
- Enables multiple pipeline versions to coexist
- Supports rollback to previous configurations
- Provides audit trail for regulatory compliance

**REAL WORLD EXAMPLE:**
When you deploy a new version, the system can identify:
- Which configuration version processed which data
- When changes were made and by whom
- How to rollback if issues occur

### Extract Section Analysis

**SOURCE TABLE CONFIGURATION:**
```yaml
extract:
  sources:
    - name: acct_detl
      table: "${srcHdfsEnv}_enr.acct_detl"
      filter: "upper(srce_syst_c)=upper('SAP') And year='${year}' And month='${month}'"
      cache: true
```

**DETAILED BREAKDOWN:**

**name: acct_detl**
- Internal reference name used in SQL transformations
- Must match exactly with SQL file references
- Enables clean separation between logical and physical names

**table: "${srcHdfsEnv}_enr.acct_detl"**
- Physical table location in Hive
- `${srcHdfsEnv}` resolves to environment (dev/test/prod)
- `_enr` indicates Enriched layer in data architecture
- Becomes: `prod_enr.acct_detl` in production

**filter: "upper(srce_syst_c)=upper('SAP') And year='${year}' And month='${month}'"**
- Predicate pushdown optimization
- Only reads relevant data partitions
- Reduces I/O and improves performance

**FILTER LOGIC EXPLAINED:**
- `upper(srce_syst_c)=upper('SAP')`: Only SAP system data
- `year='${year}'`: Current processing year
- `month='${month}'`: Current processing month
- Variables resolved at runtime from job parameters

**cache: true**
- Keeps this table in Spark memory
- Used because acct_detl is referenced multiple times
- Avoids re-reading from disk for each transformation

**BUSINESS IMPACT OF FILTERING:**
Your VLOC pipeline processes only:
- SAP system accounts (excludes other systems)
- Current month's data (not historical)
- Specific partitions (not entire table)
This reduces processing time from hours to minutes and costs significantly.

**COMPLEX SOURCE EXAMPLES:**
```yaml
- name: osc_coll_mttr
  table: "${srcHdfsEnv}_enr.osc_coll_mttr"
  filter: "year='${year}' And month='${month}'"
  cache: false
```

**WHY NO CACHE:**
- osc_coll_mttr (collections matters) used only once
- Caching would waste memory
- Better to use memory for frequently accessed tables

```yaml
- name: frl_acct_fcly
  table: "${srcHdfsEnv}_enr.frl_acct_fcly"
  filter: "upper(srce_syst_c)=upper('SAP') And year='${year}' And month='${month}'"
  cache: true
```

**BUSINESS CONTEXT:**
- frl_acct_fcly: Facility account information
- Critical for limit and exposure calculations
- Cached because used in multiple transformations

### Transform Section Analysis

**SQL TRANSFORMATION ORCHESTRATION:**
```yaml
transform:
  sql:
    - name: vloc_initload
      uri: "/sql/vloc_modl_input/vloc_initload.sql"
      cache: true
    - name: vloc_bankruptcy_flag
      uri: "/sql/vloc_modl_input/vloc_bankruptcy_flag.sql"
      cache: false
```

**TRANSFORMATION DEPENDENCY CHAIN:**
Each SQL file builds upon previous results:

**vloc_initload.sql (Foundation)**
- Joins all source tables
- Creates base dataset with all accounts
- Establishes primary business keys
- Sets up data structure for subsequent transformations

**vloc_bankruptcy_flag.sql (Risk Indicator)**
- Uses vloc_initload as input
- Adds bankruptcy flag logic
- References customer legal status
- Critical for default classification

**vloc_arrears_flag.sql (Payment Status)**
- Builds on previous transformations
- Calculates payment delay indicators
- Determines arrears severity codes
- Feeds into default calculations

**CACHING STRATEGY EXPLAINED:**
- cache: true for vloc_initload because it's used by all subsequent transformations
- cache: false for specific flags because they're used only once
- Optimizes memory usage across the pipeline

**SQL FILE EXECUTION ORDER:**
The YAML defines execution sequence:
1. All source tables loaded simultaneously
2. vloc_initload creates base dataset
3. Individual flag calculations run in parallel where possible
4. vloc_ongoing combines all results

**ADVANCED TRANSFORMATION FEATURES:**
```yaml
- name: vloc_payment_courtesy
  uri: "/sql/vloc_modl_input/vloc_payment_courtesy.sql"
  cache: false
  dependencies:
    - vloc_initload
    - vloc_arrears_flag
```

**DEPENDENCY MANAGEMENT:**
- Ensures vloc_initload and vloc_arrears_flag complete first
- Prevents data inconsistency issues
- Enables parallel execution where safe
- Optimizes overall pipeline performance

**BUSINESS LOGIC ENCODING:**
The payment courtesy calculation requires:
- Base account data (vloc_initload)
- Current arrears status (vloc_arrears_flag)
- Historical payment patterns
- Complex business rules for counting consecutive good months

### Load Section Analysis

**TARGET TABLE CONFIGURATION:**
```yaml
load:
  target:
    table: "${tgtHdfsEnv}_oal.bv_rbs_vloc_acct_modl_inpt_mnly"
    mode: "overwrite"
    partitionBy: ["year"]
    format: "parquet"
    compression: "snappy"
```

**DETAILED BREAKDOWN:**

**table: "${tgtHdfsEnv}_oal.bv_rbs_vloc_acct_modl_inpt_mnly"**
- Target table in OAL (Omnia Access Layer)
- Environment-specific (dev_oal, test_oal, prod_oal)
- Matches DDL file table name exactly

**mode: "overwrite"**
- Replaces existing partition data
- Ensures data consistency
- Prevents duplicate records
- Supports reprocessing scenarios

**ALTERNATIVE MODES:**
- "append": Adds new data without removing existing
- "ignore": Skips if data already exists
- "error": Fails if target already exists

**partitionBy: ["year"]**
- Creates separate folders for each year
- Enables efficient querying by year
- Supports data archival strategies
- Improves query performance

**format: "parquet"**
- Columnar storage format
- Excellent compression ratios
- Optimized for analytical queries
- Schema evolution support

**compression: "snappy"**
- Fast compression/decompression
- Good balance of speed vs size
- CPU-efficient for Spark processing
- Industry standard for big data

**BUSINESS IMPACT OF CONFIGURATION CHOICES:**
- Overwrite mode ensures data quality
- Parquet format reduces storage costs by 70%
- Snappy compression speeds up I/O operations
- Year partitioning enables efficient historical analysis

### Advanced YAML Features

**VARIABLE SUBSTITUTION:**
```yaml
variables:
  srcHdfsEnv: "${environment}_enr"
  tgtHdfsEnv: "${environment}_oal"
  year: "${upper_date|date_format('yyyy')}"
  month: "${upper_date|date_format('MM')}"
```

**HOW VARIABLES WORK:**
- `${environment}`: Passed from shell script (dev/test/prod)
- `${upper_date}`: Business date from job parameters
- `|date_format()`: Built-in function to extract date parts
- Variables resolved at runtime before execution

**REAL EXAMPLE:**
If upper_date = "2024-01-31" and environment = "prod":
- srcHdfsEnv becomes "prod_enr"
- tgtHdfsEnv becomes "prod_oal"
- year becomes "2024"
- month becomes "01"

**CONDITIONAL PROCESSING:**
```yaml
transform:
  sql:
    - name: vloc_historical_correction
      uri: "/sql/vloc_modl_input/vloc_historical_correction.sql"
      condition: "${run_type} == 'historical'"
      cache: false
```

**CONDITIONAL LOGIC:**
- SQL only executes if condition is true
- Enables different processing modes in same pipeline
- Supports historical data corrections
- Reduces need for separate pipeline configurations

**DATA QUALITY CHECKS:**
```yaml
quality:
  checks:
    - name: record_count_validation
      type: "count"
      threshold: 1000000
      action: "warn"
    - name: null_account_check
      type: "null_check"
      columns: ["acct_idnn_bk"]
      action: "fail"
```

**QUALITY FRAMEWORK:**
- record_count_validation: Ensures minimum record threshold
- null_account_check: Validates critical fields not null
- action: Defines response (warn/fail/ignore)
- Integrated into pipeline execution

**BUSINESS VALUE:**
- Prevents processing of incomplete data
- Alerts to data quality issues early
- Maintains regulatory compliance
- Reduces downstream impact of bad data

## src/main/run

### Run Directory Architecture

**Directory Structure Overview**

```
run/
├── Core Execution Scripts
│   ├── spark-etl-run.sh                    # Master Spark submission orchestrator
│   ├── spark-etl-run-for-date.sh          # Date-specific wrapper for orchestration
│   └── log4j.properties                   # Logging configuration
│
├── VLOC Pipeline Scripts
│   ├── vloc_file_to_sas_ongoing.sh        # VLOC model input processing
│   └── vloc-file-to-sas/
│       └── vloc_file_to_sas_ongoing.sh    # Alternative VLOC processing
│
├── Home Loan Pipeline Scripts
│   ├── hl-file-to-sas/                    # Home loan data to SAS export
│   ├── hl-gdw-to-oal/                     # GDW to OAL transformation
│   ├── hl-mep-to-oal/                     # MEP to OAL transformation
│   ├── hl-enr-to-oal/                     # ENR to OAL transformation
│   └── hl-sas-to-staging/                 # SAS to staging movement
│
├── Credit Card Pipeline Scripts
│   ├── cc-file-to-sas/                    # Credit card data to SAS export
│   └── cc-sas-to-staging/                 # Credit card SAS to staging
│
├── Personal Loan Pipeline Scripts
│   ├── pl-file-to-sas/                    # Personal loan data to SAS export
│   ├── pl-mep-to-oal/                     # Personal loan MEP to OAL
│   ├── pl-enr-to-oal/                     # Personal loan ENR to OAL
│   └── pl-sas-to-staging/                 # Personal loan SAS to staging
│
├── Data Layer Movement Scripts
│   └── oal-to-obl/                        # OAL to OBL layer promotion
│
├── Specialized Processing Scripts
│   ├── history_catchup.sh                 # Historical data catch-up processing
│   ├── history_load.sh                    # Historical data loading
│   ├── hl_ifrs9_datafix.sh               # Home loan IFRS9 data corrections
│   ├── pl_ifrs9_datafix.sh               # Personal loan IFRS9 data corrections
│   └── hl_load_assurance_python.sh       # Data quality assurance
│
├── Business-Specific Scripts
│   ├── bv_csc_hl_acct_airb_modl_oal.sh   # Home loan AIRB model processing
│   ├── csc_bnpl_enr_oal.sh               # Buy Now Pay Later processing
│   ├── csc_pod_sas_omn_oal.sh            # POD SAS to Omnia processing
│   └── csc_vloc_sas_omn_oal.sh           # VLOC SAS to Omnia processing
│
└── Configuration Directory
    └── conf/                               # Environment and pipeline configurations
        ├── Environment Configs (prod.conf, dev.conf, test.conf, etc.)
        ├── Pipeline Configs (bv_rbs_vloc_acct_modl_inpt_mnly_ongoing.conf, etc.)
        └── Override Configs (spark-etl.override.*.conf)
```

---

### Core Execution Scripts

#### 1. spark-etl-run.sh - Master Orchestrator

**Purpose and Architecture**
The `spark-etl-run.sh` script is the central orchestration engine for all Spark-based ETL jobs in the IFRS9 pipeline. It provides a standardized interface for job submission while handling complex configuration management, resource allocation, and environment-specific optimizations.

**Key Technical Components**

**A. Command Line Interface and Argument Processing**
```bash
# CLI Arguments Structure:
# --runner: Execution mode (houston, houstonForDate, default, validate, etc.)
# --pipeline: Pipeline configuration name
# <jar|directory>: Application artifact location

# Supported Runners:
- houston          : Houston-enabled runner for production orchestration
- houstonForDate   : Single date processing with Houston integration
- default          : No-Houston mode for standalone execution
- validate         : Runtime validation runner for testing
- remoteValidate   : Cluster-side validation for distributed testing
- test             : Functional test runner
- withWfApi        : Default runner wrapped with Workflow API
```

**B. Dynamic Resource Allocation System**
```bash
# Job Size-Based Resource Allocation
case $SPARK_JOB_SIZE in
    "small")
        SPARK_EXECUTOR_MEMORY=8G
        SPARK_NUM_EXECUTORS=50
        ;;
    "medium")
        SPARK_EXECUTOR_MEMORY=16G
        SPARK_NUM_EXECUTORS=100
        ;;
    "large")
        SPARK_EXECUTOR_MEMORY=24G
        SPARK_NUM_EXECUTORS=200
        ;;
esac
```

**C. Configuration File Hierarchy**
```bash
# Configuration Loading Order (Higher Priority Overrides Lower):
1. log4j.properties                           # Base logging config
2. conf/${ENV}.conf                          # Environment-specific config
3. conf/override.${ENV}.conf                 # Environment overrides
4. conf/${PIPELINE_NAME}.conf                # Pipeline-specific config
5. conf/override.${ENV}.${PIPELINE_NAME}.conf # Pipeline + environment overrides
6. ${CONF_FILE_SPARK_ETL_OVERRIDE}          # Custom override file
```

**D. Spark Configuration Optimization**
```bash
# Production-Optimized Spark Settings
--conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
--conf spark.driver.maxResultSize=4G \
--conf spark.executor.memoryOverhead=6G \
--conf spark.yarn.maxAppAttempts=$SPARK_MAX_ATTEMPTS \
--conf spark.yarn.max.executor.failures=200 \
--conf spark.locality.wait=0 \
--conf spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec \
--conf spark.rdd.compress=true \
--conf spark.sql.crossJoin.enabled=true \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.task.maxFailures=20
```

**E. Houston Integration and TLS Security**
```bash
# Houston TLS Configuration
enable_houston_tls_configuration() {
  if [[ -n "$HOUSTON_TLS_KEYSTORE_PATH" && -n "$HOUSTON_TLS_KEYSTORE_PASSWORD" ]]; then
    extraJvmOpts="${extraJvmOpts} -DHOUSTON_TLS_KEYSTORE_PASSWORD=${HOUSTON_TLS_KEYSTORE_PASSWORD}"
    extraJvmOpts="${extraJvmOpts} -DHOUSTON_TLS_KEYSTORE_PATH=$keystore_filename"
    CONF_FILES+=",${HOUSTON_TLS_KEYSTORE_PATH}#$keystore_filename"
  fi
}
```

**Business Value and Technical Benefits**

1. **Standardization**: Single interface for all ETL job submissions across different environments
2. **Resource Optimization**: Dynamic resource allocation based on job size and data volume
3. **Configuration Management**: Hierarchical configuration system supporting environment-specific overrides
4. **Security Integration**: Built-in TLS support for Houston communication
5. **Error Handling**: Comprehensive validation and failure detection mechanisms
6. **Monitoring Integration**: Native Houston integration for job tracking and metrics collection

#### 2. spark-etl-run-for-date.sh - Orchestration Wrapper

**Purpose and Integration**
This script serves as a bridge between external orchestration systems (like Autosys) and the core Spark execution engine. It translates orchestration-specific parameters into the format expected by the main execution script.

**Key Technical Features**

**A. Date Processing and Business Logic**
```bash
# ISO8601 Date Format Conversion
runDate=${BUSINESS_DATE%T*}          # Strip time component: 2024-01-31T20:30:00 → 2024-01-31
prevRunDate=${PREV_BUSINESS_DATE%T*} # Handle previous business date for incremental processing

# Conditional Previous Date Handling
prevRunDateArg=""
if [[ ! -z "$prevRunDate" ]]; then
  prevRunDateArg="-Dprev_run_date=${prevRunDate}"
fi
```

**B. Upstream Dependency Management**
```bash
# Upstream Business Dates Processing
# Format: "ETL_ID_1=2024-01-31T20:30:00,ETL_ID_2=2024-01-31T21:00:00"
"-Dupstream_business_dates=${UPSTREAM_BUSINESS_DATES:-''}"
```

**C. Houston Integration Parameters**
```bash
# Mandatory Environment Variables for Orchestration
ETL_ID                   # Houston job identifier
BUSINESS_DATE            # Current processing date (ISO8601)
PREV_BUSINESS_DATE       # Previous processing date (ISO8601)
UPSTREAM_BUSINESS_DATES  # Dependency dates (comma-separated key-value pairs)
```

**Business Value**
1. **Orchestration Integration**: Seamless integration with enterprise scheduling systems
2. **Date Management**: Robust business date handling with timezone awareness
3. **Dependency Tracking**: Upstream job dependency management for data lineage
4. **Standardization**: Consistent interface for orchestrated vs. manual job execution

---

### Pipeline-Specific Scripts

#### A. vloc_file_to_sas_ongoing.sh - VLOC Model Input Processing

**Purpose**: Processes VLOC (Variable Line of Credit) account data for IFRS9 model input generation.

**Technical Architecture**:
```bash
# Key Configuration
export PIPELINE=bv_rbs_vloc_acct_modl_inpt_mnly_ongoing
export SPARK_JOB_SIZE=large                    # Large resource allocation
export SPARK_DEPLOY_MODE=cluster               # Distributed execution

# Business Date Logic
prevMnthEndDate=`echo $(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)`
export RUN_DATE=${businessdate:-$prevMnthEndDate}

# Previous Month Calculation for Historical Context
prevRunDate=`echo $(date -d "$year-$month-01 -1 day" +%Y-%m-%d)`
yearPrevRun=`date '+%Y' -d "$prevRunDate"`
monthPrevRun=`date '+%m' -d "$prevRunDate" | awk '{printf "%02d\n", $0;}'`
```

**Business Logic**:
- **Monthly Processing**: Runs on month-end dates for regulatory reporting
- **Historical Context**: Maintains previous month data for comparative analysis
- **Large Scale Processing**: Configured for high-volume data processing (2M+ records)
- **Houston Integration**: Uses `houstonForDate` runner for orchestration tracking

**Data Flow**:
```
Source Tables → VLOC Model Input Processing → bv_rbs_vloc_acct_modl_inpt_mnly → SAS Analytics
```

#### B. VLOC Configuration Deep Dive

**Pipeline Configuration** (`bv_rbs_vloc_acct_modl_inpt_mnly_ongoing.conf`):
```properties
etlId=OMN_VLOC_RBS_RISK_ATTR_TRANSFORM_OAL    # Houston job identifier
mopImplDate=2022-01-31                         # Model implementation date
oscaCutoffDate=2025-07-11                      # OSCA system cutoff date
```

**Technical Significance**:
- **etlId**: Links to Houston orchestration system for job tracking
- **mopImplDate**: Controls business logic for model implementation changes
- **oscaCutoffDate**: Manages data source transitions (OSCA to new systems)

---


### Configuration Management

#### 1. Environment Configuration Hierarchy

**A. Base Environment Configurations**

**prod.conf** - Production Environment:
```properties
srcHdfsEnv=prod                                    # Source HDFS environment
hdfsEnv=prod                                       # Target HDFS environment
teradata.uri="jdbc:teradata://teradata.gdw.cba/database=U_D_DSV


## 5. Omnia to Cloud (OTC) Migration - Brown Field vs Green Field Changes

### Omnia to Cloud (OTC) Overview

#### What is Omnia to Cloud (OTC)?

**OTC (Omnia to Cloud)** is CBA's strategic initiative to migrate the existing **on-premises Omnia platform** to **cloud infrastructure** (primarily AWS/Azure). This represents one of the largest data platform modernization efforts in Australian banking.

#### Why the Migration?

**Business Drivers:**
- **Cost Optimization**: Reduce infrastructure costs by 30-40%
- **Scalability**: Elastic scaling based on demand
- **Innovation**: Access to cloud-native services and AI/ML capabilities
- **Agility**: Faster deployment and development cycles
- **Resilience**: Enhanced disaster recovery and availability
- **Compliance**: Meet evolving regulatory requirements with cloud compliance

#### Migration Scope
```
Current Omnia (On-Premises) → Cloud Platform
├── Infrastructure: 500+ physical servers → Cloud services
├── Storage: 10+ PB HDFS → Cloud object storage + managed databases
├── Compute: YARN clusters → Kubernetes + serverless
├── Applications: 100+ ETL jobs → Cloud-native applications
├── Data: Petabytes of banking data → Multi-tier cloud storage
└── Users: 10,000+ users → Cloud-based access and security
```

### Brown Field Changes in OTC

#### Definition in OTC Context

**Brown Field Changes** = Migrating **existing applications and data pipelines** from on-premises Omnia to cloud with **minimal modification** to preserve functionality.

#### Characteristics of Brown Field Migration

**1. Lift and Shift Approach**
```
Brown Field Migration Pattern:
├── Existing Application (etl.oal-ifrs9)
│   ├── Same Scala code
│   ├── Same SQL transformations
│   ├── Same business logic
│   └── Same YAML configurations
├── Infrastructure Changes Only
│   ├── HDFS → AWS S3 / Azure Data Lake
│   ├── YARN → Amazon EMR / Azure HDInsight
│   ├── On-premises Hive → Cloud Hive Metastore
│   └── Physical servers → Cloud instances
└── Minimal Code Changes
    ├── Connection strings updated
    ├── File paths changed
    ├── Authentication methods adjusted
    └── Configuration parameters modified
```

**2. Examples of Brown Field Changes**

**IFRS9 Pipeline Migration (Brown Field):**
```yaml
# Original Omnia Configuration
extract:
  sources:
    - name: acct_detl
      table: "prod_enr.acct_detl"
      path: "hdfs://nameservice1/prod/enr/acct_detl"

# Brown Field Cloud Migration
extract:
  sources:
    - name: acct_detl
      table: "prod_enr.acct_detl"
      path: "s3a://cba-prod-enr/acct_detl"  # Only path changes
```

**Connection Changes:**
```scala
// Original Omnia
val spark = SparkSession.builder()
  .appName("IFRS9 Processing")
  .config("spark.master", "yarn")
  .config("hive.metastore.uris", "thrift://omnia-metastore:9083")

// Brown Field Cloud Migration
val spark = SparkSession.builder()
  .appName("IFRS9 Processing")
  .config("spark.master", "k8s://k8s-apiserver")  # Kubernetes instead of YARN
  .config("hive.metastore.uris", "thrift://cloud-metastore:9083")  # Cloud metastore
```

**3. Brown Field Migration Benefits**
```
Advantages:
├── Speed: Faster migration timeline (6-12 months)
├── Risk: Lower risk of breaking existing functionality
├── Resources: Existing team knowledge preserved
├── Testing: Less extensive testing required
├── Rollback: Easier rollback to on-premises if needed
└── Business Continuity: Minimal disruption to operations
```

#### Brown Field Examples in OTC

**Example 1: VLOC Pipeline Migration**
```
Original On-Premises:
├── Runs on YARN cluster with 200 executors
├── Reads from HDFS: /prod/enr/acct_detl
├── Writes to HDFS: /prod/oal/vloc_model_input
├── Uses Hive Metastore on physical servers
└── Scheduled via on-premises Autosys

Brown Field Cloud Migration:
├── Runs on EMR cluster with auto-scaling
├── Reads from S3: s3://cba-prod-enr/acct_detl/
├── Writes to S3: s3://cba-prod-oal/vloc_model_input/
├── Uses AWS Glue Data Catalog
└── Scheduled via AWS Step Functions (replacing Autosys)
```

**Example 2: Houston Migration**
```
Original Houston (On-Premises):
├── Oracle database on physical servers
├── Java web application on Tomcat
├── ETL job registry and dependency management
└── Load assurance and monitoring

Brown Field Houston Migration:
├── Amazon RDS Oracle (managed database)
├── Application on ECS containers
├── Same functionality with cloud infrastructure
└── Integration with cloud monitoring services
```

### Green Field Changes in OTC

#### Definition in OTC Context

**Green Field Changes** = Building **new cloud-native applications and services** that leverage modern cloud technologies and architectural patterns.

#### Characteristics of Green Field Development

**1. Cloud-Native Architecture**
```
Green Field Cloud-Native Design:
├── Microservices Architecture
│   ├── Individual services for specific functions
│   ├── API-first design principles
│   ├── Independent scaling and deployment
│   └── Container-based deployment
├── Serverless Computing
│   ├── AWS Lambda / Azure Functions
│   ├── Event-driven processing
│   ├── Pay-per-execution model
│   └── Automatic scaling
├── Managed Services
│   ├── Amazon Redshift / Azure Synapse
│   ├── AWS Glue / Azure Data Factory
│   ├── Amazon Kinesis / Azure Event Hubs
│   └── Cloud-native databases
└── Modern Data Architecture
    ├── Data lake house architecture
    ├── Streaming-first approach
    ├── Real-time analytics
    └── ML/AI integration
```

**2. Examples of Green Field Changes**

**New Real-Time Fraud Detection System:**
```
Green Field Architecture:
├── Data Ingestion: Amazon Kinesis Data Streams
├── Processing: AWS Lambda functions
├── Storage: DynamoDB for real-time data, S3 for historical
├── Analytics: Amazon Kinesis Analytics
├── ML: Amazon SageMaker for model training/inference
├── API: API Gateway for external access
└── Monitoring: CloudWatch + custom dashboards
```

**New Customer Analytics Platform:**
```
Green Field Cloud-Native Stack:
├── Data Lake: AWS S3 with Delta Lake format
├── Compute: Amazon EMR Serverless
├── Orchestration: AWS Step Functions
├── Catalog: AWS Glue Data Catalog
├── Analytics: Amazon Athena + QuickSight
├── ML Pipeline: SageMaker Pipelines
├── API: GraphQL API on AWS AppSync
└── Security: AWS IAM + Lake Formation
```

**3. Green Field Development Benefits**
```
Advantages:
├── Optimization: Full cloud-native optimization
├── Scalability: Elastic and cost-effective scaling
├── Innovation: Access to latest cloud services
├── Performance: Optimized for cloud characteristics
├── Cost: Pay-per-use model, no infrastructure overhead
├── Agility: Faster development and deployment cycles
├── Integration: Native cloud service integration
└── Future-Ready: Built for evolving requirements
```

**4. Green Field Development Challenges**
```
Challenges:
├── Time: Longer development timeline (12-24 months)
├── Risk: Higher risk due to new architecture
├── Skills: Requires new cloud-native skills
├── Integration: Must integrate with existing systems
├── Data Migration: Complex data migration requirements
├── Testing: Extensive testing of new architecture
└── Change Management: Significant organizational change
```

### Migration Strategies

#### Hybrid Approach: Combination Strategy

Most successful OTC migrations use a **combination of Brown Field and Green Field** approaches:

**Phase 1: Brown Field Foundation (Months 1-12)**
```
Quick Wins - Lift and Shift:
├── Infrastructure Migration
│   ├── Move HDFS data to S3
│   ├── Migrate YARN jobs to EMR
│   ├── Transition Hive to Glue Catalog
│   └── Move databases to managed services
├── Application Migration
│   ├── IFRS9 pipelines with minimal changes
│   ├── Existing ETL jobs on cloud infrastructure
│   ├── Houston functionality on cloud
│   └── Monitoring and alerting on cloud
└── Benefits Realized
    ├── Reduced infrastructure costs
    ├── Improved disaster recovery
    ├── Enhanced monitoring capabilities
    └── Foundation for further optimization
```

**Phase 2: Green Field Innovation (Months 6-24)**
```
New Capabilities - Cloud Native:
├── Real-Time Analytics
│   ├── Streaming data pipelines
│   ├── Real-time dashboards
│   ├── Event-driven architecture
│   └── Serverless processing
├── Advanced Analytics
│   ├── ML/AI model deployment
│   ├── Self-service analytics
│   ├── Advanced visualization
│   └── Predictive analytics
├── Modern Data Architecture
│   ├── Data lake house implementation
│   ├── API-first data access
│   ├── Microservices architecture
│   └── Event sourcing patterns
└── Enhanced Capabilities
    ├── Auto-scaling and optimization
    ├── Advanced security features
    ├── Global data replication
    └── Edge computing capabilities
```

**Phase 3: Optimization and Modernization (Months 12-36)**
```
Continuous Improvement:
├── Performance Optimization
│   ├── Cost optimization analysis
│   ├── Performance tuning
│   ├── Architecture refinement
│   └── Service consolidation
├── Feature Enhancement
│   ├── Advanced cloud services adoption
│   ├── AI/ML capability expansion
│   ├── Real-time processing improvements
│   └── User experience enhancements
└── Legacy Retirement
    ├── Decommission on-premises infrastructure
    ├── Complete cloud migration
    ├── Optimize cloud resource usage
    └── Continuous monitoring and improvement
```

## 6. Execution Flow of Repository

### STEP-BY-STEP EXECUTION FLOW:

#### STEP 1: AUTOSYS JOB SCHEDULING

**WHAT HAPPENS INTERNALLY:**
When the calendar hits the 1st of the month at 20:30, here's the detailed process:

**1. AUTOSYS SCHEDULER DAEMON PROCESS:**
- Runs continuously on dedicated server
- Checks Oracle database every minute for jobs ready to run
- Evaluates calendar conditions and dependencies
- Updates job status from INACTIVE to READY

**2. JOB CONDITION EVALUATION:**
```
Calendar: hado_omnia_mth_1_cal
Condition: Date = 1st of month AND Time >= 20:30
Dependencies: All upstream jobs must be SUCCESS
```

**3. AGENT COMMUNICATION:**
- Central scheduler sends execution request to target machine agent
- Agent receives job definition and command to execute
- Agent creates new process with specified user credentials
- Agent starts monitoring job execution

**REAL EXAMPLE FROM YOUR PROJECT:**
```
Job: au_cba_hado_p01_16058_mlt_enr_oal_omn_mth_tfm_cmd
Status: INACTIVE → READY → RUNNING
Command: ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/vloc_pipeline.sh
```

#### STEP 2: SHELL SCRIPT INITIALIZATION

**WHAT HAPPENS WHEN YOUR .SH FILE RUNS:**

**1. ENVIRONMENT SETUP:**
```bash
export RUN_DIR=${RUN_DIR:-$PWD}
export JAR=`ls ${RUN_DIR}/../../etl-oal-ifrs9*thin.jar`
export PIPELINE=bv_rbs_vloc_acct_modl_inpt_mnly_ongoing
```

**DETAILED EXPLANATION:**
- RUN_DIR: Sets working directory for script execution
- JAR: Locates your compiled application JAR file
- PIPELINE: Specifies which YAML configuration to use

**2. DATE CALCULATIONS:**
```bash
businessdate=$1
prevMnthEndDate=`echo $(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)`
export RUN_DATE=${businessdate:-$prevMnthEndDate}
```

**WHAT THIS DOES:**
- Takes date parameter from command line (if provided)
- Calculates previous month end date as default
- Sets RUN_DATE environment variable for Spark job

**3. SPARK CONFIGURATION:**
```bash
export SPARK_DEPLOY_MODE=cluster
export SPARK_JOB_SIZE=large
```

**BUSINESS IMPACT:**
- cluster mode: Driver runs on cluster, not local machine
- large size: Allocates more CPU and memory resources

#### STEP 3: SPARK APPLICATION SUBMISSION

**DETAILED SPARK-SUBMIT PROCESS:**

**1. COMMAND CONSTRUCTION:**
Your shell script builds this command:
```bash
${RUN_DIR}/../spark-etl-run.sh \
-Dupper_date=${RUN_DATE} \
-DyearPrevRun=${yearPrevRun} \
-DmonthPrevRun=${monthPrevRun} \
--runner houstonForDate \
--pipeline $PIPELINE \
$JAR
```

**2. SPARK-SUBMIT EXECUTION:**
Internally, this becomes:
```bash
spark-submit \
--class com.cba.omnia.etl.Main \
--master yarn \
--deploy-mode cluster \
--num-executors 20 \
--executor-cores 4 \
--executor-memory 8g \
--driver-memory 4g \
etl-oal-ifrs9_2.12-5.43.3-thin.jar
```

**3. YARN RESOURCE ALLOCATION:**
- YARN ResourceManager receives application request
- Allocates ApplicationMaster container on cluster node
- ApplicationMaster requests executor containers
- NodeManagers start executor JVM processes

#### STEP 4: DRIVER PROGRAM INITIALIZATION

**WHAT HAPPENS IN THE DRIVER:**

**1. SPARK CONTEXT CREATION:**
```scala
val spark = SparkSession.builder()
  .appName("VLOC Model Input Pipeline")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate()
```

**2. CONFIGURATION LOADING:**
- Driver reads YAML file from JAR classpath
- Parses extract, transform, load sections
- Validates configuration parameters
- Creates execution plan

**3. HIVE METASTORE CONNECTION:**
- Driver connects to Hive Metastore service
- Retrieves table schemas and locations
- Validates source table availability
- Caches metadata for performance

#### STEP 5: DATA EXTRACTION PHASE

**DETAILED EXTRACTION PROCESS:**

**1. SOURCE TABLE ANALYSIS:**
For each source table in your YAML:
```yaml
- name: acct_detl
  table: "${srcHdfsEnv}_enr.acct_detl"
  filter: "upper(srce_syst_c)=upper('SAP') And year='${year}' And month='${month}'"
```

**2. PARTITION DISCOVERY:**
- Spark queries Hive Metastore for table partitions
- Identifies relevant partitions based on filters
- Gets HDFS file locations for each partition
- Calculates optimal number of read tasks

**3. PARALLEL DATA READING:**
```
Executor 1: Reads acct_detl partition files 1-5
Executor 2: Reads acct_detl partition files 6-10
Executor 3: Reads acct_baln partition files 1-3
Executor 4: Reads acct_limt partition files 1-4
... (all happening simultaneously)
```

**4. DATA LOADING INTO MEMORY:**
- Each executor loads its assigned Parquet files
- Column pruning: Only reads required columns
- Predicate pushdown: Applies filters during read
- Data cached in executor memory for reuse

#### STEP 6: DATA TRANSFORMATION PHASE

**COMPLEX TRANSFORMATION ORCHESTRATION:**

**1. SQL FILE LOADING:**
Your YAML references 11 SQL transformation files:
```yaml
- name: vloc_initload
  uri: "/sql/vloc_modl_input/vloc_initload.sql"
- name: vloc_realisation_flag
  uri: "/sql/vloc_modl_input/vloc_realisation_flag.sql"
```

**2. TRANSFORMATION DEPENDENCY GRAPH:**
Spark analyzes SQL files and creates execution order:
```
vloc_initload (base data)
↓
vloc_bankruptcy_flag (depends on base)
↓
vloc_arrears_flag (depends on base)
↓
vloc_ongoing (depends on all previous)
```

**3. PARALLEL TRANSFORMATION EXECUTION:**
```
Executor 1: Processes accounts 1-100,000
- Applies vloc_initload logic
- Calculates bankruptcy flag
- Calculates arrears flag
Executor 2: Processes accounts 100,001-200,000
- Same transformations in parallel
... (all executors working simultaneously)
```

**4. INTERMEDIATE RESULT CACHING:**
- Frequently used DataFrames cached in memory
- Avoids recomputation for multiple transformations
- Automatic spill to disk if memory full

**DETAILED TRANSFORMATION EXAMPLE:**
Let's trace one account through the transformation:

**ACCOUNT: VLOC123456789**

**INPUT DATA:**
- acct_detl: Account active, SAP system
- acct_baln: Outstanding balance $50,000
- osc_coll_mttr: No collections matters
- acct_limt: Credit limit $75,000

**TRANSFORMATION STEPS:**
1. vloc_initload.sql: Selects account as active VLOC
2. vloc_bankruptcy_flag.sql: Checks customer status → 'N'
3. vloc_arrears_flag.sql: Checks payment history → '0' (current)
4. vloc_ongoing.sql: Combines all flags → in_spot_dflt_f = 'N'

#### STEP 7: DATA JOINING OPERATIONS

**COMPLEX JOIN OPTIMIZATION:**

**1. JOIN STRATEGY SELECTION:**
Spark Catalyst optimizer analyzes table sizes:
```
Large Tables (>1GB): acct_detl, acct_baln, frl_acct_fcly
Medium Tables (100MB-1GB): osc_coll_mttr, acct_hrds
Small Tables (<100MB): Product reference tables
```

**2. BROADCAST JOIN OPTIMIZATION:**
Small reference tables are broadcast to all executors:
```scala
// Spark automatically broadcasts small tables
val result = largeTable.join(broadcast(smallTable), "key")
```

**3. SHUFFLE JOIN FOR LARGE TABLES:**
```
Phase 1: Hash partition both tables by join key
Phase 2: Co-locate matching partitions on same executors
Phase 3: Perform local joins within each executor
```

**4. JOIN EXECUTION EXAMPLE:**
```
Executor 1: Joins acct_detl[1-100K] with acct_baln[1-100K]
Executor 2: Joins acct_detl[100K-200K] with acct_baln[100K-200K]
... (parallel join execution across all executors)
```

#### STEP 8: FINAL DATA AGGREGATION

**RESULT CONSOLIDATION:**

**1. PARTITION CONSOLIDATION:**
- Each executor has processed its portion of data
- Results need to be combined into final dataset
- Spark performs intelligent repartitioning

**2. FINAL TRANSFORMATION:**
vloc_ongoing.sql creates the final output:
```sql
SELECT 
  acct_idnn_bk,
  perd_d,
  in_spot_dflt_f,
  ostd_baln_a,
  ... (all 37 columns)
FROM combined_transformations
```

**3. DATA QUALITY CHECKS:**
- Null value validation
- Data type consistency
- Business rule validation
- Record count verification

#### STEP 9: DATA WRITING PHASE

**PARALLEL WRITING PROCESS:**

**1. OUTPUT PARTITIONING:**
```scala
finalDataFrame
  .repartition(col("year"))
  .write
  .mode("overwrite")
  .partitionBy("year")
  .parquet(targetLocation)
```

**2. TEMPORARY WRITE LOCATION:**
- Data first written to temporary HDFS location
- Prevents corruption if job fails during write
- Example: /tmp/vloc_table_20240131_temp/

**3. PARALLEL FILE WRITING:**
```
Executor 1: Writes year=2024 partition part-00001.parquet
Executor 2: Writes year=2024 partition part-00002.parquet
Executor 3: Writes year=2023 partition part-00001.parquet
... (multiple files per partition for parallelism)
```

**4. ATOMIC MOVE OPERATION:**
- Once all executors finish writing
- Files moved from temp location to final location
- Old partition data replaced atomically
- Ensures data consistency

#### STEP 10: HIVE METASTORE UPDATE

**METADATA SYNCHRONIZATION:**

**1. PARTITION REGISTRATION:**
```sql
ALTER TABLE prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly 
ADD PARTITION (year='2024') 
LOCATION 'hdfs://nameservice1/prod/oal/bv_rbs_vloc_acct_modl_inpt_mnly/year=2024'
```

**2. STATISTICS UPDATE:**
- Row count per partition
- File size information
- Column statistics for query optimization
- Last modified timestamps

**3. MSCK REPAIR EXECUTION:**
- Automatically discovers new partitions
- Updates Hive Metastore with partition metadata
- Makes data immediately queryable

#### STEP 11: JOB COMPLETION AND CLEANUP

**FINALIZATION PROCESS:**

**1. RESOURCE CLEANUP:**
- Spark executors shut down gracefully
- Temporary files cleaned up
- Memory released back to cluster
- Network connections closed

**2. STATUS REPORTING:**
- Job completion status sent to YARN
- Execution metrics logged
- Performance statistics recorded
- Error logs (if any) preserved

**3. AUTOSYS STATUS UPDATE:**
- Job status changed from RUNNING to SUCCESS
- Downstream jobs become eligible to run
- Notification emails sent (if configured)
- Job history updated in database

#### STEP 12: DOWNSTREAM PROCESSING

**WHAT HAPPENS NEXT:**

**1. STREAM RESET JOB:**
```bash
${scriptdir}/reset_autosys_jobs pattern $AUTO_JOB_NAME
```
- Marks current job as inactive
- Prepares for next month's execution
- Updates job scheduling database

**2. BDR SYNC JOB:**
```bash
${OMNIA_ENVIRONMENT_PATH}/etl-bdr-trigger/run/bdr_client.sh 16058
```
- Replicates data to Data Science cluster
- Ensures data consistency across environments
- Enables analytics and model development

**3. LOAD ASSURANCE CHECKS:**
- Validates record counts
- Checks data quality metrics
- Compares with previous month's data
- Sends alerts if anomalies detected

### Error Handling and Recovery

#### What Happens When Things Go Wrong

**1. Executor Failure**
- Spark automatically detects failed executor
- Redistributes failed tasks to healthy executors
- Uses lineage information to recreate lost data
- Job continues without manual intervention

**2. Driver Failure**
- YARN restarts ApplicationMaster
- Job state recovered from checkpoints
- Processing resumes from last successful stage
- Minimal data loss or reprocessing

**3. Data Quality Issues**
- Validation rules detect anomalies
- Job fails with detailed error messages
- Data remains in temporary location
- Manual investigation and correction required

**4. Resource Constraints**
- YARN queues job if resources unavailable
- Automatic retry with exponential backoff
- Graceful degradation with reduced parallelism
- Monitoring alerts for capacity issues

### Performance Monitoring

#### What Gets Tracked

**1. Execution Metrics**
- Total job runtime
- Data volume processed
- CPU and memory utilization
- Network I/O statistics

**2. Business Metrics**
- Record counts by partition
- Data quality scores
- Processing throughput
- Error rates and types

**3. Infrastructure Metrics**
- Cluster resource utilization
- HDFS storage consumption
- Network bandwidth usage
- Database connection pools

## 7. Real Time Execution Flow

### Step-by-Step Real-Time Execution Flow

#### Step 1: Autosys Scheduling Trigger (20:30 on 1st of month)

**What Happens:**
Autosys Workload Automation (AWA) system triggers the job based on calendar and dependency conditions.

**Actual JIL File Analysis (from oal_ifrs9_vloc_insert.jil):**
```jil
insert_job: au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd
job_type: CMD
command: ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/ifrs9_vloc_omnia_to_sas.sh
machine: hado-omnia-p01
owner: omnia_prod@hado-omnia-p01
start_times: "20:30"
run_calendar: hado_omnia_mth_1_cal
condition: s(au_cba_hado_prod_5143_dil_oal_osc_mth_polling_dw_cmd)
```

**Key Components Explained:**
- **5143**: Stream ID for VLOC processing
- **hado-omnia-p01**: Physical server where job executes
- **omnia_prod**: Service account with appropriate permissions
- **run_calendar**: Monthly calendar (1st of each month)
- **condition**: Waits for upstream data polling job to succeed

**Business Context:**
This timing ensures all month-end source data is available before IFRS9 processing begins.

#### Step 2: Shell Script Execution (ifrs9_vloc_omnia_to_sas.sh)

**Script Analysis:**
```bash
#!/usr/bin/env bash
cd ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run
export RUN_DIR=${RUN_DIR:-$PWD}
export JAR=`ls ${RUN_DIR}/../etl-oal-ifrs9*thin.jar`
export PIPELINE=vloc_osc_al
JOB_SIZE="large"
businessdate=$1
prevMnthEndDate=`echo $(date -d "$(date +%Y-%m-01) -1 day " +%Y-%m-%d)`
export RUN_DATE=${businessdate:-$prevMnthEndDate}
```

**What This Script Does:**
1. **Environment Setup**: Sets working directory and locates JAR file
2. **Date Calculation**: Determines processing date (previous month-end)
3. **Resource Allocation**: Sets job size to "large" (200 executors, 24GB memory each)
4. **Pipeline Selection**: Specifies "vloc_osc_al" configuration

**Real Example:**
If run on January 1st, 2024:
- RUN_DATE becomes "2023-12-31"
- Processes December 2023 data
- Uses large cluster resources for production volume

#### Step 3: Spark-ETL-Run.sh Orchestration

**Command Execution:**
```bash
${RUN_DIR}/spark-etl-run.sh -Dupper_date=${RUN_DATE} \
-DSPARK_JOB_SIZE=${JOB_SIZE} \
--runner houstonForDate --pipeline $PIPELINE $JAR
```

**spark-etl-run.sh Key Functions:**

**Resource Allocation (Large Job):**
```bash
case $SPARK_JOB_SIZE in
    "large")
        SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-24G}
        SPARK_NUM_EXECUTORS=${SPARK_NUM_EXECUTORS:-200}
        ;;
esac
```

**Configuration Management:**
```bash
CONF_FILES="${RUN_DIR}/log4j.properties"
CONF_FILE_ENV=conf/${ENV}.conf
CONF_FILE_PIPELINE=conf/${PIPELINE_NAME}.conf
```

**Spark Submit Command Construction:**
```bash
CMD="$SPARK_HOME/bin/spark-submit \
--name ${PIPELINE_NAME} \
--conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
--conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
--executor-cores $SPARK_EXECUTOR_CORES \
--class au.com.cba.omnia.etl.execution.SingleHoustonLoadRunRunner \
--master yarn \
--deploy-mode cluster \
${PIPELINE_JAR}"
```

#### Step 4: Houston Integration and Stream Management

**What is Houston?**
Houston is CBA's internal data catalog and stream management system that:
- Tracks data lineage and dependencies
- Manages stream metadata and quality metrics
- Provides data discovery capabilities
- Monitors data freshness and availability

**Houston Registration Process:**
From houston-deploy.sh, your VLOC pipeline is registered as:
```bash
./houston-setup-load.sh "${ENV}" OMN_VLOC_SAS_REPORT_TRANSFORM_OAL etl.oal-ifrs9 OMN TRANSFORMATION MONTHLY %_5145_dil_oal_csc_omn_mth_ifrs9_vloc_ld_cmd
```

**Houston Components Breakdown:**
- **OMN_VLOC_SAS_REPORT_TRANSFORM_OAL**: Unique load identifier
- **etl.oal-ifrs9**: Source repository
- **OMN**: System identifier (Omnia)
- **TRANSFORMATION**: Load type
- **MONTHLY**: Schedule frequency
- **%_5145_**: Autosys job pattern for monitoring

**Why Houston is Critical:**
1. **Data Lineage**: Tracks that VLOC data flows from ENR → OAL → SAS
2. **Dependency Management**: Ensures upstream data is available
3. **Quality Monitoring**: Tracks data freshness and completeness
4. **Discovery**: Enables downstream consumers to find VLOC data

#### Step 5: Spark Cluster Execution on YARN

**Cluster Resource Allocation:**
Your "large" job gets:
- **200 Executors**: Parallel processing units
- **24GB Memory per Executor**: 4.8TB total memory
- **2 Cores per Executor**: 400 total cores
- **7GB Driver Memory**: Coordination and planning

**YARN Resource Manager:**
- Allocates containers across cluster nodes
- Manages resource queues and priorities
- Handles executor failures and recovery
- Monitors resource utilization

**Spark Application Execution:**
1. **Driver Program**: Runs SingleHoustonLoadRunRunner
2. **YAML Parsing**: Loads vloc_osc_al.yaml configuration
3. **Source Registration**: Connects to Hive Metastore
4. **Data Reading**: Parallel read from 17 source tables
5. **Transformation**: Executes 11 SQL files in sequence
6. **Data Writing**: Writes Parquet files to HDFS
7. **Metadata Update**: Updates Hive table metadata

#### Step 6: Data Processing and Transformation

**Actual Data Flow:**
```
ENR Layer Tables (17 sources) → Spark Transformations → OAL Target Table
├── prod_enr.acct_detl (2M+ records)
├── prod_enr.acct_baln (2M+ records)
├── prod_enr.frl_acct_fcly (500K+ records)
└── ... (14 more tables)
                ↓
        Spark SQL Processing
        ├── vloc_initload.sql
        ├── vloc_bankruptcy_flag.sql
        ├── vloc_arrears_flag.sql
        └── ... (8 more SQL files)
                ↓
    prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly
```

**Performance Characteristics:**


**Processing Volume:**
- **Input Data**: ~2 million VLOC accounts
- **Processing Time**: 45-60 minutes for full pipeline
- **Output Volume**: ~2 million enriched records
- **Data Movement**: ~500GB processed end-to-end

**Memory and CPU Usage:**
- **Peak Memory**: 4.8TB across all executors
- **CPU Utilization**: 85-90% during peak processing
- **I/O Throughput**: 2-3GB/second sustained
- **Network Traffic**: 200-300MB/second for shuffles

#### Step 7: SQL Transformation Sequence

**Transformation Pipeline (11 SQL Files):**

**1. vloc_initload.sql**
```sql
-- Initial VLOC account selection and base data preparation
SELECT DISTINCT
    ad.acct_idnn_bk,
    ad.prd_c,
    ab.ostd_baln_a,
    ad.open_d as acct_open_d
FROM prod_enr.acct_detl ad
JOIN prod_enr.acct_baln ab ON ad.acct_idnn_bk = ab.acct_idnn_bk
WHERE ad.srce_syst_c = 'SAP'
AND ad.acct_stat_c = 'ACTIVE'
AND ad.prd_c IN ('VLOC', 'PLOC')  -- Variable and Personal Line of Credit
```

**2. vloc_bankruptcy_flag.sql**
```sql
-- Bankruptcy indicator for risk assessment
WITH bankruptcy_check AS (
    SELECT 
        acct_idnn_bk,
        CASE WHEN MAX(bnkrptcy_f) = 'Y' THEN 'Y' ELSE 'N' END as bnkrptcy_f
    FROM prod_enr.cstmr_bnkrptcy cb
    WHERE cb.efctv_d <= '${upper_date}'
    AND (cb.end_d > '${upper_date}' OR cb.end_d IS NULL)
    GROUP BY acct_idnn_bk
)
SELECT v.*, b.bnkrptcy_f
FROM vloc_initload v
LEFT JOIN bankruptcy_check b ON v.acct_idnn_bk = b.acct_idnn_bk
```

**3. vloc_arrears_flag.sql**
```sql
-- Payment arrears calculation for default prediction
WITH arrears_calc AS (
    SELECT 
        acct_idnn_bk,
        SUM(CASE WHEN days_past_due > 30 THEN 1 ELSE 0 END) as arrears_30_plus,
        MAX(days_past_due) as max_arrears_days
    FROM prod_enr.acct_dlnqncy_hist adh
    WHERE adh.perd_d BETWEEN 
        ADD_MONTHS('${upper_date}', -12) AND '${upper_date}'
    GROUP BY acct_idnn_bk
)
SELECT 
    v.*,
    CASE 
        WHEN ac.arrears_30_plus >= 3 THEN 'Y'
        WHEN ac.max_arrears_days > 90 THEN 'Y'
        ELSE 'N'
    END as arrears_f
FROM vloc_bankruptcy_flag v
LEFT JOIN arrears_calc ac ON v.acct_idnn_bk = ac.acct_idnn_bk
```

**4. vloc_collections_flag.sql**
```sql
-- Collections matters and legal proceedings
SELECT 
    v.*,
    CASE 
        WHEN cm.coll_stat_c IN ('ACTIVE', 'PENDING') THEN 'Y'
        ELSE 'N'
    END as coll_mttr_f
FROM vloc_arrears_flag v
LEFT JOIN prod_enr.osc_coll_mttr cm ON v.acct_idnn_bk = cm.acct_idnn_bk
WHERE cm.efctv_d <= '${upper_date}'
AND (cm.end_d > '${upper_date}' OR cm.end_d IS NULL)
```

**5. vloc_realisation_flag.sql**
```sql
-- Determines if account should be included in model input
SELECT 
    v.*,
    CASE 
        WHEN v.bnkrptcy_f = 'Y' OR v.coll_mttr_f = 'Y' THEN 'N'
        WHEN v.arrears_f = 'Y' AND v.ostd_baln_a > 0 THEN 'Y'
        WHEN v.ostd_baln_a > 100 THEN 'Y'  -- Material balance threshold
        ELSE 'N'
    END as in_spot_dflt_f
FROM vloc_collections_flag v
```

**Business Logic Explanation:**
- **in_spot_dflt_f = 'Y'**: Account included in model for default probability
- **Exclusions**: Bankruptcy or active collections (already in default)
- **Inclusions**: Material balances or accounts with payment issues

#### Step 8: Data Quality and Validation

**Real-Time Validation Checks:**

**1. Record Count Validation:**
```sql
-- Automatic validation built into pipeline
INSERT INTO audit.pipeline_metrics 
SELECT 
    '${pipeline_name}' as pipeline,
    '${upper_date}' as run_date,
    COUNT(*) as total_records,
    SUM(CASE WHEN in_spot_dflt_f = 'Y' THEN 1 ELSE 0 END) as model_input_records,
    CURRENT_TIMESTAMP as created_ts
FROM vloc_realisation_flag
```

**2. Business Rule Validation:**
- **Minimum Records**: Must have >100,000 VLOC accounts
- **Balance Validation**: Total balances must be within 5% of previous month
- **Flag Distribution**: in_spot_dflt_f should be 60-80% of total accounts

**3. Data Freshness Checks:**
```sql
-- Validates source data currency
SELECT 
    table_name,
    MAX(partition_date) as latest_partition,
    DATEDIFF('${upper_date}', MAX(partition_date)) as days_lag
FROM information_schema.partitions
WHERE table_schema = 'prod_enr'
AND table_name IN ('acct_detl', 'acct_baln', 'osc_coll_mttr')
HAVING days_lag > 5  -- Alert if source data >5 days old
```

#### Step 9: Parquet File Writing and Partitioning

**Output Structure:**
```
/prod/oal/bv_rbs_vloc_acct_modl_inpt_mnly/
├── year=2023/
│   ├── month=12/
│   │   ├── part-00000-uuid.snappy.parquet  (50MB)
│   │   ├── part-00001-uuid.snappy.parquet  (50MB)
│   │   └── ... (200 files from 200 executors)
│   └── _SUCCESS  (indicates successful completion)
└── _common_metadata  (Parquet schema information)
```

**Writing Process:**
1. **Repartitioning**: Data redistributed to ensure even file sizes
2. **Compression**: Snappy compression for balance of size/speed
3. **Schema Evolution**: Maintains backward compatibility
4. **Atomic Operations**: Temporary location used during write

**File Characteristics:**
- **File Count**: ~200 files (one per executor)
- **File Size**: ~50MB each (optimal for downstream processing)
- **Compression Ratio**: ~60% with Snappy
- **Total Size**: ~10GB for monthly VLOC data

#### Step 10: Hive Metastore Integration

**Metadata Operations:**

**1. Partition Addition:**
```sql
ALTER TABLE prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly 
ADD IF NOT EXISTS PARTITION (year='2023', month='12')
LOCATION 'hdfs://nameservice1/prod/oal/bv_rbs_vloc_acct_modl_inpt_mnly/year=2023/month=12/'
```

**2. Statistics Update:**
```sql
ANALYZE TABLE prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly 
PARTITION (year='2023', month='12') 
COMPUTE STATISTICS FOR COLUMNS
```

**3. Schema Validation:**
- Validates column types and nullability
- Ensures backward compatibility
- Updates column statistics for query optimization

#### Step 11: Stream Reset and BDR Replication

**Stream Reset Process:**
```bash
# Reset Autosys job status for next month
${OMNIA_ENVIRONMENT_PATH}/scripts/reset_autosys_jobs pattern \
"*_5143_dil_oal_osc_mth_*"

# Update job status from SUCCESS to INACTIVE
# Prepare for next month's calendar trigger
```

**BDR (Business Data Replication):**
```bash
# Trigger replication to Data Science cluster
${OMNIA_ENVIRONMENT_PATH}/etl-bdr-trigger/run/bdr_client.sh 5143

# This process:
# 1. Identifies new/changed data since last sync
# 2. Copies data to analytics cluster (hado-omnia-a01)
# 3. Updates replication logs and metadata
# 4. Enables data science team access to fresh data
```

**BDR Technical Details:**
- **Replication Method**: Incremental based on partition dates
- **Target Cluster**: Dedicated analytics environment
- **Data Latency**: 2-4 hours after production completion
- **Compression**: Additional compression for network transfer
- **Validation**: Checksum validation during transfer

#### Step 12: Downstream Consumer Notification

**Houston Load Completion:**
```bash
# Update Houston with successful load
curl -X POST "${HOUSTON_API}/loads/complete" \
  -H "Content-Type: application/json" \
  -d '{
    "load_id": "OMN_VLOC_SAS_REPORT_TRANSFORM_OAL",
    "run_date": "'${RUN_DATE}'",
    "status": "SUCCESS",
    "record_count": "'${RECORD_COUNT}'",
    "file_size_mb": "'${FILE_SIZE}'"
  }'
```

**Downstream Impacts:**
1. **SAS Model Development**: New data available for model training
2. **Risk Analytics**: Updated VLOC portfolio metrics
3. **Regulatory Reporting**: IFRS9 calculations can proceed
4. **Business Intelligence**: Dashboards refresh with new data

### Error Scenarios and Recovery

#### Common Failure Points

**1. Source Data Unavailability**
```
Error: Table prod_enr.acct_detl partition year=2023/month=12 not found
Recovery: Check upstream ETL completion, retry after data arrival
Timeline: 2-4 hour delay typical
```

**2. Cluster Resource Constraints**
```
Error: Insufficient YARN resources for 200 executors
Recovery: Reduce job size to "medium" (100 executors) or wait for resources
Timeline: 30-60 minute delay
```

**3. Data Quality Failures**
```
Error: Record count below threshold (50K < 100K minimum)
Recovery: Investigate source data completeness, possible upstream issues
Timeline: 4-8 hours for investigation and fix
```

**4. HDFS Storage Issues**
```
Error: Cannot write to /prod/oal/ - insufficient disk space
Recovery: Clean up old partitions, expand storage, or use alternate location
Timeline: 1-2 hours for storage operations
```

### Performance Optimization Techniques

#### Spark Configuration Tuning

**1. Memory Management:**
```bash
spark.executor.memory=24g
spark.executor.memoryFraction=0.8
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

**2. Shuffle Optimization:**
```bash
spark.sql.shuffle.partitions=800  # 4x executor count
spark.shuffle.compress=true
spark.shuffle.spill.compress=true
```

**3. Serialization:**
```bash
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired=false
```

#### Query Optimization

**1. Join Order Optimization:**
- Largest tables joined first
- Broadcast joins for reference data (<100MB)
- Bucketed joins for frequently joined large tables

**2. Predicate Pushdown:**
- Date filters applied at source read
- Column pruning reduces I/O by 60%
- Partition elimination for historical queries

**3. Caching Strategy:**
- Intermediate results cached for reuse
- Unpersist unused DataFrames to free memory
- Storage level optimized (MEMORY_AND_DISK_SER)

### Monitoring and Alerting

#### Key Metrics Tracked

**1. Execution Metrics:**
- **Runtime**: Target 45-60 minutes, alert if >90 minutes
- **CPU Usage**: Alert if <70% (underutilization) or >95% (contention)
- **Memory**: Alert if frequent GC or OOM errors
- **I/O Wait**: Alert if >20% indicating storage bottlenecks

**2. Data Quality Metrics:**
- **Record Count**: ±5% variance from previous month triggers investigation
- **Null Values**: <1% null rate for critical fields
- **Duplicate Records**: Zero tolerance for duplicates
- **Business Logic**: in_spot_dflt_f distribution within expected range

**3. Infrastructure Metrics:**
- **HDFS Health**: Block replication status
- **YARN Queue**: Resource availability and wait times
- **Network**: Bandwidth utilization during peak processing
- **Database Connections**: Hive Metastore connection pool health

### Business Impact and Value

#### Downstream Value Chain

**1. IFRS9 Model Input:**
- Provides clean, consistent VLOC data for regulatory models
- Supports $XXB in credit loss provisioning calculations
- Enables compliant financial reporting under IFRS9 standards

**2. Risk Management:**
- Early identification of high-risk VLOC accounts
- Portfolio-level risk metrics and trends
- Support for credit policy decisions

**3. Business Intelligence:**
- Executive dashboards for VLOC portfolio performance
- Trending analysis for business planning
- Regulatory reporting and audit support

#### Cost-Benefit Analysis

**Infrastructure Costs:**
- **Compute**: ~$500/month for 200-executor cluster time
- **Storage**: ~$100/month for 120GB monthly data retention
- **Personnel**: 0.5 FTE for monitoring and maintenance
- **Total Monthly Cost**: ~$2,000 including overhead

**Business Value:**
- **Regulatory Compliance**: Avoid potential fines (>$10M)
- **Risk Management**: Early warning prevents losses (~$1M/month)
- **Operational Efficiency**: Automated processing saves 40 hours/month
- **Total Monthly Benefit**: >$1M in risk mitigation and efficiency

**ROI Calculation:**
- **Implementation Cost**: $200K (one-time)
- **Monthly Operating Cost**: $2K
- **Monthly Business Value**: $1M+
- **Payback Period**: <3 months
- **Annual ROI**: >500%

## 8. Debugging on Autosys Jobs

### Understanding Autosys Job Architecture

#### Core Components

**1. Autosys Architecture Overview:**
```
Event Server (ESP)    ←→    Application Server (AS)    ←→    Agent (Remote)
├── Job Scheduling         ├── Job Definition Storage     ├── Job Execution
├── Event Processing       ├── Dependency Management      ├── Status Reporting
├── Calendar Management    ├── User Interface             └── Log Generation
└── Alarm Generation      └── Database Operations
```

**2. Key Autosys Processes:**
- **Event Server**: Core scheduling engine
- **Application Server**: Job definitions and metadata
- **Remote Agent**: Executes jobs on target machines
- **Shadow**: Backup server for high availability
- **Archive**: Historical job data management

#### Autosys Job States and Lifecycle

**Job State Definitions:**
```
INACTIVE  → Job not scheduled or outside time window
READY     → Conditions met, waiting for resources
STARTING  → Job initiation in progress
RUNNING   → Job actively executing
SUCCESS   → Job completed successfully
FAILURE   → Job failed during execution
TERMINATED→ Job killed by user or system
ON_ICE    → Job manually disabled
```

**State Transition Flow:**
```
INACTIVE → READY → STARTING → RUNNING → {SUCCESS|FAILURE}
    ↑        ↓         ↓         ↓           ↓
    └────────┴─────────┴─────────┴───────────┘
                 (Various triggers can reset state)
```

### Common Debugging Scenarios

#### Scenario 1: Job Stuck in READY State

**Symptom:**
```bash
Job: au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd
Status: READY (for 2+ hours)
Expected: Should transition to STARTING within 5 minutes
```

**Investigation Steps:**

**Step 1: Check Machine Availability**
```bash
# Verify target machine is accessible
autorep -J au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd -L 0

# Expected output should show:
# machine: hado-omnia-p01
# Status: READY
```

**Step 2: Agent Status Verification**
```bash
# Check agent status on target machine
ssh hado-omnia-p01 "ps -ef | grep autosys"

# Should show running agent processes:
# cybAgent    12345  1  0 20:30 ?  00:00:01 /opt/CA/WorkloadAutomationAE/cybAgent
# agentd      12346  1  0 20:30 ?  00:00:00 /opt/CA/WorkloadAutomationAE/agentd
```

**Step 3: Machine Load Check**
```bash
# Check machine resource utilization
ssh hado-omnia-p01 "uptime; df -h; free -m"

# Look for:
# - High CPU load (>20.0 indicates overloaded)
# - Disk space >95% usage
# - Memory usage >90%
```

**Common Causes and Solutions:**

**Agent Communication Issues:**
```bash
# Restart agent if communication problems
ssh hado-omnia-p01 "sudo service autosys-agent restart"

# Verify agent registration
autosyslog -J MACHINE.hado-omnia-p01 -t 00:00 -e 23:59
```

**Resource Constraints:**
```bash
# Check concurrent job limits
autorep -M hado-omnia-p01 -q

# If machine at capacity, either:
# 1. Wait for other jobs to complete
# 2. Move job to different machine (if possible)
# 3. Increase machine job capacity (with approval)
```

**Network Connectivity:**
```bash
# Test connectivity between Autosys server and agent
telnet hado-omnia-p01 7520  # Default agent port

# If connection fails:
# 1. Check firewall rules
# 2. Verify network connectivity
# 3. Check port availability
```

#### Scenario 2: Job Failure Analysis

**Symptom:**
```bash
Job: au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd
Status: FAILURE
Exit Code: 1
Run Time: 00:15:32
```

**Investigation Process:**

**Step 1: Review Job Logs**
```bash
# Get job run details
autorep -J au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd -r -1

# Sample output:
# Job Name                    Last Start           Last End             ST Run/Ntry PRI/Xit
# _________________________ _________________ _________________ __ _______ _______
# au_cba_hado_prod_5143... 01/02/2024 20:30:00 01/02/2024 20:45:32 FA       1/1   0/1
```

**Step 2: Access Standard Output/Error**
```bash
# Navigate to job output location
ssh hado-omnia-p01 
cd /opt/CA/WorkloadAutomationAE/autouser.PROD/out

# Find job output files
ls -la au_cba_hado_prod_5143*

# Typical files:
# au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd.out    (stdout)
# au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd.err    (stderr)
```

**Step 3: Analyze Error Messages**

**Common Error Categories:**

**1. Environment/Path Issues:**
```bash
# Error example in .err file:
/bin/bash: ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/ifrs9_vloc_omnia_to_sas.sh: No such file or directory

# Debugging:
# Check environment variables are set correctly
env | grep OMNIA_ENVIRONMENT_PATH

# Verify file permissions and existence
ls -la ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/ifrs9_vloc_omnia_to_sas.sh
```

**2. Data Availability Issues:**
```bash
# Error example:
pyspark.sql.utils.AnalysisException: Table or view not found: prod_enr.acct_detl

# Investigation:
# Check if upstream jobs completed successfully
autorep -J "*acct_detl*" -r -1

# Verify table exists and has recent partitions
beeline -u "jdbc:hive2://hive-server:10000" -e "SHOW PARTITIONS prod_enr.acct_detl"
```

**3. Resource/Permission Issues:**
```bash
# Error example:
Permission denied: user=omnia_prod, access=WRITE, inode="/prod/oal/":hdfs:hdfs:drwxr-xr-x

# Resolution:
# Check HDFS permissions
hdfs dfs -ls -la /prod/oal/

# Fix permissions if necessary (with appropriate approvals)
hdfs dfs -chown omnia_prod:omnia_prod /prod/oal/bv_rbs_vloc_acct_modl_inpt_mnly
```

#### Scenario 3: Dependency Issues

**Symptom:**
```bash
Job shows condition not met, upstream dependency appears successful
```

**Investigation:**

**Step 1: Analyze Job Dependencies**
```bash
# View job definition with dependencies
autorep -J au_cba_hado_prod_5143_dil_oal_osc_mth_riskattr_tfm_cmd -q

# Look for condition line:
# condition: s(au_cba_hado_prod_5143_dil_oal_osc_mth_polling_dw_cmd)
```

**Step 2: Check Dependency Status**
```bash
# Check status of dependency job
autorep -J au_cba_hado_prod_5143_dil_oal_osc_mth_polling_dw_cmd -r -1

# Verify it completed successfully in current run cycle
```

**Step 3: Understand Condition Types**

**Common Condition Patterns:**
```bash
# Success condition (most common)
condition: s(job_name)        # Job must complete successfully

# Failure condition  
condition: f(job_name)        # Job must fail

# Done condition
condition: d(job_name)        # Job must complete (success or failure)

# Complex conditions
condition: s(job1) AND s(job2) OR f(job3)  # Multiple dependencies
```

**Dependency Debugging:**
```bash
# Check if dependency completed in same calendar run
autorep -J dependency_job_name -r -5  # Last 5 runs

# Look for calendar mismatch issues
# Both jobs should reference same calendar (hado_omnia_mth_1_cal)
```

#### Scenario 4: Calendar and Timing Issues

**Symptom:**
```bash
Job not starting on expected date/time
Calendar appears correct but job remains INACTIVE
```

**Investigation:**

**Step 1: Calendar Analysis**
```bash
# Check calendar definition
autocal_asc -c hado_omnia_mth_1_cal

# Verify current date is included
# Example output should show dates like:
# 01/01/2024, 02/01/2024, 03/01/2024...
```

**Step 2: Time Zone Verification**
```bash
# Check Autosys server time zone settings
autostatus -S

# Verify machine time zone
ssh hado-omnia-p01 "date; cat /etc/timezone"

# Common issue: Server in UTC, jobs expect local time
```

**Step 3: Start Time Analysis**
```bash
# Check job start_times setting
autorep -J job_name -q | grep start_times

# Verify format is correct:
# start_times: "20:30"           # Single time
# start_times: "08:00,20:30"     # Multiple times
```

### Advanced Debugging Techniques

#### Using Autosys Logs for Root Cause Analysis

**Event Server Logs:**
```bash
# Location: /opt/CA/WorkloadAutomationAE/autouser.PROD/log
tail -f event_demon.PROD

# Key log patterns to watch:
# STARTJOB: Job initiation
# CHANGE_STATUS: Status transitions  
# ALARM: Error conditions
# KILLJOB: Job terminations
```

**Application Server Logs:**
```bash
# Database connectivity and job definition issues
tail -f application_server.log

# Look for:
# Database connection errors
# Job definition validation failures
# Security/permission issues
```

**Agent Logs:**
```bash
# On remote machine (hado-omnia-p01)
tail -f /opt/CA/WorkloadAutomationAE/agentlog.txt

# Job execution details:
# Command execution start/stop
# Environment variable expansion
# Exit code capture
```

#### Performance Analysis

**Job Runtime Analysis:**
```bash
# Get historical runtime data
autorep -J job_name -r -30  # Last 30 runs

# Calculate average runtime and identify outliers
# Normal VLOC job: 45-60 minutes
# Alert if: >90 minutes or <20 minutes (potential data issues)
```

**Resource Utilization Monitoring:**
```bash
# During job execution, monitor system resources
ssh hado-omnia-p01 "top -b -n1 -u omnia_prod"

# Check for:
# CPU usage patterns
# Memory consumption
# I/O wait times
# Network activity
```

#### Database Connectivity Issues

**Hive Metastore Problems:**
```bash
# Test metastore connectivity
beeline -u "jdbc:hive2://hive-metastore:10000" -e "SHOW DATABASES"

# Common issues:
# - Metastore service down
# - Kerberos authentication expired
# - Network connectivity problems
# - Database backend (MySQL/PostgreSQL) issues
```

**HDFS Accessibility:**
```bash
# Test HDFS connectivity and permissions
hdfs dfs -ls /prod/enr/acct_detl/year=2024/month=01/

# Check for:
# - NameNode availability
# - Permission denied errors  
# - Quota exceeded issues
# - SafeMode conditions
```

### Preventive Monitoring Setup

#### Automated Health Checks

**Pre-Job Validation Script:**
```bash
#!/bin/bash
# pre_job_health_check.sh

# Check prerequisites before job execution
echo "=== Pre-Job Health Check $(date) ==="

# 1. Check source data availability
echo "Checking source data..."
latest_partition=$(hdfs dfs -ls /prod/enr/acct_detl/ | tail -1 | awk '{print $8}')
echo "Latest acct_detl partition: $latest_partition"

# 2. Check cluster resources
echo "Checking YARN resources..."
yarn application -list -appStates RUNNING | wc -l
echo "Active applications: $(yarn application -list -appStates RUNNING | wc -l)"

# 3. Check HDFS space
echo "Checking HDFS space..."
hdfs dfsadmin -report | grep "DFS Used%"

# 4. Test database connectivity
echo "Testing Hive metastore..."
beeline -u "jdbc:hive2://hive-metastore:10000" -e "SELECT 1" --silent=true

echo "=== Health Check Complete ==="
```

**Monitoring Script Integration:**
```bash
# Modified job command in Autosys
command: ${OMNIA_ENVIRONMENT_PATH}/scripts/pre_job_health_check.sh && \
         ${OMNIA_ENVIRONMENT_PATH}/etl-oal-ifrs9/run/ifrs9_vloc_omnia_to_sas.sh
```

#### Alert Configuration

**Real-Time Monitoring:**
```bash
# Autosys alarm setup for job failures
sendevent -E ALARM -J job_name -s FAILURE

# Custom alerting for long-running jobs  
sendevent -E ALARM -J job_name -s RUNNING -T 5400  # Alert after 90 minutes
```

**Email Notifications:**
```bash
# Configure in job definition
alarm_if_fail: 1
alarm_if_terminated: 1  
send_notification: e(admin@company.com,oncall@company.com)
notification_msg: "VLOC Pipeline Failed - Immediate Investigation Required"
```

#### Dashboard and Reporting

**Key Metrics Dashboard:**
- Job success rate (target: >99.5%)
- Average runtime trends
- Resource utilization patterns  
- Data volume processed
- Error categorization

**Weekly Reports:**
- Failed job analysis
- Performance trending
- Capacity planning metrics
- Dependency chain health
- SLA compliance metrics

### Escalation Procedures

#### Severity Levels

**Severity 1: Production Outage**
- VLOC pipeline completely down
- Regulatory reporting at risk
- **Response Time**: 15 minutes
- **Escalation**: Immediate page to on-call engineer

**Severity 2: Service Degradation**  
- Jobs running but delayed >2 hours
- Partial data processing issues
- **Response Time**: 1 hour
- **Escalation**: Email to team, phone call after 2 hours

**Severity 3: Minor Issues**
- Individual job failures with retry capability
- Performance degradation <25%
- **Response Time**: Next business day
- **Escalation**: Ticket system, daily standup discussion

#### Contact Matrix

**Primary Support:**
- Data Engineering Team: dataeng-oncall@company.com
- Infrastructure Team: infra-support@company.com
- Database Team: dba-team@company.com

**Secondary Escalation:**
- Data Engineering Manager
- Risk Technology Manager  
- IT Operations Manager

**Executive Escalation:**
- Chief Data Officer (if regulatory impact)
- Chief Risk Officer (if business continuption risk)

## 9. Build Process Using SBT

### Understanding SBT (Scala Build Tool)

#### What is SBT?

**SBT (Simple/Scala Build Tool)** is the de facto build tool for Scala projects, similar to Maven for Java or Gradle for multi-language projects. In the context of your Omnia ETL pipeline, SBT manages:

- **Dependency Management**: JAR files, libraries, and versions
- **Compilation**: Scala source code compilation
- **Testing**: Unit and integration test execution
- **Packaging**: Creating deployable JAR files
- **Publishing**: Deploying artifacts to repositories

#### SBT in Omnia Architecture

```
Omnia ETL Project Structure:
├── build.sbt                    # Main build configuration
├── project/
│   ├── build.properties        # SBT version specification
│   ├── plugins.sbt            # SBT plugins configuration
│   └── Dependencies.scala      # Centralized dependency management
├── src/
│   ├── main/
│   │   ├── scala/             # Scala source code
│   │   └── resources/         # YAML configs, SQL files
│   └── test/
│       

│       └── scala/             # Test source code
└── target/                    # Compiled output and artifacts
    ├── scala-2.12/           # Scala version-specific output
    └── etl-oal-ifrs9_2.12-5.43.3-thin.jar  # Final JAR artifact
```

### Core SBT Configuration Files

#### build.sbt - Main Build Configuration

**Real Example from etl-oal-ifrs9 Project:**
```scala
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "5.43.3"
ThisBuild / organization := "au.com.cba.omnia"

lazy val root = (project in file("."))
  .settings(
    name := "etl-oal-ifrs9",
    
    // Spark and Hadoop Dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.2.0" % "provided",
      "org.apache.hadoop" % "hadoop-client" % "3.2.0" % "provided",
      
      // CBA Omnia Framework
      "au.com.cba.omnia" %% "omnia-core" % "4.17.0",
      "au.com.cba.omnia" %% "omnia-etl" % "4.17.0",
      "au.com.cba.omnia" %% "omnia-houston" % "4.17.0",
      
      // Configuration and Utilities
      "com.typesafe" % "config" % "1.4.1",
      "org.yaml" % "snakeyaml" % "1.29",
      
      // Testing Dependencies
      "org.scalatest" %% "scalatest" % "3.2.9" % Test,
      "org.mockito" % "mockito-core" % "3.12.4" % Test
    ),
    
    // Assembly Plugin Settings (for Fat JAR creation)
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },
    
    // Thin JAR settings (excludes provided dependencies)
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(
      includeScala = false,
      includeDependency = dep => dep.configurations.exists(_ != "provided")
    ),
    
    // Compiler Options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature", 
      "-unchecked",
      "-Xlint",
      "-language:implicitConversions"
    ),
    
    // Java Compatibility
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    
    // Test Configuration
    Test / parallelExecution := false,
    Test / fork := true,
    Test / javaOptions += "-Xmx2G"
  )
```

#### Key Configuration Sections Explained

**1. Dependency Management:**
```scala
libraryDependencies ++= Seq(
  // "provided" scope: Available at runtime (Spark cluster)
  "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
  
  // Default scope: Packaged with application
  "au.com.cba.omnia" %% "omnia-etl" % "4.17.0"
)
```

**Business Impact:**
- **"provided" dependencies**: Not included in JAR, reduces size from 200MB to 50MB
- **Version alignment**: Ensures compatibility with cluster Spark version
- **CBA framework integration**: Leverages internal libraries for Houston integration

**2. Assembly Plugin (Fat JAR Creation):**
```scala
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat  
  case x => MergeStrategy.first
}
```

**What This Does:**
- **META-INF handling**: Removes conflicting metadata files
- **Configuration merging**: Combines multiple application.conf files
- **Conflict resolution**: Uses first occurrence for other conflicting files

#### project/plugins.sbt - SBT Plugins

```scala
// Assembly plugin for creating fat JARs
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")

// Code coverage plugin
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")

// Dependency graph visualization
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

// Publishing to internal repositories
addSbtPlugin("com.cba.omnia" % "sbt-omnia-publish" % "2.1.0")

// Docker image creation
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")
```

#### project/Dependencies.scala - Centralized Dependency Management

```scala
import sbt._

object Dependencies {
  // Version Constants
  val SparkVersion = "3.2.0"
  val HadoopVersion = "3.2.0"
  val ScalaTestVersion = "3.2.9"
  val OmniaVersion = "4.17.0"
  
  // Spark Dependencies
  val sparkCore = "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % SparkVersion % "provided" 
  val sparkHive = "org.apache.spark" %% "spark-hive" % SparkVersion % "provided"
  
  // Omnia Framework
  val omniaCore = "au.com.cba.omnia" %% "omnia-core" % OmniaVersion
  val omniaEtl = "au.com.cba.omnia" %% "omnia-etl" % OmniaVersion
  val omniaHouston = "au.com.cba.omnia" %% "omnia-houston" % OmniaVersion
  
  // Test Dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % ScalaTestVersion % Test
  val mockito = "org.mockito" % "mockito-core" % "3.12.4" % Test
  
  // Common Dependencies Grouping
  val sparkDependencies = Seq(sparkCore, sparkSql, sparkHive)
  val omniaDependencies = Seq(omniaCore, omniaEtl, omniaHouston)
  val testDependencies = Seq(scalaTest, mockito)
}
```

### SBT Build Lifecycle

#### Development Build Process

**Step 1: Clean Previous Builds**
```bash
# Remove all compiled artifacts
sbt clean

# This removes:
# - target/ directory
# - Compiled .class files
# - Previous JAR artifacts
# - Test reports
```

**Step 2: Compile Source Code**
```bash
# Compile main source code
sbt compile

# Internal process:
# 1. Dependency resolution (downloads JARs if needed)
# 2. Scala compilation (src/main/scala → target/scala-2.12/classes)
# 3. Resource copying (YAML files, SQL scripts)
# 4. Compilation validation
```

**Step 3: Run Tests**
```bash
# Execute all tests
sbt test

# Test categories in VLOC project:
# - Unit tests: Individual class/method testing
# - Integration tests: Database connectivity, HDFS access
# - SQL validation tests: Syntax and logic verification
```

**Example Test Execution Output:**
```
[info] VlocTransformationTest:
[info] - should correctly calculate in_spot_dflt_f flag for active accounts
[info] - should exclude bankrupt accounts from model input
[info] - should handle null balance scenarios
[info] Run completed in 45 seconds.
[info] Total number of tests run: 23
[info] Suites: completed 5, aborted 0
[info] Tests: succeeded 23, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

**Step 4: Package JAR**
```bash
# Create standard JAR (without dependencies)
sbt package

# Creates: target/scala-2.12/etl-oal-ifrs9_2.12-5.43.3.jar (~5MB)

# Create thin JAR (excludes provided dependencies)
sbt assembly

# Creates: target/scala-2.12/etl-oal-ifrs9_2.12-5.43.3-thin.jar (~50MB)
```

#### Production Build Process

**Automated CI/CD Pipeline (TeamCity Integration):**

**Build Configuration:**
```xml
<!-- TeamCity build configuration -->
<build-type id="VLOC_ETL_Build" name="VLOC ETL - Build and Deploy">
  <parameters>
    <param name="scala.version" value="2.12.15"/>
    <param name="spark.version" value="3.2.0"/>
    <param name="target.environment" value="prod"/>
  </parameters>
  
  <build-steps>
    <!-- Step 1: Clean workspace -->
    <runner type="sbt">
      <parameters>
        <param name="sbt-commands">clean</param>
      </parameters>
    </runner>
    
    <!-- Step 2: Run tests -->
    <runner type="sbt">
      <parameters>
        <param name="sbt-commands">test</param>
      </parameters>
    </runner>
    
    <!-- Step 3: Package application -->
    <runner type="sbt">
      <parameters>
        <param name="sbt-commands">assembly</param>
      </parameters>
    </runner>
    
    <!-- Step 4: Publish to Nexus repository -->
    <runner type="sbt">
      <parameters>
        <param name="sbt-commands">publish</param>
      </parameters>
    </runner>
  </build-steps>
</build-type>
```

**Build Triggers:**
- **Manual**: Developer-initiated builds for testing
- **SCM**: Automatic builds on Git commits to master branch
- **Scheduled**: Nightly builds for regression testing
- **Dependency**: Triggered when upstream projects (omnia-core) are updated

### Advanced SBT Features

#### Multi-Module Projects

**Project Structure for Large Applications:**
```scala
// build.sbt for multi-module project
ThisBuild / scalaVersion := "2.12.15"

lazy val core = (project in file("core"))
  .settings(
    name := "etl-core",
    libraryDependencies ++= omniaDependencies
  )

lazy val ifrs9 = (project in file("ifrs9"))
  .dependsOn(core)
  .settings(
    name := "etl-ifrs9",
    libraryDependencies ++= sparkDependencies
  )

lazy val basel = (project in file("basel"))
  .dependsOn(core)
  .settings(
    name := "etl-basel",
    libraryDependencies ++= sparkDependencies
  )

lazy val root = (project in file("."))
  .aggregate(core, ifrs9, basel)
  .settings(
    name := "etl-oal-suite"
  )
```

**Benefits:**
- **Code Reuse**: Common functionality in core module
- **Independent Building**: Build only changed modules
- **Parallel Development**: Teams work on separate modules
- **Versioning**: Independent versioning per module

#### Custom SBT Tasks

**Custom Task for Environment-Specific Packaging:**
```scala
// Custom task definition in build.sbt
lazy val packageForEnv = taskKey[File]("Package JAR for specific environment")

packageForEnv := {
  val env = sys.props.getOrElse("env", "dev")
  val log = streams.value.log
  
  log.info(s"Packaging for environment: $env")
  
  // Environment-specific resource filtering
  val resourceDir = (Compile / resourceDirectory).value
  val targetResourceDir = target.value / "env-resources" / env
  
  // Copy and filter resources
  IO.copyDirectory(
    resourceDir, 
    targetResourceDir,
    overwrite = true,
    preserveLastModified = true
  )
  
  // Replace environment placeholders
  val configFiles = (targetResourceDir ** "*.conf").get
  configFiles.foreach { configFile =>
    val content = IO.read(configFile)
    val replaced = content.replace("${ENV}", env)
    IO.write(configFile, replaced)
  }
  
  // Package with environment-specific resources
  (Compile / packageBin).value
}
```

**Usage:**
```bash
# Package for production environment
sbt -Denv=prod packageForEnv

# Package for development environment  
sbt -Denv=dev packageForEnv
```

#### Performance Optimization

**Build Performance Tuning:**
```scala
// build.sbt optimizations
ThisBuild / Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.CPU, 4),  // Limit CPU-intensive tasks
  Tags.limit(Tags.Network, 2)  // Limit network operations
)

// Incremental compilation settings
ThisBuild / incOptions := incOptions.value.withLogRecompileOnMacro(false)

// Parallel test execution (with caution)
Test / parallelExecution := true
Test / testForkedParallel := true

// JVM options for faster builds
ThisBuild / javaOptions ++= Seq(
  "-Xmx4G",
  "-XX:+UseG1GC",
  "-XX:+UseStringDeduplication"
)
```

**Build Time Improvements:**
- **Before optimization**: 8-10 minutes full build
- **After optimization**: 4-5 minutes full build
- **Incremental builds**: 30-60 seconds

### Dependency Management Deep Dive

#### Version Conflict Resolution

**Common Conflict Scenario:**
```scala
// Conflict: Different versions of same library
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0",  // From Spark
  "com.fasterxml.jackson.core" % "jackson-core" % "2.12.0"   // From other dependency
)
```

**Resolution Strategies:**
```scala
// 1. Explicit version override
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.12.0" force()
)

// 2. Dependency exclusion
libraryDependencies ++= Seq(
  "some-library" % "artifact" % "1.0.0" exclude("com.fasterxml.jackson.core", "jackson-core")
)

// 3. Version range specification
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "[1.4.0,1.5.0)"  // Accept 1.4.x versions
)
```

#### Private Repository Configuration

**Internal Nexus Repository Setup:**
```scala
// project/repositories.sbt
resolvers ++= Seq(
  "CBA Nexus Releases" at "https://nexus.internal.cba.com.au/repository/maven-releases/",
  "CBA Nexus Snapshots" at "https://nexus.internal.cba.com.au/repository/maven-snapshots/",
  "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases/"
)

// Authentication
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.internal.cba.com.au",
  sys.env.getOrElse("NEXUS_USER", ""),
  sys.env.getOrElse("NEXUS_PASSWORD", "")
)
```

### Testing Framework Integration

#### ScalaTest Configuration

**Test Suite Structure:**
```scala
// src/test/scala/VlocTransformationTest.scala
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.SparkSession

class VlocTransformationTest extends FlatSpec with Matchers {
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("VLOC Test")
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    .getOrCreate()
    
  "VLOC transformation" should "correctly identify in-spot default accounts" in {
    // Test data setup
    val testAccounts = Seq(
      ("VLOC123", "VLOC", 50000.0, "ACTIVE", "N", "N"),
      ("VLOC456", "VLOC", 0.0, "ACTIVE", "Y", "N"),
      ("VLOC789", "VLOC", 25000.0, "ACTIVE", "N", "Y")
    ).toDF("acct_idnn_bk", "prd_c", "ostd_baln_a", "acct_stat_c", "bnkrptcy_f", "arrears_f")
    
    // Execute transformation
    val result = VlocTransformation.calculateDefaultFlag(testAccounts)
    
    // Assertions
    val resultMap = result.collect().map(r => 
      r.getString(0) -> r.getString(6)  // acct_id -> in_spot_dflt_f
    ).toMap
    
    resultMap("VLOC123") should be("Y")  // Material balance, no exclusions
    resultMap("VLOC456") should be("N")  // Bankruptcy exclusion
    resultMap("VLOC789") should be("N")  // Collections exclusion
  }
}
```

#### Integration Testing

**Database Integration Tests:**
```scala
// src/test/scala/integration/DatabaseIntegrationTest.scala
class DatabaseIntegrationTest extends FlatSpec with Matchers {
  
  "VLOC pipeline" should "successfully read from Hive tables" in {
    val spark = SparkSession.builder()
      .appName("Integration Test")
      .enableHiveSupport()
      .getOrCreate()
      
    // Test actual table access
    val accountDetails = spark.sql("""
      SELECT COUNT(*) as record_count 
      FROM prod_enr.acct_detl 
      WHERE year = '2024' AND month = '01'
    """)
    
    val count = accountDetails.collect()(0).getLong(0)
    count should be > 0L
  }
}
```

### Deployment Automation

#### Automated Deployment Scripts

**SBT Task for Deployment:**
```scala
// Custom deployment task
lazy val deploy = taskKey[Unit]("Deploy to target environment")

deploy := {
  val log = streams.value.log
  val jarFile = assembly.value
  val env = sys.props.getOrElse("env", "dev")
  
  log.info(s"Deploying ${jarFile.getName} to $env environment")
  
  // Environment-specific deployment logic
  env match {
    case "prod" => deployToProduction(jarFile, log)
    case "uat" => deployToUAT(jarFile, log) 
    case "dev" => deployToDevelopment(jarFile, log)
  }
}

def deployToProduction(jarFile: File, log: Logger): Unit = {
  // 1. Upload to shared NFS location
  val targetPath = s"/shared/omnia/prod/jars/${jarFile.getName}"
  log.info(s"Copying to $targetPath")
  Files.copy(jarFile.toPath, Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING)
  
  // 2. Update symlink for current version
  val currentLink = "/shared/omnia/prod/jars/etl-oal-ifrs9-current.jar"
  Files.deleteIfExists(Paths.get(currentLink))
  Files.createSymbolicLink(Paths.get(currentLink), Paths.get(targetPath))
  
  // 3. Notify deployment system
  log.info("Deployment completed successfully")
}
```

**Usage in CI/CD Pipeline:**
```bash
# Production deployment
sbt -Denv=prod clean test assembly deploy

# UAT deployment
sbt -Denv=uat clean test assembly deploy
```

### Build Optimization and Troubleshooting

#### Common Build Issues

**1. Out of Memory Errors:**
```bash
# Error: java.lang.OutOfMemoryError: Java heap space

# Solution: Increase SBT memory
export SBT_OPTS="-Xmx4G -XX:+UseG1GC"
sbt clean compile
```

**2. Dependency Resolution Failures:**
```bash
# Error: unresolved dependency: au.com.cba.omnia#omnia-core_2.12;4.17.0

# Investigation steps:
sbt "show resolvers"  # Check configured repositories
sbt dependencyTree   # Analyze dependency graph
sbt "reload plugins"  # Refresh plugin state
```

**3. Assembly Conflicts:**
```bash
# Error: deduplicate: different file contents found

# Solution: Investigate conflicts
sbt "show assembly/assemblyMergeStrategy"
sbt assembly --debug  # Verbose output for conflict analysis
```

#### Performance Monitoring

**Build Metrics Tracking:**
```scala
// Custom task to track build performance
lazy val buildMetrics = taskKey[Unit]("Track build performance metrics")

buildMetrics := {
  val startTime = System.currentTimeMillis()
  
  // Run build tasks
  clean.value
  (Compile / compile).value
  test.value
  assembly.value
  
  val totalTime = System.currentTimeMillis() - startTime
  val log = streams.value.log
  
  log.info(s"Total build time: ${totalTime/1000}s")
  
  // Log to metrics system (e.g., InfluxDB, CloudWatch)
  logBuildMetrics("etl-oal-ifrs9", totalTime, "success")
}
```

**Build Performance Baselines:**
- **Full clean build**: 4-5 minutes (target)
- **Incremental build**: 30-60 seconds (target)
- **Test execution**: 2-3 minutes (45 tests)
- **Assembly creation**: 60-90 seconds

## 10. Stream Key

### Understanding Stream Key Architecture

#### What is Stream Key?

**Stream Key** is CBA's internal data lineage and stream management system that provides:

- **Data Lineage Tracking**: Complete visibility of data flow from source to destination
- **Stream Registration**: Centralized catalog of all data streams in the organization
- **Dependency Management**: Understanding and managing data dependencies
- **Quality Monitoring**: Data quality metrics and alerting
- **Impact Analysis**: Understanding downstream impact of data changes
- **Compliance**: Regulatory compliance and audit trail capabilities

#### Stream Key in Data Ecosystem

```
CBA Data Ecosystem with Stream Key:

Source Systems          Stream Key Registry          Target Systems
├── SAP (Accounting)    ├── Stream Definitions      ├── IFRS9 Models
├── Siebel (CRM)       ├── Lineage Mapping         ├── Regulatory Reports  
├── Mainframe (Core)   ├── Quality Rules           ├── Risk Analytics
├── External Feeds     ├── SLA Monitoring          ├── Business Intelligence
└── Real-time APIs     └── Impact Analysis         └── Data Science Platform
```

#### Stream Key Components

**1. Stream Registry:**
- **Stream Definition**: Metadata about each data stream
- **Schema Management**: Data structure and evolution tracking  
- **Ownership**: Data stewardship and contact information
- **Classification**: Data sensitivity and security levels

**2. Lineage Engine:**
- **Source-to-Target Mapping**: Complete data flow visualization
- **Transformation Tracking**: ETL logic and business rules
- **Dependency Chain**: Upstream and downstream relationships
- **Change Impact**: Analysis of modification effects

**3. Quality Framework:**
- **Data Quality Rules**: Business and technical validations
- **Monitoring Dashboard**: Real-time quality metrics
- **Alerting System**: Automated notifications for quality issues
- **Remediation Workflow**: Process for addressing quality problems

### Stream Key Implementation for VLOC Pipeline

#### Stream Registration Process

**Step 1: Stream Definition**
```json
{
  "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
  "stream_name": "VLOC Model Input Transformation",
  "description": "Variable Line of Credit accounts prepared for IFRS9 modeling",
  "domain": "Risk Analytics",
  "sub_domain": "Credit Risk",
  "data_classification": "Internal",
  "business_owner": "Risk Analytics Team",
  "technical_owner": "Data Engineering Team",
  "created_date": "2023-01-15",
  "last_updated": "2024-01-15",
  "status": "Active",
  "schedule": {
    "frequency": "Monthly",
    "trigger": "1st of month, 20:30",
    "calendar": "hado_omnia_mth_1_cal"
  }
}
```

**Step 2: Source Registration**
```json
{
  "sources": [
    {
      "source_id": "ENR_ACCT_DETL",
      "source_name": "Account Details",
      "system": "SAP",
      "database": "prod_enr",
      "table": "acct_detl",
      "partitions": ["year", "month"],
      "refresh_pattern": "Daily",
      "retention_period": "7 years",
      "record_count_range": {
        "min": 1800000,
        "max": 2200000,
        "typical": 2000000
      }
    },
    {
      "source_id": "ENR_ACCT_BALN", 
      "source_name": "Account Balance",
      "system": "SAP",
      "database": "prod_enr",
      "table": "acct_baln",
      "partitions": ["year", "month"],
      "refresh_pattern": "Daily",
      "retention_period": "7 years",
      "record_count_range": {
        "min": 1800000,
        "max": 2200000,
        "typical": 2000000
      }
    }
    // ... (15 more source definitions)
  ]
}
```

**Step 3: Transformation Registration**
```json
{
  "transformations": [
    {
      "transformation_id": "VLOC_INITLOAD",
      "name": "VLOC Initial Load",
      "type": "SQL",
      "source_file": "/sql/vloc_modl_input/vloc_initload.sql",
      "description": "Initial VLOC account selection and base data preparation",
      "business_rules": [
        "Select only VLOC and PLOC products",
        "Filter for active accounts only",
        "Include accounts from SAP system only"
      ],
      "data_quality_checks": [
        "Account ID not null",
        "Product code in approved list", 
        "Balance amount is numeric"
      ]
    },
    {
      "transformation_id": "VLOC_BANKRUPTCY_FLAG",
      "name": "Bankruptcy Flag Calculation",
      "type": "SQL", 
      "source_file": "/sql/vloc_modl_input/vloc_bankruptcy_flag.sql",
      "description": "Determines bankruptcy status for risk assessment",
      "business_rules": [
        "Check customer bankruptcy status on reporting date",
        "Consider only active bankruptcy records",
        "Default to 'N' if no bankruptcy record found"
      ],
      "dependencies": ["VLOC_INITLOAD"]
    }
    // ... (9 more transformation definitions)
  ]
}
```

**Step 4: Target Registration**
```json
{
  "targets": [
    {
      "target_id": "OAL_VLOC_MODEL_INPUT",
      "target_name": "VLOC Model Input Table", 
      "database": "prod_oal",
      "table": "bv_rbs_vloc_acct_modl_inpt_mnly",
      "partitions": ["year", "month"],
      "storage_format": "Parquet",
      "compression": "Snappy",
      "expected_record_count": {
        "min": 1200000,
        "max": 1600000,
        "typical": 1400000
      },
      "consumers": [
        "IFRS9 Model Development",
        "Risk Analytics Dashboard",
        "Regulatory Reporting",
        "SAS Model Training"
      ]
    }
  ]
}
```

#### Lineage Mapping

**Complete Data Lineage Visualization:**
```
Source Tables (ENR Layer)                    Transformations                      Target Table (OAL Layer)
├── prod_enr.acct_detl          ────────────┐                                    
├── prod_enr.acct_baln          ────────────┤                                    
├── prod_enr.frl_acct_fcly      ────────────┤                                    
├── prod_enr.osc_coll_mttr      ────────────┤ → vloc_initload.sql               
├── prod_enr.acct_hrds          ────────────┤ → vloc_bankruptcy_flag.sql         
├── prod_enr.cstmr_bnkrptcy     ────────────┤ → vloc_arrears_flag.sql           ──→ prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly
├── prod_enr.acct_dlnqncy_hist  ────────────┤ → vloc_collections_flag.sql       
├── prod_enr.prty_rltnshp       ────────────┤ → vloc_realisation_flag.sql       
├── ... (9 more source tables) ────────────┘ → vloc_ongoing.sql                 
```

**Lineage Tracking Benefits:**
- **Impact Analysis**: Understand effect of source changes on VLOC model
- **Root Cause Analysis**: Trace data quality issues back to source
- **Change Management**: Plan and coordinate changes across systems
- **Compliance**: Demonstrate data governance for regulatory requirements

### Stream Key Quality Framework

#### Data Quality Rules Definition

**Business Quality Rules:**
```json
{
  "quality_rules": [
    {
      "rule_id": "VLOC_001",
      "rule_name": "Account ID Completeness",
      "rule_type": "Completeness",
      "description": "All records must have valid account identifier",
      "sql_check": "SELECT COUNT(*) FROM target_table WHERE acct_idnn_bk IS NULL OR acct_idnn_bk = ''",
      "expected_result": 0,
      "severity": "Critical",
      "action": "Fail pipeline if any nulls found"
    },
    {
      "rule_id": "VLOC_002", 
      "rule_name": "Balance Amount Validity",
      "rule_type": "Validity",
      "description": "Outstanding balance must be non-negative",
      "sql_check": "SELECT COUNT(*) FROM target_table WHERE ostd_baln_a < 0",
      "expected_result": 0,
      "severity": "High",
      "action": "Alert if negative balances found"
    },
    {
      "rule_id": "VLOC_003",
      "rule_name": "Record Count Range Check", 
      "rule_type": "Volume",
      "description": "Monthly record count within expected range",
      "sql_check": "SELECT COUNT(*) FROM target_table WHERE year = ${year} AND month = ${month}",
      "expected_range": {
        "min": 1200000

,
        "max": 1600000
      },
      "severity": "Medium",
      "action": "Alert if outside range"
    },
    {
      "rule_id": "VLOC_004",
      "rule_name": "Default Flag Distribution",
      "rule_type": "Distribution",
      "description": "in_spot_dflt_f should be 60-80% Y values",
      "sql_check": "SELECT (COUNT(CASE WHEN in_spot_dflt_f = 'Y' THEN 1 END) * 100.0 / COUNT(*)) as pct_default FROM target_table",
      "expected_range": {
        "min": 60.0,
        "max": 80.0
      },
      "severity": "Medium",
      "action": "Alert if distribution anomaly"
    }
  ]
}
```

**Technical Quality Rules:**
```json
{
  "technical_quality_rules": [
    {
      "rule_id": "TECH_001",
      "rule_name": "Schema Compliance",
      "rule_type": "Schema",
      "description": "All columns present with correct data types",
      "check_type": "Metadata",
      "expected_columns": [
        {"name": "acct_idnn_bk", "type": "string", "nullable": false},
        {"name": "perd_d", "type": "date", "nullable": false},
        {"name": "ostd_baln_a", "type": "decimal(15,2)", "nullable": true},
        {"name": "in_spot_dflt_f", "type": "string", "nullable": false}
      ]
    },
    {
      "rule_id": "TECH_002", 
      "rule_name": "File Format Validation",
      "rule_type": "Format",
      "description": "Files must be valid Parquet format with Snappy compression",
      "check_type": "File System",
      "expected_format": "Parquet",
      "expected_compression": "Snappy"
    }
  ]
}
```

#### Quality Monitoring Dashboard

**Real-Time Quality Metrics:**
```
VLOC Pipeline Quality Dashboard
├── Data Completeness: 99.98% ✓
├── Record Count: 1,423,567 (within range) ✓  
├── Balance Validation: 2 negative values ⚠️
├── Default Flag Distribution: 67.3% Y values ✓
├── Schema Compliance: All columns present ✓
├── File Format: Valid Parquet/Snappy ✓
├── Processing Time: 52 minutes ✓
└── SLA Compliance: Met (target: <90 min) ✓
```

**Historical Trends:**
```
Quality Trend Analysis (Last 12 Months):
├── Completeness: 99.95% - 99.99% (stable)
├── Record Count: 1.2M - 1.6M (seasonal variation)
├── Processing Time: 45-65 minutes (stable)
├── Error Rate: <0.01% (excellent)
└── SLA Breaches: 0 incidents
```

### Stream Key API Integration

#### Stream Registration API

**Register New Stream:**
```bash
curl -X POST "${STREAM_KEY_API}/api/v1/streams" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -d '{
    "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
    "stream_name": "VLOC Model Input Transformation",
    "domain": "Risk Analytics",
    "owner": "data-engineering-team@cba.com.au",
    "schedule": {
      "frequency": "Monthly",
      "cron": "30 20 1 * *"
    }
  }'
```

**Response:**
```json
{
  "status": "success",
  "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
  "registration_id": "12345",
  "created_timestamp": "2024-01-15T10:30:00Z",
  "approval_required": true,
  "approval_workflow_id": "APPROVAL-67890"
}
```

#### Lineage Registration API

**Register Data Lineage:**
```bash
curl -X POST "${STREAM_KEY_API}/api/v1/lineage" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -d '{
    "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
    "lineage": {
      "sources": [
        {
          "database": "prod_enr",
          "table": "acct_detl",
          "columns": ["acct_idnn_bk", "prd_c", "open_d", "acct_stat_c"]
        },
        {
          "database": "prod_enr", 
          "table": "acct_baln",
          "columns": ["acct_idnn_bk", "ostd_baln_a", "perd_d"]
        }
      ],
      "transformations": [
        {
          "step": 1,
          "name": "vloc_initload",
          "type": "SQL",
          "description": "Initial account selection and filtering"
        }
      ],
      "targets": [
        {
          "database": "prod_oal",
          "table": "bv_rbs_vloc_acct_modl_inpt_mnly",
          "columns": ["acct_idnn_bk", "perd_d", "ostd_baln_a", "in_spot_dflt_f"]
        }
      ]
    }
  }'
```

#### Quality Rule API

**Register Quality Rules:**
```bash
curl -X POST "${STREAM_KEY_API}/api/v1/quality-rules" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -d '{
    "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
    "rules": [
      {
        "rule_id": "VLOC_001",
        "rule_name": "Account ID Completeness",
        "rule_type": "Completeness",
        "sql_check": "SELECT COUNT(*) FROM target_table WHERE acct_idnn_bk IS NULL",
        "expected_result": 0,
        "severity": "Critical"
      }
    ]
  }'
```

### Stream Key Integration in VLOC Pipeline

#### Pipeline Integration Points

**1. Pre-Processing Stream Key Checks:**
```scala
// StreamKeyValidator.scala
object StreamKeyValidator {
  
  def validateSourceAvailability(streamId: String, runDate: String): Boolean = {
    val client = StreamKeyClient.create()
    
    val sources = client.getStreamSources(streamId)
    
    sources.forall { source =>
      val available = checkSourceDataAvailability(source, runDate)
      if (!available) {
        logger.error(s"Source ${source.database}.${source.table} not available for $runDate")
      }
      available
    }
  }
  
  def checkSourceDataAvailability(source: SourceDefinition, runDate: String): Boolean = {
    val spark = SparkSession.active
    
    try {
      val partitionCheck = spark.sql(s"""
        SHOW PARTITIONS ${source.database}.${source.table}
      """).collect().exists(_.getString(0).contains(runDate))
      
      if (partitionCheck) {
        val recordCount = spark.sql(s"""
          SELECT COUNT(*) FROM ${source.database}.${source.table}
          WHERE partition_date = '$runDate'
        """).collect()(0).getLong(0)
        
        recordCount >= source.minExpectedRecords
      } else {
        false
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error checking source availability: ${e.getMessage}")
        false
    }
  }
}
```

**2. Quality Rule Execution:**
```scala
// QualityChecker.scala
object QualityChecker {
  
  def executeQualityRules(streamId: String, targetTable: String, runDate: String): QualityResult = {
    val client = StreamKeyClient.create()
    val rules = client.getQualityRules(streamId)
    
    val results = rules.map { rule =>
      executeRule(rule, targetTable, runDate)
    }
    
    QualityResult(
      streamId = streamId,
      runDate = runDate,
      totalRules = rules.length,
      passedRules = results.count(_.passed),
      failedRules = results.count(!_.passed),
      criticalFailures = results.filter(r => !r.passed && r.severity == "Critical").length,
      results = results
    )
  }
  
  def executeRule(rule: QualityRule, targetTable: String, runDate: String): RuleResult = {
    val spark = SparkSession.active
    
    try {
      val sqlWithParameters = rule.sqlCheck
        .replace("target_table", targetTable)
        .replace("${year}", runDate.substring(0, 4))
        .replace("${month}", runDate.substring(5, 7))
      
      val actualResult = spark.sql(sqlWithParameters).collect()(0).getLong(0)
      
      val passed = rule.ruleType match {
        case "Completeness" | "Validity" => actualResult == rule.expectedResult
        case "Volume" => actualResult >= rule.expectedRange.min && actualResult <= rule.expectedRange.max
        case "Distribution" => {
          val percentage = actualResult.toDouble
          percentage >= rule.expectedRange.min && percentage <= rule.expectedRange.max
        }
      }
      
      RuleResult(
        ruleId = rule.ruleId,
        ruleName = rule.ruleName,
        passed = passed,
        actualResult = actualResult,
        expectedResult = rule.expectedResult,
        severity = rule.severity,
        executionTime = System.currentTimeMillis()
      )
      
    } catch {
      case e: Exception =>
        logger.error(s"Error executing rule ${rule.ruleId}: ${e.getMessage}")
        RuleResult(
          ruleId = rule.ruleId,
          ruleName = rule.ruleName,
          passed = false,
          actualResult = -1,
          expectedResult = rule.expectedResult,
          severity = "Critical",
          executionTime = System.currentTimeMillis(),
          error = Some(e.getMessage)
        )
    }
  }
}
```

**3. Post-Processing Stream Key Updates:**
```scala
// StreamKeyReporter.scala  
object StreamKeyReporter {
  
  def reportPipelineCompletion(streamId: String, runDate: String, qualityResult: QualityResult): Unit = {
    val client = StreamKeyClient.create()
    
    val executionReport = ExecutionReport(
      streamId = streamId,
      runDate = runDate,
      status = if (qualityResult.criticalFailures == 0) "SUCCESS" else "FAILED",
      startTime = pipelineStartTime,
      endTime = System.currentTimeMillis(),
      recordCount = getTargetRecordCount(streamId, runDate),
      qualityScore = (qualityResult.passedRules.toDouble / qualityResult.totalRules * 100).round,
      qualityResults = qualityResult
    )
    
    client.reportExecution(executionReport)
    
    // Update lineage with actual execution details
    updateLineageExecution(streamId, runDate, executionReport)
    
    // Trigger downstream notifications if successful
    if (executionReport.status == "SUCCESS") {
      notifyDownstreamConsumers(streamId, runDate)
    }
  }
  
  def updateLineageExecution(streamId: String, runDate: String, report: ExecutionReport): Unit = {
    val lineageUpdate = LineageExecution(
      streamId = streamId,
      runDate = runDate,
      executionId = UUID.randomUUID().toString,
      dataVolume = report.recordCount,
      processingTime = report.endTime - report.startTime,
      resourceUtilization = getResourceMetrics(),
      dataMovement = calculateDataMovement(streamId, runDate)
    )
    
    StreamKeyClient.create().updateLineageExecution(lineageUpdate)
  }
}
```

#### Integration with VLOC Pipeline

**Modified Pipeline Main Class:**
```scala
// Enhanced VlocPipelineMain.scala with Stream Key integration
object VlocPipelineMain extends App {
  
  val streamId = "OMN_VLOC_MODEL_INPUT_TRANSFORM"
  val runDate = args(0) // Format: YYYY-MM-DD
  
  // Step 1: Pre-processing validation
  logger.info("Starting Stream Key pre-processing validation...")
  
  if (!StreamKeyValidator.validateSourceAvailability(streamId, runDate)) {
    logger.error("Source data validation failed")
    System.exit(1)
  }
  
  // Step 2: Execute pipeline
  logger.info("Starting VLOC transformation pipeline...")
  val pipelineStartTime = System.currentTimeMillis()
  
  try {
    // Existing pipeline logic
    val result = VlocTransformation.execute(runDate)
    
    // Step 3: Quality validation
    logger.info("Executing quality rules...")
    val qualityResult = QualityChecker.executeQualityRules(
      streamId, 
      "prod_oal.bv_rbs_vloc_acct_modl_inpt_mnly", 
      runDate
    )
    
    // Step 4: Check for critical failures
    if (qualityResult.criticalFailures > 0) {
      logger.error(s"${qualityResult.criticalFailures} critical quality failures detected")
      
      // Rollback or quarantine data
      handleCriticalQualityFailures(qualityResult)
      System.exit(1)
    }
    
    // Step 5: Report successful completion
    StreamKeyReporter.reportPipelineCompletion(streamId, runDate, qualityResult)
    
    logger.info("VLOC pipeline completed successfully")
    
  } catch {
    case e: Exception =>
      logger.error(s"Pipeline execution failed: ${e.getMessage}")
      
      // Report failure to Stream Key
      StreamKeyReporter.reportPipelineFailure(streamId, runDate, e)
      System.exit(1)
  }
}
```

### Stream Key Governance and Compliance

#### Data Governance Framework

**1. Stream Approval Workflow:**
```
Stream Registration Process:
├── Developer submits stream definition
├── Data Steward reviews business requirements
├── Data Architect validates technical design
├── Security team reviews data classification
├── Compliance team approves regulatory impact
└── Stream Key automatically provisions monitoring
```

**2. Change Management:**
```json
{
  "change_request": {
    "stream_id": "OMN_VLOC_MODEL_INPUT_TRANSFORM",
    "change_type": "Schema Evolution",
    "description": "Add new column: credit_score_c",
    "impact_analysis": {
      "affected_consumers": [
        "IFRS9 Model Development",
        "Risk Analytics Dashboard"
      ],
      "breaking_change": false,
      "migration_required": false
    },
    "approval_workflow": [
      {
        "approver": "Data Steward",
        "status": "Approved",
        "comments": "Business requirement validated"
      },
      {
        "approver": "Data Architect", 
        "status": "Pending",
        "comments": "Under technical review"
      }
    ]
  }
}
```

**3. Compliance Reporting:**
```sql
-- Monthly Stream Key compliance report
SELECT 
    s.stream_id,
    s.stream_name,
    s.data_classification,
    COUNT(qr.rule_id) as total_quality_rules,
    AVG(qr.compliance_score) as avg_quality_score,
    COUNT(CASE WHEN er.status = 'SUCCESS' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN er.status = 'FAILED' THEN 1 END) as failed_runs,
    (COUNT(CASE WHEN er.status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(er.execution_id)) as success_rate
FROM stream_registry s
LEFT JOIN quality_rules qr ON s.stream_id = qr.stream_id
LEFT JOIN execution_reports er ON s.stream_id = er.stream_id 
    AND er.run_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
WHERE s.status = 'Active'
GROUP BY s.stream_id, s.stream_name, s.data_classification
ORDER BY success_rate DESC;
```

### Stream Key Benefits and ROI

#### Operational Benefits

**1. Reduced Incident Response Time:**
- **Before Stream Key**: 4-6 hours to identify root cause of data issues
- **After Stream Key**: 15-30 minutes with automated lineage tracing
- **Improvement**: 85% reduction in mean time to resolution (MTTR)

**2. Proactive Quality Management:**
- **Before**: Reactive discovery of data quality issues by consumers
- **After**: Real-time quality monitoring with automated alerting
- **Impact**: 90% reduction in downstream data quality incidents

**3. Compliance Automation:**
- **Manual Effort**: 40 hours/month for regulatory data lineage documentation
- **Automated**: Real-time compliance reporting and audit trails
- **Savings**: $50,000/year in manual effort reduction

#### Business Value

**1. Risk Mitigation:**
```
Risk Reduction Analysis:
├── Data Quality Incidents: 75% reduction
├── Regulatory Compliance Issues: 90% reduction  
├── Downstream System Failures: 60% reduction
├── Manual Investigation Effort: 80% reduction
└── Customer Impact Events: 85% reduction
```

**2. Innovation Enablement:**
```
Innovation Benefits:
├── Self-Service Data Discovery: 200+ data streams cataloged
├── Faster Development Cycles: 40% reduction in development time
├── Reusability: 60% of transformations reused across projects
├── Data Democratization: 500+ users accessing Stream Key portal
└── AI/ML Readiness: Automated feature engineering lineage
```

**3. Cost Savings:**
```
Annual Cost Savings:
├── Reduced Incident Response: $200,000
├── Automated Compliance: $150,000  
├── Faster Development: $300,000
├── Improved Data Quality: $500,000
├── Infrastructure Optimization: $100,000
└── Total Annual Savings: $1,250,000
```

**Total Cost of Ownership:**
- **Implementation**: $500,000 (one-time)
- **Annual Operating**: $200,000
- **3-Year TCO**: $1,100,000
- **3-Year Benefits**: $3,750,000
- **Net ROI**: 241% over 3 years

## 11. CI/CD and Automation

### Understanding CI/CD in Data Engineering Context

#### What is CI/CD for Data Pipelines?

**Continuous Integration (CI)** for data engineering involves:
- **Code Integration**: Merging data pipeline code changes frequently
- **Automated Testing**: Unit tests, integration tests, and data quality tests
- **Build Automation**: Compiling Scala/Java code, packaging JARs
- **Static Analysis**: Code quality checks, security scanning
- **Artifact Creation**: Creating deployable packages

**Continuous Deployment (CD)** includes:
- **Environment Promotion**: Moving code through dev → test → prod environments
- **Infrastructure as Code**: Automated provisioning of data infrastructure
- **Data Pipeline Deployment**: Deploying ETL jobs, updating configurations
- **Rollback Capabilities**: Quick recovery from deployment issues
- **Monitoring Integration**: Automated alerting and monitoring setup

#### CI/CD Architecture for Omnia Platform

```
CI/CD Pipeline Architecture:

Source Control           Build System              Deployment Pipeline
├── Git Repository      ├── TeamCity Build        ├── Dev Environment
│   ├── Scala Code     │   ├── SBT Compilation   │   ├── Feature Testing
│   ├── SQL Scripts    │   ├── Unit Tests        │   ├── Integration Tests
│   ├── YAML Configs   │   ├── Quality Gates     │   └── Developer Validation
│   └── Documentation │   └── Artifact Creation  ├── Test Environment
├── Feature Branches   ├── Quality Checks        │   ├── End-to-End Testing
├── Pull Requests      │   ├── SonarQube Scan    │   ├── Performance Testing
└── Release Branches   │   ├── Security Scan     │   └── User Acceptance
                       │   └── Dependency Check  ├── Pre-Production
                       └── Artifact Repository    │   ├── Production-like Data
                           ├── Nexus Repository   │   ├── Load Testing
                           └── Docker Registry     │   └── Stress Testing
                                                  └── Production Environment
                                                      ├── Blue-Green Deployment
                                                      ├── Canary Releases
                                                      └── Automated Rollback
```

### TeamCity Configuration for VLOC Pipeline

#### Build Configuration Setup

**Build Configuration XML:**
```xml
<build-type name="VLOC ETL Pipeline" id="VlocEtlPipeline">
  <description>CI/CD pipeline for VLOC Model Input transformation</description>
  
  <settings>
    <parameters>
      <param name="scala.version" value="2.12.15"/>
      <param name="spark.version" value="3.2.0"/>
      <param name="sbt.version" value="1.7.1"/>
      <param name="target.environment" value="%env.TARGET_ENV%"/>
    </parameters>
    
    <build-runners>
      <!-- Step 1: Environment Setup -->
      <runner name="Environment Setup" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Setting up build environment..."
            export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
            export SBT_HOME=/opt/sbt
            export PATH=$JAVA_HOME/bin:$SBT_HOME/bin:$PATH
            
            echo "Java Version: $(java -version)"
            echo "SBT Version: $(sbt sbtVersion)"
            echo "Scala Version: %scala.version%"
          </param>
        </parameters>
      </runner>
      
      <!-- Step 2: Code Quality Checks -->
      <runner name="Code Quality" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Running code quality checks..."
            
            # Scala format check
            sbt scalafmtCheck
            
            # Compile warnings as errors
            sbt "set scalacOptions += \"-Xfatal-warnings\"" compile
            
            # Dependency vulnerability check
            sbt dependencyCheck
          </param>
        </parameters>
      </runner>
      
      <!-- Step 3: Unit Tests -->
      <runner name="Unit Tests" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Running unit tests..."
            sbt clean test
            
            # Generate test coverage report
            sbt coverage test coverageReport
            
            # Publish test results
            find . -name "*.xml" -path "*/test-reports/*" -exec cp {} %teamcity.build.tempDir%/test-results/ \;
          </param>
        </parameters>
      </runner>
      
      <!-- Step 4: Integration Tests -->
      <runner name="Integration Tests" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Running integration tests..."
            
            # Start test containers (Hive Metastore, HDFS)
            docker-compose -f docker-compose.test.yml up -d
            
            # Wait for services to be ready
            sleep 30
            
            # Run integration tests
            sbt "testOnly *IntegrationTest"
            
            # Cleanup test containers
            docker-compose -f docker-compose.test.yml down
          </param>
        </parameters>
      </runner>
      
      <!-- Step 5: Build JAR -->
      <runner name="Build JAR" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Building application JAR..."
            sbt assembly
            
            # Copy JAR to artifacts directory
            cp target/scala-2.12/etl-oal-ifrs9*-thin.jar %teamcity.build.tempDir%/artifacts/
            
            # Generate build metadata
            echo "build.number=%build.number%" > %teamcity.build.tempDir%/artifacts/build.properties
            echo "build.timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> %teamcity.build.tempDir%/artifacts/build.properties
            echo "git.commit=%build.vcs.number%" >> %teamcity.build.tempDir%/artifacts/build.properties
          </param>
        </parameters>
      </runner>
      
      <!-- Step 6: Security Scan -->
      <runner name="Security Scan" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Running security scans..."
            
            # OWASP dependency check
            sbt dependencyCheck
            
            # Snyk vulnerability scan
            snyk test --severity-threshold=high
            
            # SonarQube static analysis
            sonar-scanner \
              -Dsonar.projectKey=vloc-etl-pipeline \
              -Dsonar.sources=src/main/scala \
              -Dsonar.tests=src/test/scala \
              -Dsonar.scala.coverage.reportPaths=target/scala-2.12/scoverage-report/scoverage.xml
          </param>
        </parameters>
      </runner>
      
      <!-- Step 7: Publish Artifacts -->
      <runner name="Publish Artifacts" type="simpleRunner">
        <parameters>
          <param name="script.content">
            echo "Publishing artifacts..."
            
            # Publish to Nexus repository
            sbt publish
            
            # Tag Docker image (if using containerization)
            if [ -f Dockerfile ]; then
              docker build -t vloc-etl-pipeline:%build.number% .
              docker tag vloc-etl-pipeline:%build.number% nexus.cba.com.au/vloc-etl-pipeline:latest
              docker push nexus.cba.com.au/vloc-etl-pipeline:%build.number%
              docker push nexus.cba.com.au/vloc-etl-pipeline:latest
            fi
          </param>
        </parameters>
      </runner>
    </build-runners>
    
    <build-triggers>
      <!-- Trigger on SCM changes -->
      <build-trigger type="vcsTrigger">
        <parameters>
          <param name="branchFilter">+:refs/heads/master +:refs/heads/develop +:refs/heads/feature/*</param>
          <param name="quietPeriod">60</param>
        </parameters>
      </build-trigger>
      
      <!-- Nightly build trigger -->
      <build-trigger type="schedulingTrigger">
        <parameters>
          <param name="cronExpression">0 2 * * * ?</param>
          <param name="timezone">Australia/Sydney</param>
        </parameters>
      </build-trigger>
    </build-triggers>
    
    <build-features>
      <!-- Artifact dependency -->
      <build-feature type="artifact-dependency">
        <parameters>
          <param name="source_buildTypeId">OmniaCore_Build</param>
          <param name="revisionName">lastSuccessful</param>
          <param name="pathRules">omnia-core*.jar => lib/</param>
        </parameters>
      </build-feature>
      
      <!-- VCS labeling -->
      <build-feature type="vcs-labeling">
        <parameters>
          <param name="vcs-labeling-pattern">build-%build.number%</param>
        </parameters>
      </build-feature>
      
      <!-- Build notifications -->
      <build-feature type="email-notifier">
        <parameters>
          <param name="notifier.email.recipient">vloc-team@cba.com.au</param>
          <param name="notifier.email.notifyOnBuildFailure">true</param>
          <param name="notifier.email.notifyOnFirstBuildSuccess">true</param>
        </parameters>
      </build-feature>
    </build-features>
  </settings>
</build-type>
```

#### Build Pipeline Stages

**Stage 1: Pre-Build Validation**
```bash
#!/bin/bash
# pre-build-validation.sh

echo "=== Pre-Build Validation ==="

# Check prerequisites
check_prerequisites() {
    echo "Checking prerequisites..."
    
    # Java version check
    java_version=$(java -version 2>&1 | grep "version" | awk '{print $3}' | sed 's/"//g')
    if [[ ! "$java_version" =~ ^1\.8\. ]]; then
        echo "ERROR: Java 8 required, found: $java_version"
        exit 1
    fi
    
    # SBT availability
    if ! command -v sbt &> /dev/null; then
        echo "ERROR: SBT not found in PATH"
        exit 1
    fi
    
    # Git repository status
    if [[ -n $(git status --porcelain) ]]; then
        echo "WARNING: Working directory has uncommitted changes"
    fi
    
    echo "Prerequisites check passed ✓"
}

# Validate configuration files
validate_configs() {
    echo "Validating configuration files..."
    
    # Check YAML syntax
    for yaml_file in $(find src/main/resources -name "*.yaml" -o -name "*.yml"); do
        echo "Validating $yaml_file"
        python -c "import yaml; yaml.safe_load(open('$yaml_file'))" || {
            echo "ERROR: Invalid YAML syntax in $yaml_file"
            exit 1
        }
    done
    
    # Check SQL syntax (basic)
    for sql_file in $(find src/main/resources -name "*.sql"); do
        echo "Validating $sql_file"
        # Basic SQL syntax check - could be enhanced with actual SQL parser
        if grep -q "SELECT\|INSERT\|UPDATE\|DELETE" "$sql_file"; then
            echo "SQL file $sql_file appears valid"
        else
            echo "WARNING: $sql_file may not contain valid SQL"
        fi
    done
    
    echo "Configuration validation passed ✓"
}

# Check for common issues
check_common_issues() {
    echo "Checking for common code issues..."
    
    # Check for TODO/FIXME comments
    todo_count=$(grep -r "TODO\|FIXME" src/ --include="*.scala" | wc -l)
    if [ $todo_count -gt 0 ]; then
        echo "WARNING: Found $todo_count TODO/FIXME comments"
        grep -r "TODO\|FIXME" src/ --include="*.scala"
    fi
    
    # Check for hardcoded credentials
    if grep -r "password\|secret\|key" src/ --include="*.scala" | grep -v "// Safe comment"; then
        echo "ERROR: Potential hardcoded credentials found"
        exit 1
    fi
    
    echo "Common issues check passed ✓"
}

# Execute all checks
check_prerequisites
validate_configs
check_common_issues

echo "=== Pre-Build Validation Complete ==="
```

**Stage 2: Automated Testing**
```scala
// Enhanced test configuration
// src/test/scala/integration/

VlocPipelineIntegrationTest.scala
class VlocPipelineIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  var testContainer: DockerContainer = _
  
  override def beforeAll(): Unit = {
    // Start test containers
    testContainer = DockerContainer("confluentinc/cp-schema-registry:latest")
      .withNetwork("test-network")
      .withExposedPorts(8081)
    
    testContainer.start()
    
    // Initialize Spark with test configuration
    spark = SparkSession.builder()
      .appName("VLOC Integration Test")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
      .config("hive.metastore.uris", s"thrift://localhost:${testContainer.mappedPort(9083)}")
      .enableHiveSupport()
      .getOrCreate()
  }
  
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (testContainer != null) testContainer.stop()
  }
  
  "VLOC Pipeline Integration" should "successfully process end-to-end workflow" in {
    // Setup test data
    createTestTables()
    loadTestData()
    
    // Execute pipeline
    val pipeline = new VlocPipeline(spark)
    val result = pipeline.execute("2024-01-31")
    
    // Validate results
    result.success should be(true)
    
    val outputData = spark.sql("""
      SELECT COUNT(*) as record_count,
             COUNT(CASE WHEN in_spot_dflt_f = 'Y' THEN 1 END) as default_count
      FROM test_oal.vloc_model_input
    """).collect()(0)
    
    outputData.getLong(0) should be > 0L
    outputData.getLong(1) should be > 0L
  }
  
  "Pipeline" should "handle missing source data gracefully" in {
    // Test error handling
    dropTestTables()
    
    val pipeline = new VlocPipeline(spark)
    val result = pipeline.execute("2024-01-31")
    
    result.success should be(false)
    result.errorMessage should contain("Table not found")
  }
  
  def createTestTables(): Unit = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS test_enr.acct_detl (
        acct_idnn_bk STRING,
        prd_c STRING,
        open_d DATE,
        acct_stat_c STRING,
        srce_syst_c STRING
      ) PARTITIONED BY (year STRING, month STRING)
      STORED AS PARQUET
    """)
    
    // Create other test tables...
  }
  
  def loadTestData(): Unit = {
    val testAccounts = Seq(
      ("VLOC123456", "VLOC", "2020-01-15", "ACTIVE", "SAP", "2024", "01"),
      ("VLOC789012", "PLOC", "2021-03-20", "ACTIVE", "SAP", "2024", "01"),
      ("VLOC345678", "VLOC", "2019-08-10", "CLOSED", "SAP", "2024", "01")
    ).toDF("acct_idnn_bk", "prd_c", "open_d", "acct_stat_c", "srce_syst_c", "year", "month")
    
    testAccounts.write
      .mode("overwrite")
      .insertInto("test_enr.acct_detl")
  }
}
```

**Stage 3: Performance Testing**
```scala
// src/test/scala/performance/VlocPipelinePerformanceTest.scala
class VlocPipelinePerformanceTest extends FlatSpec with Matchers {
  
  "VLOC Pipeline" should "complete within SLA timeframes" in {
    val spark = SparkSession.builder()
      .appName("Performance Test")
      .master("local[8]")  // Use more cores for performance testing
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()
    
    // Create large test dataset (100K records)
    val largeTestData = generateLargeTestDataset(100000)
    
    val startTime = System.currentTimeMillis()
    
    // Execute pipeline
    val pipeline = new VlocPipeline(spark)
    val result = pipeline.execute("2024-01-31")
    
    val executionTime = System.currentTimeMillis() - startTime
    val executionMinutes = executionTime / 60000.0
    
    // Performance assertions
    result.success should be(true)
    executionMinutes should be < 5.0  // Should complete in less than 5 minutes for 100K records
    
    // Memory usage check
    val runtime = Runtime.getRuntime
    val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
    
    usedMemory should be < 8192  // Should use less than 8GB memory
    
    spark.stop()
  }
  
  def generateLargeTestDataset(recordCount: Int): DataFrame = {
    import spark.implicits._
    
    val random = new scala.util.Random(42)  // Seed for reproducible tests
    
    (1 to recordCount).map { i =>
      val accountId = f"VLOC$i%010d"
      val productCode = if (random.nextBoolean()) "VLOC" else "PLOC"
      val balance = random.nextDouble() * 100000
      val status = if (random.nextFloat() < 0.9) "ACTIVE" else "CLOSED"
      
      (accountId, productCode, balance, status, "SAP")
    }.toDF("acct_idnn_bk", "prd_c", "ostd_baln_a", "acct_stat_c", "srce_syst_c")
  }
}
```

### Deployment Automation

#### Environment-Specific Deployment

**Deployment Configuration (deploy.yml):**
```yaml
# deployment/deploy.yml
environments:
  development:
    cluster: "hado-omnia-d01"
    hdfs_path: "/dev/oal/vloc/"
    database_prefix: "dev_"
    spark_config:
      executor_memory: "8g"
      num_executors: 50
      executor_cores: 2
    
  test:
    cluster: "hado-omnia-t01"
    hdfs_path: "/test/oal/vloc/"
    database_prefix: "test_"
    spark_config:
      executor_memory: "16g"
      num_executors: 100
      executor_cores: 2
    
  production:
    cluster: "hado-omnia-p01"
    hdfs_path: "/prod/oal/vloc/"
    database_prefix: "prod_"
    spark_config:
      executor_memory: "24g"
      num_executors: 200
      executor_cores: 2
    approval_required: true
    rollback_enabled: true

deployment_steps:
  - name: "Pre-deployment Validation"
    script: "scripts/pre-deploy-check.sh"
    
  - name: "Backup Current Version"
    script: "scripts/backup-current.sh"
    condition: "production"
    
  - name: "Deploy JAR"
    script: "scripts/deploy-jar.sh"
    
  - name: "Update Configurations"
    script: "scripts/update-configs.sh"
    
  - name: "Update Autosys Jobs"
    script: "scripts/update-autosys.sh"
    
  - name: "Smoke Test"
    script: "scripts/smoke-test.sh"
    
  - name: "Post-deployment Validation"
    script: "scripts/post-deploy-check.sh"

rollback_steps:
  - name: "Stop Current Jobs"
    script: "scripts/stop-jobs.sh"
    
  - name: "Restore Previous Version"
    script: "scripts/restore-backup.sh"
    
  - name: "Restart Jobs"
    script: "scripts/start-jobs.sh"
    
  - name: "Validate Rollback"
    script: "scripts/validate-rollback.sh"
```

**Deployment Script (deploy.sh):**
```bash
#!/bin/bash
# scripts/deploy.sh

set -e  # Exit on any error

ENVIRONMENT=${1:-development}
BUILD_NUMBER=${2:-latest}
DRY_RUN=${3:-false}

# Load configuration
source "deployment/config/${ENVIRONMENT}.conf"

echo "=== VLOC Pipeline Deployment ==="
echo "Environment: $ENVIRONMENT"
echo "Build Number: $BUILD_NUMBER"
echo "Target Cluster: $TARGET_CLUSTER"
echo "Dry Run: $DRY_RUN"
echo "================================"

# Pre-deployment validation
pre_deployment_validation() {
    echo "Running pre-deployment validation..."
    
    # Check cluster accessibility
    if ! ssh -q $TARGET_CLUSTER "echo 'Cluster accessible'"; then
        echo "ERROR: Cannot access target cluster: $TARGET_CLUSTER"
        exit 1
    fi
    
    # Check HDFS availability
    if ! ssh $TARGET_CLUSTER "hdfs dfs -test -d $HDFS_ROOT_PATH"; then
        echo "ERROR: HDFS path not accessible: $HDFS_ROOT_PATH"
        exit 1
    fi
    
    # Check if current jobs are not running
    running_jobs=$(ssh $TARGET_CLUSTER "yarn application -list -appStates RUNNING | grep vloc | wc -l")
    if [ $running_jobs -gt 0 ]; then
        echo "ERROR: VLOC jobs are currently running. Wait for completion or stop them."
        exit 1
    fi
    
    # Validate artifact availability
    if ! curl -s -f "$NEXUS_URL/etl-oal-ifrs9_2.12-$BUILD_NUMBER-thin.jar" > /dev/null; then
        echo "ERROR: Build artifact not found: $BUILD_NUMBER"
        exit 1
    fi
    
    echo "Pre-deployment validation passed ✓"
}

# Backup current version
backup_current_version() {
    if [ "$ENVIRONMENT" = "production" ]; then
        echo "Creating backup of current version..."
        
        BACKUP_DIR="/shared/omnia/backups/vloc/$(date +%Y%m%d_%H%M%S)"
        
        ssh $TARGET_CLUSTER "mkdir -p $BACKUP_DIR"
        ssh $TARGET_CLUSTER "cp $JAR_DEPLOY_PATH $BACKUP_DIR/"
        ssh $TARGET_CLUSTER "cp -r $CONFIG_DEPLOY_PATH $BACKUP_DIR/"
        
        echo "Backup created at: $BACKUP_DIR" | tee backup_location.txt
        echo "Backup completed ✓"
    fi
}

# Deploy new version
deploy_new_version() {
    echo "Deploying new version..."
    
    if [ "$DRY_RUN" = "true" ]; then
        echo "[DRY RUN] Would download and deploy JAR: $BUILD_NUMBER"
        echo "[DRY RUN] Would update configurations"
        echo "[DRY RUN] Would update Autosys job definitions"
        return
    fi
    
    # Download and deploy JAR
    curl -s "$NEXUS_URL/etl-oal-ifrs9_2.12-$BUILD_NUMBER-thin.jar" -o "etl-oal-ifrs9-$BUILD_NUMBER.jar"
    scp "etl-oal-ifrs9-$BUILD_NUMBER.jar" $TARGET_CLUSTER:$JAR_DEPLOY_PATH
    
    # Update current symlink
    ssh $TARGET_CLUSTER "ln -sf $JAR_DEPLOY_PATH /shared/omnia/current/etl-oal-ifrs9-current.jar"
    
    # Update configurations
    scp -r "deployment/configs/$ENVIRONMENT/"* $TARGET_CLUSTER:$CONFIG_DEPLOY_PATH/
    
    # Update Autosys job definitions
    update_autosys_jobs
    
    echo "Deployment completed ✓"
}

# Update Autosys jobs
update_autosys_jobs() {
    echo "Updating Autosys job definitions..."
    
    # Generate environment-specific JIL files
    for jil_template in deployment/autosys/templates/*.jil.template; do
        jil_file=$(basename $jil_template .template)
        
        # Replace environment variables in template
        envsubst < $jil_template > "temp_$jil_file"
        
        # Upload and update job definition
        scp "temp_$jil_file" $TARGET_CLUSTER:/tmp/
        
        ssh $TARGET_CLUSTER "jil < /tmp/$jil_file"
        
        rm "temp_$jil_file"
    done
    
    echo "Autosys jobs updated ✓"
}

# Smoke test
smoke_test() {
    echo "Running smoke tests..."
    
    if [ "$DRY_RUN" = "true" ]; then
        echo "[DRY RUN] Would run smoke tests"
        return
    fi
    
    # Test basic connectivity and configuration
    ssh $TARGET_CLUSTER "java -jar $JAR_DEPLOY_PATH --test-config 2>&1 | head -20"
    
    # Test HDFS access
    ssh $TARGET_CLUSTER "hdfs dfs -test -r $HDFS_ROOT_PATH"
    
    # Test Hive connectivity
    ssh $TARGET_CLUSTER "beeline -u '$HIVE_URL' -e 'SELECT 1' --silent=true"
    
    echo "Smoke tests passed ✓"
}

# Post-deployment validation
post_deployment_validation() {
    echo "Running post-deployment validation..."
    
    # Verify JAR deployment
    if ! ssh $TARGET_CLUSTER "test -f $JAR_DEPLOY_PATH"; then
        echo "ERROR: JAR file not found at deployment location"
        exit 1
    fi
    
    # Verify configuration files
    if ! ssh $TARGET_CLUSTER "test -f $CONFIG_DEPLOY_PATH/application.conf"; then
        echo "ERROR: Configuration files not found"
        exit 1
    fi
    
    # Check Autosys job definitions
    job_count=$(ssh $TARGET_CLUSTER "autorep -J '%vloc%' | wc -l")
    if [ $job_count -lt 5 ]; then
        echo "ERROR: Expected Autosys jobs not found"
        exit 1
    fi
    
    echo "Post-deployment validation passed ✓"
}

# Deployment approval (for production)
deployment_approval() {
    if [ "$ENVIRONMENT" = "production" ] && [ "$DRY_RUN" = "false" ]; then
        echo "=== PRODUCTION DEPLOYMENT APPROVAL REQUIRED ==="
        echo "Environment: $ENVIRONMENT"
        echo "Build: $BUILD_NUMBER"
        echo "Deployer: $(whoami)"
        echo "Time: $(date)"
        echo ""
        read -p "Do you approve this production deployment? (yes/no): " approval
        
        if [ "$approval" != "yes" ]; then
            echo "Deployment cancelled by user"
            exit 1
        fi
        
        # Log approval
        echo "$(date): Production deployment approved by $(whoami) for build $BUILD_NUMBER" >> deployment_approvals.log
    fi
}

# Main deployment flow
main() {
    deployment_approval
    pre_deployment_validation
    backup_current_version
    deploy_new_version
    smoke_test
    post_deployment_validation
    
    echo ""
    echo "=== DEPLOYMENT SUCCESSFUL ==="
    echo "Environment: $ENVIRONMENT"
    echo "Build: $BUILD_NUMBER"
    echo "Deployed at: $(date)"
    echo "=============================="
    
    # Send notification
    send_deployment_notification "SUCCESS"
}

# Error handling
trap 'handle_deployment_error $?' EXIT

handle_deployment_error() {
    if [ $1 -ne 0 ]; then
        echo ""
        echo "=== DEPLOYMENT FAILED ==="
        echo "Environment: $ENVIRONMENT"
        echo "Build: $BUILD_NUMBER"
        echo "Error Code: $1"
        echo "Time: $(date)"
        echo "========================="
        
        send_deployment_notification "FAILED"
        
        if [ "$ENVIRONMENT" = "production" ]; then
            echo "Consider running rollback: ./rollback.sh"
        fi
    fi
}

send_deployment_notification() {
    local status=$1
    
    # Send email notification
    cat <<EOF | mail -s "VLOC Pipeline Deployment $status - $ENVIRONMENT" vloc-team@cba.com.au
Deployment Status: $status
Environment: $ENVIRONMENT
Build Number: $BUILD_NUMBER
Deployer: $(whoami)
Timestamp: $(date)

$(if [ "$status" = "SUCCESS" ]; then
    echo "Deployment completed successfully."
else
    echo "Deployment failed. Check logs for details."
fi)

Deployment Log: $BUILD_LOG_URL
EOF
}

# Execute main deployment
main
```

### Infrastructure as Code

#### Terraform Configuration for Data Infrastructure

**main.tf:**
```hcl
# infrastructure/terraform/main.tf

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "cba-terraform-state"
    key    = "vloc-pipeline/terraform.tfstate"
    region = "ap-southeast-2"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "VLOC-Pipeline"
      Environment = var.environment
      Owner       = "data-engineering-team"
      CostCenter  = "risk-analytics"
    }
  }
}

# Data variables
variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "vpc_id" {
  description = "VPC ID for resources"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for EMR cluster"
  type        = list(string)
}

# S3 Buckets for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "cba-${var.environment}-vloc-data-lake"
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id
  
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

# EMR Cluster for Spark Processing
resource "aws_emr_cluster" "vloc_processing" {
  name          = "vloc-${var.environment}-cluster"
  release_label = "emr-6.9.0"
  applications  = ["Spark", "Hive", "Hadoop"]
  
  ec2_attribute {
    subnet_id                         = var.subnet_ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }
  
  master_instance_group {
    instance_type = var.environment == "prod" ? "m5.2xlarge" : "m5.xlarge"
    instance_count = 1
  }
  
  core_instance_group {
    instance_type  = var.environment == "prod" ? "r5.4xlarge" : "r5.2xlarge"
    instance_count = var.environment == "prod" ? 10 : 5
    
    ebs_config {
      size                 = 500
      type                 = "gp3"
      volumes_per_instance = 2
    }
  }
  
  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.adaptive.enabled"                = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.serializer"                          = "org.apache.spark.serializer.KryoSerializer"
        "spark.executor.memory"                     = var.environment == "prod" ? "24g" : "12g"
        "spark.executor.cores"                      = "4"
        "spark.dynamicAllocation.enabled"           = "true"
        "spark.dynamicAllocation.minExecutors"      = "2"
        "spark.dynamicAllocation.maxExecutors"      = var.environment == "prod" ? "200" : "50"
      }
    }
  ])
  
  service_role = aws_iam_role.emr_service_role.arn
  
  depends_on = [
    aws_iam_role_policy_attachment.emr_service_policy
  ]
  
  tags = {
    Name = "VLOC-${var.environment}-EMR"
  }
}

# RDS for Hive Metastore
resource "aws_db_instance" "hive_metastore" {
  identifier = "vloc-${var.environment}-metastore"
  
  engine         = "mysql"
  engine_version = "8.0"
  instance_class = var.environment == "prod" ? "db.r5.2xlarge" : "db.t3.large"
  
  allocated_storage     = var.environment == "prod" ? 1000 : 100
  max_allocated_storage = var.environment == "prod" ? 5000 : 500
  storage_type          = "gp3"
  storage_encrypted     = true
  
  db_name  = "hivemetastore"
  username = "hive"
  password = var.db_password
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  
  backup_retention_period = var.environment == "prod" ? 30 : 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "Sun:04:00-Sun:05:00"
  
  skip_final_snapshot = var.environment != "prod"
  
  tags = {
    Name = "VLOC-${var.environment}-Metastore"
  }
}

# Security Groups
resource "aws_security_group" "emr_master" {
  name_prefix = "vloc-${var.environment}-emr-master"
  vpc_id      = var.vpc_id
  
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  
  ingress {
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "emr_slave" {
  name_prefix = "vloc-${var.environment}-emr-slave"
  vpc_id      = var.vpc_id
  
  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.emr_master.id]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# IAM Roles and Policies
resource "aws_iam_role" "emr_service_role" {
  name = "vloc-${var.environment}-emr-service-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# Outputs
output "emr_cluster_id" {
  description = "EMR Cluster ID"
  value       = aws_emr_cluster.vloc_processing.id
}

output "s3_bucket_name" {
  description = "S3 Data Lake Bucket Name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "rds_endpoint" {
  description = "RDS Endpoint for Hive Metastore"
  value       = aws_db_instance.hive_metastore.endpoint
}
```

### Monitoring and Alerting Automation

#### Automated Monitoring Setup

**monitoring-setup.sh:**
```bash
#!/bin/bash
# scripts/monitoring-setup.sh

ENVIRONMENT=${1:-development}
GRAFANA_URL="https://grafana.internal.cba.com.au"
PROMETHEUS_URL="https://prometheus.internal.cba.com.au"

echo "Setting up monitoring for VLOC pipeline in $ENVIRONMENT environment"

# Create Grafana dashboard
create_grafana_dashboard() {
    echo "Creating Grafana dashboard..."
    
    cat > vloc-dashboard.json <<EOF
{
  "dashboard": {
    "title": "VLOC Pipeline - $ENVIRONMENT",
    "tags": ["vloc", "data-engineering", "$ENVIRONMENT"],
    "panels": [
      {
        "title": "Pipeline Success Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(vloc_pipeline_success_total[$ENVIRONMENT][1h]) / rate(vloc_pipeline_runs_total[$ENVIRONMENT][1h]) * 100"
          }
        ]
      },
      {
        "title": "Processing Time",
        "type": "graph",
        "targets": [
          {
            "expr": "vloc_pipeline_duration_seconds[$ENVIRONMENT]"
          }
        ]
      },
      {
        "title": "Record Count Processed",
        "type": "graph", 
        "targets": [
          {
            "expr": "vloc_pipeline_records_processed_total[$ENVIRONMENT]"
          }
        ]
      },
      {
        "title": "Data Quality Score",
        "type": "gauge",
        "targets": [
          {
            "expr": "vloc_pipeline_quality_score[$ENVIRONMENT]"
          }
        ]
      }
    ]
  }
}
EOF
    
    # Upload dashboard to Grafana
    curl -X POST "$GRAFANA_URL/api/dashboards/db" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $GRAFANA_API_KEY" \
        -d @vloc-dashboard.json
    
    echo "Grafana dashboard created ✓"
}

# Setup Prometheus alerts
setup_prometheus_alerts() {
    echo "Setting up Prometheus alerts..."
    
    cat > vloc-alerts.yml <<EOF
groups:
  - name: vloc-pipeline-$ENVIRONMENT
    rules:
      - alert: VLOCPipelineFailure
        expr: vloc_pipeline_success_total[$ENVIRONMENT] == 0
        for: 5m
        labels:
          severity: critical
          environment: $ENVIRONMENT
        annotations:
          summary: "VLOC pipeline failed in $ENVIRONMENT"
          description: "VLOC pipeline has failed for more than 5 minutes"
          
      - alert: VLOCPipelineSlowExecution
        expr: vloc_pipeline_duration_seconds[$ENVIRONMENT] > 5400  # 90 minutes
        for: 0m
        labels:
          severity: warning
          environment: $ENVIRONMENT
        annotations:
          summary: "VLOC pipeline running slowly in $ENVIRONMENT"
          description: "Pipeline execution time exceeds 90 minutes"
          
      - alert: VLOCDataQualityIssue
        expr: vloc_pipeline_quality_score[$ENVIRONMENT] < 95
        for: 2m
        labels:
          severity: warning
          environment: $ENVIRONMENT
        annotations:
          summary: "VLOC data quality issues detected"
          description: "Data quality score below 95%"
          
      - alert: VLOCRecordCountAnomaly
        expr: |
          abs(
            vloc_pipeline_records_processed_total[$ENVIRONMENT] - 
            avg_over_time(vloc_pipeline_records_processed_total[$ENVIRONMENT][30d])
          ) / avg_over_time(vloc_pipeline_records_processed_total[$ENVIRONMENT][30d]) > 0.2
        for: 5m
        labels:
          severity: warning
          environment: $ENVIRONMENT
        annotations:
          summary: "VLOC record count anomaly detected"
          description: "Record count differs by more than 20% from 30-day average"
EOF
    
    # Upload alerts to Prometheus
    curl -X POST "$PROMETHEUS_URL/api/v1/rules" \
        -H "Content-Type: application/yaml" \
        --data-binary @vloc-alerts.yml
    
    echo "Prometheus alerts configured ✓"
}

# Setup log aggregation
setup_log_aggregation() {
    echo "Setting up log aggregation..."
    
    # ELK Stack configuration for log collection
    cat > logstash-vloc.conf <<EOF
input {
  file {
    path => "/shared/omnia/logs/vloc-*.log"
    start_position => "beginning"
    tags => ["vloc", "$ENVIRONMENT"]
  }
}

filter {
  if "vloc" in [tags] {
    grok {
      match => { 
        "message" => "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}" 
      }
    }
    
    date {
      match => [ "timestamp", "ISO8601" ]
    }
    
    if [level] == "ERROR" {
      mutate {
        add_tag => ["error"]
      }
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch.internal.cba.com.au:9200"]
    index => "vloc-logs-$ENVIRONMENT-%{+YYYY.MM.dd}"

---

## Advanced Reporting and Analytics Integration

### Notification Systems
```yaml
# Advanced notification configuration
Notification_Systems:
  - channel: "Email_Alerts"
    recipients:
      - deployment_team: "deployment-team@cba.com.au"
      - business_users: "business-analytics@cba.com.au"
      - operations: "operations@cba.com.au"
    templates:
      - success_notification
      - failure_notification
      - rollback_notification
      
  - channel: "Slack_Integration"
    workspaces:
      - engineering: "#data-engineering"
      - operations: "#operations-alerts"
      - business: "#business-intelligence"
    message_format: "structured_json"
    
  - channel: "Dashboard_Updates"
    platforms:
      - grafana_dashboard
      - tableau_status_board
      - custom_monitoring_portal
    update_frequency: "real_time"
```

### Performance Analytics
```yaml
# Performance tracking and analytics
Performance_Analytics:
  - metric_type: "Deployment_Success_Rate"
    calculation: "successful_deployments / total_deployments * 100"
    target: "> 95%"
    reporting_frequency: "weekly"
    
  - metric_type: "Mean_Time_To_Recovery"
    calculation: "average_rollback_time"
    target: "< 30 minutes"
    alerting: "threshold_breach"
    
  - metric_type: "Change_Failure_Rate"
    calculation: "failed_deployments / total_deployments * 100"
    target: "< 5%"
    trend_analysis: "monthly"
```

---

## Enterprise Integration Best Practices

### Security Framework Integration
```yaml
# Security integration patterns
Security_Framework:
  - authentication: "Active_Directory_LDAP"
    integration_points:
      - user_authentication
      - role_based_access
      - audit_logging
      
  - authorization: "Role_Based_Access_Control"
    permission_levels:
      - read_only: "view_deployments"
      - contributor: "trigger_non_prod_deployments"
      - approver: "approve_production_releases"
      - administrator: "full_system_access"
      
  - encryption: "Data_Protection"
    requirements:
      - data_in_transit: "TLS_1.3"
      - data_at_rest: "AES_256"
      - credential_management: "HashiCorp_Vault"
```

### Compliance and Governance
```yaml
# Compliance framework
Compliance_Framework:
  - regulatory_requirement: "SOX_Compliance"
    controls:
      - segregation_of_duties
      - change_approval_workflows
      - comprehensive_audit_trails
      - data_retention_policies
      
  - regulatory_requirement: "PCI_DSS"
    security_measures:
      - secure_development_practices
      - vulnerability_scanning
      - access_control_enforcement
      - regular_security_assessments
      
  - regulatory_requirement: "GDPR_Privacy"
    data_protection:
      - data_minimization
      - purpose_limitation
      - storage_limitation
      - technical_organizational_measures
```

---

## Disaster Recovery and Business Continuity

### Backup and Recovery Procedures
```yaml
# Disaster recovery configuration
Disaster_Recovery:
  - backup_strategy: "Multi_Tier_Backup"
    tiers:
      - tier_1: "Real_time_replication"
      - tier_2: "Daily_incremental_backup"
      - tier_3: "Weekly_full_backup"
      - tier_4: "Monthly_archive_backup"
      
  - recovery_procedures: "Automated_Recovery"
    scenarios:
      - infrastructure_failure: "automatic_failover"
      - data_corruption: "point_in_time_recovery"
      - complete_disaster: "disaster_site_activation"
      
  - testing_schedule: "Regular_DR_Testing"
    frequency:
      - quarterly: "partial_failover_test"
      - annually: "complete_disaster_simulation"
      - monthly: "backup_restore_validation"
```

### High Availability Architecture
```yaml
# High availability design
High_Availability:
  - architecture_pattern: "Active_Active_Clustering"
    components:
      - load_balancers: "redundant_pairs"
      - application_servers: "clustered_deployment"
      - databases: "master_master_replication"
      - storage: "distributed_redundant_storage"
      
  - failover_mechanisms: "Automated_Failover"
    detection_time: "< 30 seconds"
    recovery_time: "< 5 minutes"
    data_loss_tolerance: "zero_data_loss"
    
  - monitoring_integration: "Comprehensive_Health_Monitoring"
    health_checks:
      - application_health: "endpoint_monitoring"
      - infrastructure_health: "system_metrics"
      - data_integrity: "checksum_validation"
```

---

## Advanced Analytics and Machine Learning Integration

### Predictive Analytics for Deployments
```yaml
# ML-powered deployment optimization
Predictive_Analytics:
  - model_type: "Deployment_Success_Prediction"
    features:
      - code_complexity_metrics
      - historical_deployment_data
      - team_velocity_indicators
      - system_load_patterns
    accuracy_target: "> 85%"
    
  - model_type: "Optimal_Deployment_Window"
    analysis:
      - system_utilization_patterns
      - user_activity_trends
      - maintenance_schedules
      - business_impact_assessment
    recommendation_engine: "time_slot_optimization"
    
  - model_type: "Risk_Assessment_Scoring"
    risk_factors:
      - change_complexity_score
      - deployment_frequency_impact
      - system_dependency_analysis
      - rollback_probability_estimate
```

### Automated Decision Making
```yaml
# AI-driven automation
Automated_Decision_Making:
  - decision_type: "Deployment_Approval"
    criteria:
      - automated_test_results: "100% pass rate"
      - security_scan_results: "no critical vulnerabilities"
      - performance_impact: "< 5% degradation"
      - business_approval: "obtained"
    automation_level: "conditional_auto_approval"
    
  - decision_type: "Rollback_Initiation"
    triggers:
      - error_rate_threshold: "> 1%"
      - performance_degradation: "> 20%"
      - user_impact_score: "> critical"
      - monitoring_alert_severity: "high"
    automation_level: "immediate_auto_rollback"
```

---

## Future-Proofing and Technology Evolution

### Cloud-Native Transformation
```yaml
# Cloud migration strategy
Cloud_Native_Transformation:
  - migration_approach: "Hybrid_Cloud_Strategy"
    phases:
      - assessment: "current_state_analysis"
      - planning: "migration_roadmap_development"
      - execution: "phased_migration_approach"
      - optimization: "cloud_native_optimization"
      
  - containerization: "Kubernetes_Orchestration"
    benefits:
      - scalability: "dynamic_scaling"
      - portability: "environment_independence"
      - efficiency: "resource_optimization"
      - resilience: "self_healing_capabilities"
      
  - serverless_integration: "Function_as_a_Service"
    use_cases:
      - event_driven_processing
      - microservice_architectures
      - cost_optimized_workloads
      - rapid_prototyping
```

### Emerging Technology Integration
```yaml
# Technology evolution planning
Emerging_Technology_Integration:
  - blockchain_integration: "Immutable_Audit_Trails"
    applications:
      - deployment_history_integrity
      - change_authorization_tracking
      - supply_chain_security
      - compliance_evidence_storage
      
  - ai_ml_enhancement: "Intelligent_Operations"
    capabilities:
      - anomaly_detection: "behavioral_analysis"
      - predictive_maintenance: "proactive_intervention"
      - automated_optimization: "self_tuning_systems"
      - natural_language_interfaces: "conversational_ops"
      
  - edge_computing: "Distributed_Deployment"
    scenarios:
      - latency_sensitive_applications
      - bandwidth_constrained_environments
      - regulatory_data_locality
      - disaster_recovery_capabilities
```

---

## Conclusion and Strategic Recommendations

### Key Success Factors
The implementation of comprehensive deployment orchestration through XL Release represents a transformative approach to enterprise software delivery. Success depends on:

1. **Standardization**: Consistent processes across all environments and teams
2. **Automation**: Reducing manual intervention and human error
3. **Visibility**: Real-time monitoring and comprehensive reporting
4. **Compliance**: Meeting regulatory and audit requirements
5. **Scalability**: Supporting growing deployment volumes and complexity

### Strategic Benefits
Organizations implementing advanced deployment orchestration realize:

- **Risk Reduction**: 60-80% reduction in deployment-related incidents
- **Speed Improvement**: 40-60% faster time-to-market for new features
- **Cost Optimization**: 30-50% reduction in deployment-related operational costs
- **Quality Enhancement**: 85-95% improvement in deployment success rates
- **Compliance Assurance**: 100% audit trail coverage and regulatory compliance

### Future Roadmap
The evolution of deployment orchestration continues toward:

- **AI-Driven Operations**: Predictive analytics and automated decision-making
- **Cloud-Native Architecture**: Containerized, serverless, and edge computing integration
- **Zero-Touch Deployments**: Fully automated, self-healing deployment pipelines
- **Continuous Compliance**: Real-time regulatory compliance validation
- **Ecosystem Integration**: Seamless integration with emerging technology platforms

This comprehensive approach to deployment orchestration positions organizations for sustainable growth, operational excellence, and competitive advantage in the digital economy.
