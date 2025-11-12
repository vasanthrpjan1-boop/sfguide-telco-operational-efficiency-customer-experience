# Accelerating Operational Efficiency and Customer Experience in Telecommunications with Snowflake Intelligence

**A Hands-On Lab Leveraging Cortex AI, Semantic Models, and Unstructured Data Analytics**

*Featuring real-world CelcomDigi Malaysia telecommunications use case*

## Overview

This lab focuses on two key use cases:
- **Operational Efficiency**: Network performance monitoring, capacity planning, tower optimization
- **Customer Experience**: Call transcript analysis, CSAT tracking, churn prediction, sentiment analysis

## Components

| Component | Description | Details |
|-----------|-------------|---------|
| CSV Data | Structured operational data | 81,425 records (7.5 MB) |
| Audio Files | Customer support call recordings | 25 MP3 files (11 MB) |
| PDF Documents | CelcomDigi help documentation | 8 PDF files (4.7 MB) |
| Notebook | Hands-on processing lab | 9 cells |
| SQL Scripts | Database setup and teardown | Complete automation |
| Semantic Models | YAML definitions for structured data | 3 files |

### Data Breakdown
- `network_performance.csv` - 49,864 records from 23 towers across Malaysia
- `customer_feedback_summary.csv` - 20,061 daily feedback aggregations
- `customer_details.csv` - 10,000 customers (Postpaid, Prepaid, Business, Enterprise)
- `infrastructure_capacity.csv` - 1,500 tower capacity snapshots
- `csat_surveys.csv` - 25 customer satisfaction scores
- `customer_interaction_history.csv` - 25 customer interaction summaries

**Coverage**: Kuala Lumpur, Selangor, Penang, Johor, Sabah, Sarawak, Melaka, Perak

## Setup Instructions

Total Time: ~70 minutes

### Step 1: Run Setup Script (5 mins)

Open `scripts/setup.sql` in Snowsight and execute all commands.

**Creates:** Database, tables, views, stages, Cortex Search service

### Step 2: Upload All Files to RAW_FILES Stage (10 mins)

Navigate: Data → CELCOMDIGI_INTELLIGENCE_DB → ANALYTICS → Stages → RAW_FILES

**Upload all files to stage root (no subfolders needed):**
1. Upload 6 CSV files from `data/` folder
2. Upload 25 MP3 files from `audio_files/` folder
3. Upload 8 PDF files from `data/pdfs_to_upload/` folder

**Total: 39 files in @raw_files stage**

**Verify:**
```sql
LIST @raw_files;
-- Should show: 6 CSV + 25 MP3 + 8 PDF = 39 files
```

**Note:** Notebook will automatically identify file types by extension (.csv, .mp3, .pdf)

### Step 3: Run Data Processing Notebook (15 mins)

Run: `notebooks/data_processing.ipynb`

**This notebook:**
- Loads all 6 CSV files to tables (structured data)
- Processes 25 audio files with AI_TRANSCRIBE
- Processes 8 PDF files with AI_PARSE_DOCUMENT
- Applies AI sentiment, summarization, classification
- Saves everything to tables

### Step 4: Upload Semantic Models (5 mins)

Navigate to: **Stages** → **SEMANTIC_MODELS**

Upload 3 YAML files from `scripts/semantic_models/`:
- network_performance.yaml
- infrastructure_capacity.yaml
- customer_feedback.yaml

### Step 5: Run Hands-On Lab Notebook (20 mins)

Run: `notebooks/intelligence_lab.ipynb`

**Important:** Before running, add `matplotlib` package:
- In notebook settings, click "Packages"
- Type "matplotlib" and add it
- This enables all visualizations

**11 hands-on exercises:**
1. Explore processed transcripts and documents
2. Try AISQL functions interactively
3. Sentiment analysis with charts
4. Combine structured + unstructured data
5. Test Cortex Search
6. Customer 360 analysis
7. Revenue at risk calculations
8. Network impact visualization
9. Custom analysis workspace

### Step 6: Create Cortex Agent (5 mins)

Navigate to: **AI & ML** → **Agents** → **Create Agent**

1. Name: `CelcomDigi_Intelligence_Agent`
2. Schema: Select `CELCOMDIGI_INTELLIGENCE_DB.AGENTS`
3. Description: "Intelligence agent for CelcomDigi operations"
4. Add Tools:
   - Add all 3 semantic model files from SEMANTIC_MODELS stage
   - Add Cortex Search service: `celcomdigi_feedback_search`
5. Warehouse: CELCOMDIGI_INTELLIGENCE_WH
6. Timeout: 60 seconds
7. Click **Create**

## Example Queries

### Network Operations
- "Which 5G towers in Kuala Lumpur have the highest latency?"
- "Show me towers at 80% capacity that need upgrades"
- "What is the network performance trend in Penang?"

### Customer Experience
- "What are customers in Penang complaining about?"
- "Which customers have the lowest CSAT scores?"
- "Show me calls where customers mentioned switching to Maxis"
- "What does the help center say about roaming charges?" (PDF search)

### Business Intelligence
- "What revenue is at risk from at-risk customers?"
- "Which customer segment has the highest churn risk?"
- "Are network issues correlated with customer churn?"

## Data Model

### Core Tables
- **CUSTOMER_DETAILS**: 10,000 customers
- **NETWORK_PERFORMANCE**: 49,864 hourly measurements
- **CUSTOMER_CALL_TRANSCRIPTS**: Processed audio recordings
- **CUSTOMER_COMPLAINT_DOCUMENTS**: Processed PDF documents
- **CSAT_SURVEYS**: Customer satisfaction scores

### Analytical Views
- **customer_360_view**: Complete customer profile with CSAT and risk flags
- **call_analysis_view**: Enriched call data with customer context
- **network_customer_impact**: Links network issues to customer complaints
- **churn_risk_analysis**: Calculates churn probability and revenue impact

### Key Relationships
```
CUSTOMER_DETAILS (10K)
    ↓ (1:N)
CUSTOMER_CALL_TRANSCRIPTS (25-200)
    ↓ (1:1)
CSAT_SURVEYS (25-200)
    ↓ (aggregated)
CUSTOMER_INTERACTION_HISTORY
    → is_at_risk flag
    → churn_risk_score
```

## Cortex Search

The search service `celcomdigi_feedback_search` indexes:
- Audio call transcripts (processed by AI_TRANSCRIBE)
- PDF documents (processed by AI_PARSE_DOCUMENT)
- Unified view combining all customer feedback

Enables semantic search across structured and unstructured data.

## Validation

After setup, run:

```sql
-- Check data volume
SELECT 'network_performance', COUNT(*) FROM network_performance
UNION SELECT 'customer_details', COUNT(*) FROM customer_details
UNION SELECT 'csat_surveys', COUNT(*) FROM csat_surveys;

-- Check audio processed
SELECT COUNT(*) FROM customer_call_transcripts;

-- Check PDF processed
SELECT COUNT(*) FROM customer_complaint_documents;

-- Check Cortex Search
SHOW CORTEX SEARCH SERVICES;

-- Test views
SELECT * FROM customer_360_view WHERE is_at_risk = TRUE LIMIT 5;
```

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| Tables empty after load | Check CSV file format and column mapping |
| Audio transcription fails | Verify MP3 files uploaded to @raw_files/audio/ |
| PDF parsing fails | Verify PDF files uploaded to @raw_files/pdfs/ |
| Views return errors | Ensure all base tables have data |
| Agent cannot answer | Upload semantic models to @semantic_models |
| Search service not found | Re-run setup.sql |

## Technical Specifications

**Database Objects:**
- Tables: 9
- Views: 4
- Stages: 3
- Cortex Search Services: 1

**Cortex AI Functions:**
- AI_TRANSCRIBE (audio to text)
- AI_PARSE_DOCUMENT (PDF text extraction)
- AI_SENTIMENT (sentiment analysis)
- AI_CLASSIFY (categorization)
- AI_COMPLETE (LLM reasoning)
- SUMMARIZE (text summarization)

**Data Characteristics:**
- Date Range: November 2024 - January 2025
- Regions: 8 Malaysian states
- Towers: 23 (13 x 5G, 10 x 4G)
- Customer Segments: Postpaid, Prepaid, Business, Enterprise
- Languages: English (for AI processing)

## Malaysian Context

**CelcomDigi Specific:**
- Actual plan names: Postpaid 40/60/80/100, Business Pro/Elite, Enterprise 300/500
- Malaysian Ringgit (RM) pricing
- Competitor references: Maxis, U Mobile, Digi
- Local landmarks: KLCC, Bayan Lepas, Johor Bahru, Warisan Square, Warisan Square

**Realistic Scenarios:**
- Network capacity issues in industrial areas
- 5G coverage in high-rise buildings
- Cross-border roaming charges (Malaysia-Singapore)
- Business customer escalations
- Support quality variations

## Repository Structure

```
├── README.md                        # This file
├── LICENSE                          # License
├── LEGAL.md                         # Legal notices
├── data/                            # CSV files and PDFs
│   ├── *.csv (6 files)
│   └── pdfs_to_upload/ (8 PDFs)
├── audio_files/                     # 25 MP3 files
├── notebooks/                       # Processing notebook
├── scripts/
│   ├── setup.sql
│   ├── teardown.sql
│   └── semantic_models/ (3 YAML)
```

## Next Steps

After completing setup:
1. Explore the analytical views in Snowsight
2. Test queries with the Cortex Agent
3. Analyze customer interaction patterns
4. Identify network optimization opportunities
5. Build custom dashboards using the data

## Support

For questions or issues during setup, refer to the inline comments in setup.sql and the notebook cells.

---

**Ready to start:** Follow Step 1 above.

**Estimated completion time:** 60 minutes
