# Clinical Trials Knowledge Graph Pipeline

A data engineering pipeline that extracts clinical trial data from AACT (Aggregate Analysis of ClinicalTrials.gov), transforms it, and loads it into a Neo4j knowledge graph database.

## 🎯 Overview

This pipeline implements a complete ETL workflow:

```
AACT (PostgreSQL) → Extract → Transform → Neo4j (Graph DB)
```

### Graph Model

```
                    ┌─────────────────┐
                    │   Organization  │
                    │  ─────────────  │
                    │  org_key        │
                    │  name           │
                    │  agency_class   │
                    └────────▲────────┘
                             │
            SPONSORED_BY ────┤
            COLLABORATES_WITH┘
                             │
┌─────────────┐      ┌───────┴───────┐      ┌─────────────┐
│    Drug     │◄─────│     Trial     │─────►│  Condition  │
│ ─────────── │      │ ───────────── │      │ ─────────── │
│ drug_key    │      │ nct_id        │      │ condition_key│
│ name        │      │ brief_title   │      │ name        │
│ type        │      │ phase         │      └─────────────┘
└─────────────┘      │ status        │        TARGETS
   INVESTIGATES      │ enrollment    │
   [route,           └───────────────┘
    dosage_form]
```

## 📊 Data Query Choice & Rationale

**Query Parameters:**
- **Intervention Types**: `DRUG`, `BIOLOGICAL` — Focus on pharmaceutical interventions
- **Phases**: Phase 1 through Phase 4 — Capture all clinical development stages
- **Limit**: 1,000 studies — Non-trivial dataset demonstrating scalability

**Rationale:**
- Drug/Biological trials contain the most relevant intervention data for knowledge graphs
- Phase filtering ensures we capture trials with structured clinical data
- 1,000 studies provides sufficient variety while keeping demo manageable

## 📊 Data Coverage

From 1,000 clinical trials (Drug/Biological, Phase 1-4):

| Entity | Count | Notes |
|--------|-------|-------|
| Trials | 1,000 | Core study data |
| Organizations | 169 | Deduplicated from 1,498 raw |
| Drugs | 687 | Unique drug entities |
| Conditions | 466 | Unique conditions |
| SPONSORED_BY | ~850 | Lead sponsor relations |
| COLLABORATES_WITH | ~600 | Collaborator relations |
| INVESTIGATES | 1,851 | Drug-trial connections |
| TARGETS | 1,918 | Condition-trial connections |

**Route/Dosage Extraction:**
- Route coverage: ~7% (130 drug-trial relations with route)
- Dosage form coverage: ~2.3% (43 relations with dosage form)

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose (for Neo4j)
- AACT database credentials ([register here](https://aact.ctti-clinicaltrials.org/users/sign_up))

### 1. Setup

```bash
# Clone and setup
cd warp10-code-challenge

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env with your AACT and Neo4j credentials
```

### 2. Configure `.env`

```env
# AACT Database
AACT_HOST=aact-db.ctti-clinicaltrials.org
AACT_PORT=5432
AACT_DATABASE=aact
AACT_USER=your_username
AACT_PASSWORD=your_password

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password

# Extraction settings
EXTRACTION_LIMIT=1000
LOG_LEVEL=INFO
```

### 3. Start Neo4j

```bash
docker-compose up -d neo4j
```

### 4. Run Pipeline

```bash
# Option A: Run each step manually
python scripts/extract_data.py --limit 1000
python scripts/transform_data.py
python scripts/load_neo4j.py

# Option B: Run via Airflow DAG (see below)
```

## 📁 Project Structure

```
warp10-code-challenge/
├── config/
│   └── settings.py           # Configuration management
├── dags/
│   └── clinical_trials_pipeline.py  # Airflow DAG
├── data/
│   ├── raw/                  # Extracted Parquet files
│   └── staged/               # Transformed data
├── queries/
│   └── demo_queries.cypher   # Sample Cypher queries
├── scripts/
│   ├── extract_data.py       # CLI: Extract from AACT
│   ├── transform_data.py     # CLI: Transform raw → staged
│   └── load_neo4j.py         # CLI: Load into Neo4j
├── src/
│   ├── ingestion/
│   │   └── aact_extractor.py # AACT database extraction
│   ├── transformation/
│   │   ├── normalizers.py    # Org/drug name normalization
│   │   ├── extractors.py     # Route/dosage extraction
│   │   └── staged_transformer.py  # Full transformation
│   ├── loading/
│   │   └── neo4j_loader.py   # Neo4j batch loader
│   └── utils/
│       └── logging_config.py # Structured logging
├── tests/
│   ├── test_normalizers.py
│   └── test_extractors.py
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## 🔧 Components

### Extraction (`src/ingestion/`)

Extracts from AACT PostgreSQL:
- **Studies**: Trial metadata (phase, status, dates)
- **Sponsors**: Lead sponsors and collaborators
- **Interventions**: Drugs and biologicals
- **Conditions**: Target diseases
- **Design Groups**: Treatment arms

### Transformation (`src/transformation/`)

**Normalizers:**
- `OrganizationNormalizer`: Removes suffixes (Inc., Ltd., Corp.), normalizes whitespace
- `DrugNormalizer`: Removes dosage information, normalizes names

**Extractors:**
- `RouteExtractor`: Regex-based extraction of administration routes
- `DosageFormExtractor`: Identifies dosage forms (tablet, capsule, etc.)

### Loading (`src/loading/`)

- MERGE operations for idempotency
- Batch processing (500 records/batch)
- Constraint and index creation
- Route/dosage form stored as relationship properties

## 🔍 Required Queries & Sample Outputs

### Query 1: For a given company, list associated trials

```cypher
MATCH (t:Trial)-[:SPONSORED_BY|COLLABORATES_WITH]->(o:Organization)
WHERE o.name CONTAINS 'National Cancer Institute'
RETURN t.nct_id, t.brief_title, t.phase, t.overall_status
LIMIT 10;
```

**Sample Output:**
```
╒═══════════════╤════════════════════════════════════════╤═════════╤══════════════╕
│nct_id         │brief_title                             │phase    │overall_status│
╞═══════════════╪════════════════════════════════════════╪═════════╪══════════════╡
│"NCT00000625"  │"A Phase I Study of Azidothymidine..."  │"PHASE1" │"COMPLETED"   │
│"NCT00000628"  │"A Randomized Trial Comparing..."       │"PHASE3" │"COMPLETED"   │
│"NCT00000631"  │"An Open Study of Foscarnet..."         │"PHASE1" │"COMPLETED"   │
└───────────────┴────────────────────────────────────────┴─────────┴──────────────┘
```

### Query 2: Top companies by number of trials

```cypher
MATCH (t:Trial)-[:SPONSORED_BY]->(o:Organization)
RETURN o.name AS Organization, count(t) AS Trials
ORDER BY Trials DESC LIMIT 10;
```

**Sample Output:**
```
╒══════════════════════════════════════════════════════╤════════╕
│Organization                                          │Trials  │
╞══════════════════════════════════════════════════════╪════════╡
│"National Institute of Allergy and Infectious..."     │400     │
│"National Cancer Institute"                           │97      │
│"National Heart, Lung, and Blood Institute"           │96      │
│"National Institute on Drug Abuse"                    │82      │
│"Yale University"                                     │21      │
└──────────────────────────────────────────────────────┴────────┘
```

### Query 3: Route and dosage form coverage

```cypher
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WITH count(*) AS Total,
     sum(CASE WHEN inv.route IS NOT NULL THEN 1 ELSE 0 END) AS WithRoute,
     sum(CASE WHEN inv.dosage_form IS NOT NULL THEN 1 ELSE 0 END) AS WithDosageForm
RETURN Total, WithRoute, round(100.0*WithRoute/Total, 1) AS RoutePercent,
       WithDosageForm, round(100.0*WithDosageForm/Total, 1) AS DosageFormPercent;
```

**Sample Output:**
```
╒═══════╤══════════╤══════════════╤════════════════╤══════════════════╕
│Total  │WithRoute │RoutePercent  │WithDosageForm  │DosageFormPercent │
╞═══════╪══════════╪══════════════╪════════════════╪══════════════════╡
│1851   │130       │7.0           │43              │2.3               │
└───────┴──────────┴──────────────┴────────────────┴──────────────────┘
```

See `queries/demo_queries.cypher` for 50+ demonstration queries.

## ⚙️ Airflow Orchestration

The DAG (`dags/clinical_trials_pipeline.py`) runs:

1. **extract_from_aact** → Extract latest data
2. **transform_to_staged** → Normalize and enrich
3. **load_to_neo4j** → Load graph
4. **validate_graph** → Verify counts

Schedule: Daily at 2 AM UTC

```bash
# Test DAG locally
python dags/clinical_trials_pipeline.py
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## 🐳 Docker Deployment

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## ⚡ Design Decisions & Assumptions

### Route/Dosage Form Extraction

**Challenge**: Route of administration and dosage form are NOT direct fields in AACT.

**Approach**: Regex-based extraction from:
- `intervention_name` field
- `intervention_description` field  
- `design_group_description` field

**Limitations**:
- ~7% route coverage (130/1851 drug-trial relations)
- ~2.3% dosage form coverage (43/1851 relations)
- Misses routes mentioned in free-text clinical descriptions
- No disambiguation of ambiguous terms

**Alternative considered**: Creating dedicated `Route` and `DosageForm` nodes. Rejected due to sparse data—would create mostly disconnected nodes. Edge properties better represent the trial-specific nature of administration.

### Organization Normalization

**Challenge**: Same organization appears with different names (e.g., "Pfizer Inc.", "Pfizer, Inc", "Pfizer").

**Approach**:
1. Remove common suffixes: Inc., Ltd., Corp., LLC, GmbH, etc.
2. Normalize whitespace and case
3. Generate `org_key` for deduplication

**Result**: 1,498 raw records → 169 unique organizations

### Idempotency

All loads use `MERGE` statements:
- Safe to re-run on failures
- Incremental updates preserve existing data
- `updated_at` timestamp tracks freshness

## 📈 Next Steps (With More Time)

### Short-term Improvements
1. **Better Route Extraction**: Use NLP/spaCy with clinical NER models
2. **MeSH Term Integration**: Add `browse_interventions` as drug classification hierarchy
3. **Facility Nodes**: Model trial sites for geographic analysis

### Medium-term Enhancements
4. **Incremental Ingestion**: Track last extraction date, only fetch new/updated trials
5. **Outcome Parsing**: Extract primary/secondary endpoints from `design_outcomes`
6. **Entity Resolution**: Use fuzzy matching for organization names

### Long-term Vision
7. **External Enrichment**: Link drugs to DrugBank, PubChem
8. **Change Data Capture**: Track schema/data changes over time
9. **GraphQL API**: Expose graph via API for downstream applications

## 📄 License

MIT License - See LICENSE file

## 🙏 Acknowledgments

- [AACT Database](https://aact.ctti-clinicaltrials.org/) - Clinical Trials data source
- [ClinicalTrials.gov](https://clinicaltrials.gov/) - Original data provider
