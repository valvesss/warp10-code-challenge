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

## 📊 Data Coverage

From 1,000 clinical trials (Drug/Biological, Phase 1-4):

| Entity | Count | Notes |
|--------|-------|-------|
| Trials | 1,000 | Core study data |
| Organizations | 169 | Deduplicated sponsors |
| Drugs | 1,851 | Includes biologicals |
| Conditions | 1,918 | Disease targets |
| Trial-Org Relations | 1,474 | Sponsors + collaborators |

**Route/Dosage Extraction:**
- Route coverage: ~7% (extracted via regex)
- Dosage form coverage: ~2.3%

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

## 🔍 Sample Queries

After loading, explore with Cypher in Neo4j Browser:

```cypher
-- Top sponsors
MATCH (t:Trial)-[:SPONSORED_BY]->(o:Organization)
RETURN o.name AS Sponsor, count(t) AS Trials
ORDER BY Trials DESC LIMIT 10;

-- Drugs with known routes
MATCH (t:Trial)-[r:INVESTIGATES]->(d:Drug)
WHERE r.route IS NOT NULL
RETURN d.name, r.route, count(t) AS Trials
ORDER BY Trials DESC LIMIT 20;

-- Drug repurposing candidates (tested for multiple conditions)
MATCH (d:Drug)<-[:INVESTIGATES]-(t:Trial)-[:TARGETS]->(c:Condition)
WITH d, collect(DISTINCT c.name) AS Conditions
WHERE size(Conditions) >= 3
RETURN d.name, Conditions
ORDER BY size(Conditions) DESC;
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

## ⚡ Design Decisions

### Why Route as Edge Property?

Route of administration is not a direct AACT field. Instead of creating sparse `Route` nodes, we:
1. Extract via regex patterns (~7% coverage)
2. Store as property on `INVESTIGATES` relationship
3. Allow NULL for unknown routes

### Idempotency

All loads use `MERGE` statements:
- Safe to re-run on failures
- Incremental updates preserve existing data
- `updated_at` timestamp tracks freshness

### Normalization Strategy

Organizations are heavily deduplicated (1,498 → 169) to enable:
- Proper organization network analysis
- Accurate sponsorship counts
- Cross-trial collaboration discovery

## 📈 Future Enhancements

1. **MeSH Term Integration**: Add drug classification hierarchy
2. **Outcome Analysis**: Parse primary/secondary outcomes
3. **Facility Networks**: Model trial site relationships
4. **NLP Enhancement**: Use ML for better route extraction
5. **Change Data Capture**: Track schema/data changes

## 📄 License

MIT License - See LICENSE file

## 🙏 Acknowledgments

- [AACT Database](https://aact.ctti-clinicaltrials.org/) - Clinical Trials data source
- [ClinicalTrials.gov](https://clinicaltrials.gov/) - Original data provider
