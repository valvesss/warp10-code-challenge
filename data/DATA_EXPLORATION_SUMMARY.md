# Data Exploration Summary

## Overview

Extracted data from AACT database (ClinicalTrials.gov mirror):
- **1,000 clinical trials** (Drug/Biological interventions, Phase 1-4)
- **10 tables** extracted with **27,154 total records**

## Data Quality Assessment

### Studies Table
- **100% coverage** for core fields: nct_id, title, phase, status, dates
- Phase distribution: PHASE2 (40%), PHASE1 (34%), PHASE3 (22%), PHASE4 (4%)
- All studies are INTERVENTIONAL type

### Organizations (Sponsors)
- **1,474 sponsor records** for 1,000 studies
- 1,000 lead sponsors (100% coverage)
- 474 collaborators
- Agency class: NIH (68%), OTHER (16%), INDUSTRY (14%)
- **Good normalization needed**: Many organization name variants

### Interventions (Drugs/Biologicals)
- **1,851 drug/biological interventions**
- Only **8.7% have descriptions** (161 records)
- Drug names are generally clean but need normalization

### Route of Administration

**Challenge**: Route is NOT a direct field in AACT.

**Extraction Sources**:
| Source | Coverage | Notes |
|--------|----------|-------|
| `interventions.description` | 4.3% | Sparse, but high quality when present |
| `interventions.name` | ~2% | Some drugs have route in name |
| `design_groups.description` | 7-14% | Better coverage for treatment arms |

**Detected Routes** (from intervention + design_group descriptions):
- Intravenous (IV): 33 interventions + 30 design_groups
- Oral: 26 interventions + 16 design_groups
- Subcutaneous: 8 interventions + 6 design_groups
- Topical: 14 interventions + 8 design_groups
- Inhalation: 2 interventions + 11 design_groups

### Dosage Form

**Even lower coverage than route** (~1-2% detectable):
- Tablet, capsule, injection, patch, drops, etc.
- Must be extracted via regex patterns from descriptions

## Recommended Graph Model

Based on data analysis, recommended approach:

### Nodes
1. **Trial** (nct_id, title, phase, status, dates) - 100% coverage
2. **Organization** (name, normalized_name, agency_class) - 100% coverage  
3. **Drug** (name, normalized_name, intervention_type) - 100% coverage
4. **Condition** (name) - 100% coverage

### Route/Dosage Representation
Given low coverage (~5-15%), recommend:
- **Option A (Recommended)**: Store as properties on Drug or a DrugInTrial edge
- **Option B**: Create Route/DosageForm nodes only when data exists

### Relationships
- `(Trial)-[:SPONSORED_BY]->(Organization)` - lead sponsor
- `(Trial)-[:COLLABORATES_WITH]->(Organization)` - collaborators
- `(Trial)-[:INVESTIGATES]->(Drug)` - with route/dosage as edge properties
- `(Trial)-[:TARGETS]->(Condition)`

## Data Files Generated

```
data/raw/
├── studies_*.parquet           (130 KB, 1,000 rows)
├── sponsors_*.parquet          (25 KB, 1,474 rows)
├── interventions_*.parquet     (48 KB, 1,980 rows)
├── conditions_*.parquet        (39 KB, 1,918 rows)
├── design_groups_*.parquet     (221 rows)
├── browse_interventions_*.parquet (148 KB, 11,692 rows - MeSH terms)
├── facilities_*.parquet        (108 KB, 6,716 rows)
├── eligibilities_*.parquet     (666 KB, 1,000 rows)
├── design_outcomes_*.parquet   (50 KB, 780 rows)
├── responsible_parties_*.parquet (12 KB, 439 rows)
└── intervention_other_names_*.parquet (7 KB, 155 rows)
```

## Key Limitations

1. **Route/Dosage Form**: ~5-15% coverage, must be extracted via NLP/regex
2. **Organization Normalization**: Same org appears with different name variants
3. **Drug Names**: No standard ontology mapping (would need RxNorm/ChEMBL)
4. **Historical Data**: Older trials have less structured data

## Next Steps

1. Implement transformation layer with:
   - Organization name normalization
   - Route/dosage extraction via regex
   - Drug name deduplication
2. Create Neo4j graph model
3. Implement idempotent loader
4. Add Airflow orchestration


