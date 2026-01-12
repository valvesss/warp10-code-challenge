// ============================================================================
// CLINICAL TRIALS KNOWLEDGE GRAPH - DEMONSTRATION CYPHER QUERIES
// ============================================================================
// These queries showcase the analytical capabilities of the graph model.
// Run these in Neo4j Browser after loading data.


// ----------------------------------------------------------------------------
// 1. BASIC GRAPH EXPLORATION
// ----------------------------------------------------------------------------

// 1.1 Count all node types
MATCH (n)
RETURN labels(n)[0] AS NodeType, count(n) AS Count
ORDER BY Count DESC;

// 1.2 Count all relationship types
MATCH ()-[r]->()
RETURN type(r) AS RelationshipType, count(r) AS Count
ORDER BY Count DESC;

// 1.3 Sample graph structure - Show first 5 trials with their connections
MATCH (t:Trial)-[r]->(n)
WHERE t.nct_id IN ['NCT04280705', 'NCT04252274', 'NCT04261517']
RETURN t, r, n
LIMIT 50;


// ----------------------------------------------------------------------------
// 2. DRUG-CENTRIC QUERIES
// ----------------------------------------------------------------------------

// 2.1 Find most studied drugs (drugs in most trials)
MATCH (t:Trial)-[:INVESTIGATES]->(d:Drug)
RETURN d.name AS Drug, 
       d.intervention_type AS Type,
       count(DISTINCT t) AS TrialCount
ORDER BY TrialCount DESC
LIMIT 20;

// 2.2 Find drugs with known route of administration
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WHERE inv.route IS NOT NULL
RETURN d.name AS Drug,
       inv.route AS Route,
       collect(DISTINCT t.nct_id)[0..5] AS SampleTrials,
       count(DISTINCT t) AS TrialCount
ORDER BY TrialCount DESC
LIMIT 20;

// 2.3 Distribution of dosage forms
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WHERE inv.dosage_form IS NOT NULL
RETURN inv.dosage_form AS DosageForm,
       count(DISTINCT d) AS DrugCount,
       count(DISTINCT t) AS TrialCount
ORDER BY TrialCount DESC;

// 2.4 Find drugs and all conditions they target
MATCH (d:Drug)<-[:INVESTIGATES]-(t:Trial)-[:TARGETS]->(c:Condition)
WITH d, collect(DISTINCT c.name) AS Conditions, count(DISTINCT t) AS TrialCount
WHERE TrialCount >= 3
RETURN d.name AS Drug,
       TrialCount,
       size(Conditions) AS ConditionCount,
       Conditions[0..5] AS TopConditions
ORDER BY TrialCount DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 3. ORGANIZATION-CENTRIC QUERIES (REQUIRED BY CHALLENGE)
// ----------------------------------------------------------------------------

// 3.1 REQUIRED QUERY: For a given company, list of associated clinical trials
// Example: List all trials for "National Cancer Institute"
MATCH (t:Trial)-[:SPONSORED_BY|COLLABORATES_WITH]->(o:Organization)
WHERE o.name CONTAINS 'National Cancer Institute'
RETURN t.nct_id AS TrialID,
       t.brief_title AS Title,
       t.phase AS Phase,
       t.overall_status AS Status
ORDER BY t.nct_id
LIMIT 25;

// 3.2 REQUIRED QUERY: Top companies by number of trials
MATCH (t:Trial)-[:SPONSORED_BY]->(o:Organization)
RETURN o.name AS Organization,
       o.agency_class AS Class,
       count(t) AS SponsoredTrials
ORDER BY SponsoredTrials DESC
LIMIT 20;

// 3.2 Organizations with most collaborations
MATCH (t:Trial)-[:COLLABORATES_WITH]->(o:Organization)
RETURN o.name AS Organization,
       count(DISTINCT t) AS Collaborations
ORDER BY Collaborations DESC
LIMIT 20;

// 3.3 Organizations and the conditions they research most
MATCH (o:Organization)<-[:SPONSORED_BY]-(t:Trial)-[:TARGETS]->(c:Condition)
WITH o, c.name AS Condition, count(t) AS TrialCount
ORDER BY o.name, TrialCount DESC
WITH o, collect({condition: Condition, trials: TrialCount})[0..3] AS TopConditions
RETURN o.name AS Organization,
       TopConditions
ORDER BY size([x IN TopConditions | x.trials]) DESC
LIMIT 20;

// 3.4 Industry vs Academic sponsorship comparison
MATCH (t:Trial)-[:SPONSORED_BY]->(o:Organization)
RETURN o.agency_class AS SponsorType,
       count(DISTINCT t) AS TrialCount,
       count(DISTINCT o) AS OrganizationCount
ORDER BY TrialCount DESC;


// ----------------------------------------------------------------------------
// 4. CONDITION-CENTRIC QUERIES
// ----------------------------------------------------------------------------

// 4.1 Most studied conditions
MATCH (t:Trial)-[:TARGETS]->(c:Condition)
RETURN c.name AS Condition,
       count(t) AS TrialCount
ORDER BY TrialCount DESC
LIMIT 20;

// 4.2 Conditions and the drugs being tested for them
MATCH (c:Condition)<-[:TARGETS]-(t:Trial)-[:INVESTIGATES]->(d:Drug)
WITH c, d.name AS Drug, count(DISTINCT t) AS TrialCount
ORDER BY c.name, TrialCount DESC
WITH c, collect({drug: Drug, trials: TrialCount})[0..5] AS TopDrugs
WHERE size(TopDrugs) >= 2
RETURN c.name AS Condition,
       TopDrugs
ORDER BY size([x IN TopDrugs | x.trials]) DESC
LIMIT 20;

// 4.3 Find conditions with Phase 3/4 trials (late-stage research)
MATCH (t:Trial)-[:TARGETS]->(c:Condition)
WHERE t.phase IN ['PHASE3', 'PHASE4', 'PHASE2/PHASE3']
RETURN c.name AS Condition,
       count(DISTINCT t) AS LateStageTrials,
       collect(DISTINCT t.phase) AS Phases
ORDER BY LateStageTrials DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 5. TRIAL ANALYTICS
// ----------------------------------------------------------------------------

// 5.1 Trials by phase distribution
MATCH (t:Trial)
RETURN t.phase AS Phase,
       count(t) AS TrialCount
ORDER BY 
  CASE t.phase
    WHEN 'EARLY_PHASE1' THEN 1
    WHEN 'PHASE1' THEN 2
    WHEN 'PHASE1/PHASE2' THEN 3
    WHEN 'PHASE2' THEN 4
    WHEN 'PHASE2/PHASE3' THEN 5
    WHEN 'PHASE3' THEN 6
    WHEN 'PHASE4' THEN 7
    ELSE 8
  END;

// 5.2 Trials by status
MATCH (t:Trial)
RETURN t.overall_status AS Status,
       t.status_category AS Category,
       count(t) AS TrialCount
ORDER BY TrialCount DESC;

// 5.3 FDA regulated vs non-regulated drugs
MATCH (t:Trial)-[:INVESTIGATES]->(d:Drug)
RETURN t.is_fda_regulated_drug AS FDARegulated,
       count(DISTINCT t) AS TrialCount,
       count(DISTINCT d) AS DrugCount
ORDER BY TrialCount DESC;

// 5.4 Multi-arm trials (trials testing multiple drugs)
MATCH (t:Trial)-[:INVESTIGATES]->(d:Drug)
WITH t, count(d) AS DrugCount
WHERE DrugCount > 1
RETURN t.nct_id AS TrialID,
       t.brief_title AS Title,
       DrugCount AS DrugsUnderTest,
       t.number_of_arms AS Arms
ORDER BY DrugCount DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 6. GRAPH PATTERN MATCHING (Advanced)
// ----------------------------------------------------------------------------

// 6.1 Find drugs tested for multiple conditions (repurposing candidates)
MATCH (d:Drug)<-[:INVESTIGATES]-(t:Trial)-[:TARGETS]->(c:Condition)
WITH d, collect(DISTINCT c.name) AS Conditions
WHERE size(Conditions) >= 3
RETURN d.name AS Drug,
       size(Conditions) AS ConditionCount,
       Conditions
ORDER BY ConditionCount DESC
LIMIT 20;

// 6.2 Find organizations that collaborate frequently
MATCH (o1:Organization)<-[:SPONSORED_BY]-(t:Trial)-[:COLLABORATES_WITH]->(o2:Organization)
WHERE o1 <> o2
WITH o1, o2, count(DISTINCT t) AS SharedTrials
WHERE SharedTrials >= 2
RETURN o1.name AS Sponsor,
       o2.name AS Collaborator,
       SharedTrials
ORDER BY SharedTrials DESC
LIMIT 20;

// 6.3 Find drugs studied by the same organization for different conditions
MATCH (o:Organization)<-[:SPONSORED_BY]-(t1:Trial)-[:INVESTIGATES]->(d:Drug),
      (o)<-[:SPONSORED_BY]-(t2:Trial)-[:TARGETS]->(c1:Condition),
      (t1)-[:TARGETS]->(c2:Condition)
WHERE t1 <> t2 AND c1 <> c2 AND t1.nct_id < t2.nct_id
WITH o, d, collect(DISTINCT c1.name) + collect(DISTINCT c2.name) AS AllConditions
WHERE size(AllConditions) >= 3
RETURN o.name AS Organization,
       d.name AS Drug,
       AllConditions
LIMIT 10;

// 6.4 Pathway analysis: Organizations -> Trials -> Drugs -> Conditions
MATCH path = (o:Organization)<-[:SPONSORED_BY]-(t:Trial)-[:INVESTIGATES]->(d:Drug)
WHERE o.agency_class = 'INDUSTRY'
MATCH (t)-[:TARGETS]->(c:Condition)
WITH o, d, collect(DISTINCT c.name) AS Conditions, count(DISTINCT t) AS TrialCount
WHERE TrialCount >= 2
RETURN o.name AS Sponsor,
       d.name AS Drug,
       TrialCount,
       Conditions
ORDER BY TrialCount DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 7. ROUTE/DOSAGE FORM ANALYSIS (REQUIRED BY CHALLENGE)
// ----------------------------------------------------------------------------

// 7.1 REQUIRED QUERY: Route and dosage form coverage
// Shows how many trials have route/dosage form captured
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WITH count(*) AS Total,
     sum(CASE WHEN inv.route IS NOT NULL THEN 1 ELSE 0 END) AS WithRoute,
     sum(CASE WHEN inv.dosage_form IS NOT NULL THEN 1 ELSE 0 END) AS WithDosageForm
RETURN Total AS TotalDrugTrialRelations,
       WithRoute,
       round(100.0 * WithRoute / Total, 1) AS RoutePercent,
       WithDosageForm,
       round(100.0 * WithDosageForm / Total, 1) AS DosageFormPercent;

// 7.2 Distribution of routes of administration
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
RETURN inv.route AS Route,
       count(DISTINCT d) AS DrugCount,
       count(DISTINCT t) AS TrialCount
ORDER BY TrialCount DESC;

// 7.2 Oral drugs and their conditions
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WHERE inv.route = 'ORAL'
MATCH (t)-[:TARGETS]->(c:Condition)
RETURN d.name AS Drug,
       collect(DISTINCT c.name)[0..3] AS Conditions,
       count(DISTINCT t) AS TrialCount
ORDER BY TrialCount DESC
LIMIT 15;

// 7.3 Injectable drugs (IV, IM, SC) overview
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WHERE inv.route IN ['INTRAVENOUS', 'INTRAMUSCULAR', 'SUBCUTANEOUS']
RETURN inv.route AS Route,
       d.name AS Drug,
       count(DISTINCT t) AS TrialCount
ORDER BY Route, TrialCount DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 8. GRAPH VISUALIZATION QUERIES
// ----------------------------------------------------------------------------

// 8.1 Visualize a condition and all related entities
MATCH (c:Condition {name: 'COVID-19'})<-[:TARGETS]-(t:Trial)
MATCH (t)-[:INVESTIGATES]->(d:Drug)
MATCH (t)-[:SPONSORED_BY]->(o:Organization)
RETURN c, t, d, o
LIMIT 50;

// 8.2 Organization network (sponsors and their collaborators)
MATCH (o1:Organization)<-[:SPONSORED_BY]-(t:Trial)-[:COLLABORATES_WITH]->(o2:Organization)
RETURN o1, t, o2
LIMIT 100;

// 8.3 Drug similarity network (drugs tested for same conditions)
MATCH (d1:Drug)<-[:INVESTIGATES]-(t1:Trial)-[:TARGETS]->(c:Condition)<-[:TARGETS]-(t2:Trial)-[:INVESTIGATES]->(d2:Drug)
WHERE d1 <> d2 AND d1.name < d2.name
WITH d1, d2, collect(DISTINCT c.name) AS SharedConditions
WHERE size(SharedConditions) >= 2
RETURN d1.name AS Drug1, 
       d2.name AS Drug2,
       size(SharedConditions) AS SharedConditionCount,
       SharedConditions
ORDER BY SharedConditionCount DESC
LIMIT 20;


// ----------------------------------------------------------------------------
// 9. DATA QUALITY QUERIES
// ----------------------------------------------------------------------------

// 9.1 Trials without any drugs (data quality check)
MATCH (t:Trial)
WHERE NOT (t)-[:INVESTIGATES]->(:Drug)
RETURN count(t) AS TrialsWithoutDrugs;

// 9.2 Orphan drugs (drugs only in one trial)
MATCH (d:Drug)<-[:INVESTIGATES]-(t:Trial)
WITH d, count(t) AS TrialCount
WHERE TrialCount = 1
RETURN d.name AS Drug, d.intervention_type AS Type
ORDER BY d.name
LIMIT 20;

// 9.3 Coverage of route extraction
MATCH (t:Trial)-[inv:INVESTIGATES]->(d:Drug)
WITH count(*) AS Total,
     sum(CASE WHEN inv.route IS NOT NULL THEN 1 ELSE 0 END) AS WithRoute
RETURN Total,
       WithRoute,
       round(100.0 * WithRoute / Total, 1) AS RoutePercent;


// ----------------------------------------------------------------------------
// 10. FULL-TEXT SEARCH SETUP (Optional Enhancement)
// ----------------------------------------------------------------------------

// Create full-text index for drug names
// CALL db.index.fulltext.createNodeIndex("drugSearch", ["Drug"], ["name", "name_original"]);

// Search for drugs containing "aspirin"
// CALL db.index.fulltext.queryNodes("drugSearch", "aspirin*") YIELD node
// RETURN node.name AS Drug, node.intervention_type AS Type;

