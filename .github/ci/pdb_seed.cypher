// Seed for the CI "PDB": an empty neo4j:4.4 with the production DB's uniqueness constraints and the few
// ontology terms and relations the flybase2neo loaders link to, so a test exercises the code against data we
// control instead of downloading the 13 GB live PDB (which does not fit, or finish, on a GitHub runner).
// Labels follow the PDB: classes are :Class:Entity, relations :Property:Entity. Keep it small.

CREATE CONSTRAINT IF NOT EXISTS ON (n:Class) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Class) ASSERT n.short_form IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Individual) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Individual) ASSERT n.short_form IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Entity) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Feature) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Feature) ASSERT n.short_form IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Property) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Site) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:Site) ASSERT n.short_form IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:pub) ASSERT n.iri IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (n:pub) ASSERT n.short_form IS UNIQUE;

// classes the loaders hang features and expression patterns on
UNWIND [
  ['CARO_0030002', 'expression pattern', 'http://purl.obolibrary.org/obo/'],
  ['VFBext_0000010', 'split expression pattern', 'http://purl.obolibrary.org/obo/'],
  ['SO_0000110', 'sequence_feature', 'http://purl.obolibrary.org/obo/'],
  ['SO_0000704', 'gene', 'http://purl.obolibrary.org/obo/'],
  ['SO_0000797', 'natural_transposable_element', 'http://purl.obolibrary.org/obo/'],
  ['SO_0000902', 'transgene', 'http://purl.obolibrary.org/obo/'],
  ['SO_0001023', 'allele', 'http://purl.obolibrary.org/obo/'],
  ['SO_1000183', 'chromosome_structure_variation', 'http://purl.obolibrary.org/obo/'],
  ['GENO_0000418', 'inserted transgene', 'http://purl.obolibrary.org/obo/'],
  ['FBbt_00000001', 'organism', 'http://purl.obolibrary.org/obo/'],
  ['FBbt_00005106', 'neuron', 'http://purl.obolibrary.org/obo/'],
  ['FBbt_00003624', 'adult brain', 'http://purl.obolibrary.org/obo/'],
  ['FBdv_00005369', 'adult stage', 'http://purl.obolibrary.org/obo/']
] AS c
MERGE (n:Class:Entity { iri: c[2] + c[0] }) SET n.short_form = c[0], n.label = c[1];

MATCH (n:Class { short_form: 'FBbt_00005106' }) SET n:Cell:Neuron:Anatomy;
MATCH (n:Class) WHERE n.short_form IN ['FBbt_00000001', 'FBbt_00003624'] SET n:Anatomy;

// relations the edge writers look up by short_form (kb_owl_edge_writer.check_properties)
UNWIND [
  ['RO_0002292', 'expresses'],
  ['RO_0002204', 'gene_product_of'],
  ['GENO_0000408', 'is_allele_of'],
  ['BFO_0000051', 'has_part'],
  ['BFO_0000050', 'part_of'],
  ['RO_0002131', 'overlaps'],
  ['RO_0002488', 'existence_starts_during'],
  ['RO_0002490', 'existence_overlaps'],
  ['RO_0002492', 'existence_ends_during'],
  ['VFBext_0000008', 'has_hemidriver']
] AS p
MERGE (n:Property:Entity { iri: 'http://purl.obolibrary.org/obo/' + p[0] }) SET n.short_form = p[0], n.label = p[1];

MERGE (s:Individual:Site:Entity { iri: 'http://flybase.org' }) SET s.short_form = 'FlyBase', s.label = 'FlyBase';
