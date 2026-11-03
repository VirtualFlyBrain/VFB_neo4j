// Minimal PDB fixture for CI (replaces the 13.7 GB production dump). Statements are separated by a line holding only ';'.
// add_refs_for_anat reads :Class nodes whose definition / *_synonym attributes hold JSON strings, and writes pub nodes
// plus edges to them. These three classes cover: a definition + exact synonym with a FlyBase ref, a definition with no
// annotations (-> FlyBase:Unattributed), and a related synonym with one supported (FlyBase) and one unsupported (PMID) ref.
// The annotation property add_refs_for_anat writes its edges with: the edge writer drops every triple whose property
// has no :Property node (match on short_form), which is why the first fixture wrote pub nodes but no edges.
// Values copied from the live PDB.
CREATE (:Entity:Property:AnnotationProperty {iri: 'http://purl.org/dc/terms/references', curie: 'dcterms:references',
  short_form: 'references', label_rdfs: ['has_reference'], sl: 'has_reference', qsl: 'has_reference', label: 'has_reference'})
;
CREATE (:Class:Entity:Anatomy {iri: 'http://purl.obolibrary.org/obo/FBbt_00005106', short_form: 'FBbt_00005106', label: ['neuron'],
  definition: ['{"value": "A cell that is specialised for conducting nerve impulses.", "annotations": {"database_cross_reference": ["FlyBase:FBrf0112030"]}}'],
  has_exact_synonym: ['{"value": "nerve cell", "annotations": {"database_cross_reference": ["FlyBase:FBrf0112030"], "has_synonym_type": "ci_fixture"}}']})
;
CREATE (:Class:Entity:Anatomy {iri: 'http://purl.obolibrary.org/obo/FBbt_00005801', short_form: 'FBbt_00005801', label: ['mushroom body'],
  definition: ['{"value": "Neuropil of the insect brain made up of Kenyon cell processes."}']})
;
CREATE (:Class:Entity:Anatomy {iri: 'http://purl.obolibrary.org/obo/FBbt_00003624', short_form: 'FBbt_00003624', label: ['adult brain'],
  has_related_synonym: ['{"value": "cerebrum", "annotations": {"database_cross_reference": ["FlyBase:FBrf0000001", "PMID:1234567"]}}']})
;
CREATE (:Class:Entity:Anatomy {iri: 'http://purl.obolibrary.org/obo/FBbt_00000001', short_form: 'FBbt_00000001', label: ['unannotated anatomy']})
;
