## Draft v2 TermInfo Cypher queries

### Reviewing TermInfo

#### Class

![image](https://user-images.githubusercontent.com/112839/46010830-e3ea4a80-c0bb-11e8-829f-cb44a67d1726.png)

This doesn't need to change much except that xrefs (linkouts) should be a separate section (links) to references).

Question: Should image browser show more (e.g. source)?

#### Individual:Anatomy

![image](https://user-images.githubusercontent.com/112839/46010900-1c8a2400-c0bc-11e8-873d-65fba4ba3e80.png)

Non logical bits should be split out from relationships:
	- has_license & has_source - move to downloads section
	- member of ???
	
As for Class, xrefs (linkouts) should be split out from References.  But we also need to make a link to data at source distinct from linkouts to any other resources that may have the same data.

#### DataSet

![image](https://user-images.githubusercontent.com/112839/46011046-8efb0400-c0bc-11e8-83d5-d0e0f3bfa1b0.png)

Odd to have 'has_license' as relationship.  

### Common elements

label, description/definition, comment, short_form
xrefs(links), references, Queries (Need flexibility in layout - or should these be in a separate, sync'd widget 

### Elements differing between types

pub_syn (Class only) vs synonyms (Individuals)
  - Synonyms for classes live on edge, linked to pubs.
  - all links to pubs for Individuals are currently via DataSet (although this is denormalized to direct link to individuals in current pdb)
  - Synonyms on individuals are currently lists - without the additional datastructures allowed for classes.

images:
   - Can (& should) use the same dataStructure on Classes, DataSets
   - The image dataStructure on anatomical individuals necessarily lacks the Anatomical individual node present on Classes & DataSets.  It's also easier - and probably more appropriate - to include imaging type on the individuals.  
   - Question: Should we allow for 1:many Anatomical individual: Image Individual?  We can cope with multiple registration templates by having multiple in_register_with_edges
   

### Type: Class

```cql
MATCH (a:Class { short_form: 'FBbt_00003632'}) WITH a
OPTIONAL MATCH (parent:Class)<-[r]-(a)
WITH collect({ object: parent.label, rel: r.label}) as rels, a
OPTIONAL MATCH (a)-[rp]->(pub:pub)
WITH collect ({ miniref: p.miniref, pmid: p.PMID, 
		FlyBase: p.FlyBase, DOI: p.DOI, ISBN: p.ISBN,
	       typ: rp.typ, synonym_scope: rp.scope, 
	       synonym: rp.synonym, cat: rp.cat }) 
	       as pub_syn, a, rels
OPTIONAL MATCH (s:Site)<-[dbx:hasDbXref]-(a)
WITH collect ({ link: s.link_base + s.accession, label: s.label}) as xrefs, pub_syn, rels, a
OPTIONAL MATCH (a)<-[:SUBCLASSOF|INSTANCEOF*]-
	(i:Individual)<-[:depicts]-(:Individual)-[irw:in_register_with]->(template:Individual)
WITH a, pub_syn, rels, i, irw, template, xrefs limit 5 
RETURN collect ({image_sf: i.short_form, image_label: i.label, 
		template: template.label, folder: irw.folder}) as images,
       		xrefs, pub_syn, rels, a.short_form, a.label, a.description, 
		a.comment, labels(a) as atyp, a.synonym
```

### Type: Anatomy:individual

```cql
MATCH (a:Anatomy:Individual { short_form : 'VFB_00030852' }) with a 
MATCH (a)-[:has_source]->(ds:DataSet)-[:has_license]->(l:License)
WITH COLLECT({ dataset_short_form: ds.short_form, 
		dataset_name :  ds.label, license_name: l.label}) as dataset_license, a 
OPTIONAL MATCH (a)-[:has_source]->(ds:DataSet)-[:has_reference]->(p:pub)
WITH COLLECT ({ miniref: p.miniref, pmid: p.PMID, FlyBase: p.FlyBase, 
		DOI: p.DOI, ISBN: p.ISBN }) as pub, dataset_license, a
OPTIONAL MATCH (s:Site:Individual)<-[dbx:hasDbXref]-(a)
WITH COLLECT ({ link: s.link_base + s.accession, label: s.label}) as xrefs, d
		ataset_license, pub, a
OPTIONAL MATCH (parent:Class)<-[r]-(a)
WITH collect({ object: parent.label, rel: r.label}) 
as rels, xrefs, dataset_license, pub, a
OPTIONAL MATCH (a:Individual)<-[:depicts]-
(c:Individual)-[irw:in_register_with]-(template:Individual)
WITH rels, xrefs, dataset_license, pub, a, template, irw, c
OPTIONAL MATCH (c)-[:is_specified_output_of]->(it:Class) 
WITH rels, xrefs, dataset_license, pub, a, template, irw, imaging_type. // Potentially dodgy assumption about 1:1 ?
RETURN COLLECT ({template: template.label, folder: irw.folder})
	as images, dataset_license, xrefs, pub, rels, a.label, a.definition, 
	a.synonyms, imaging_type.label
```       

### Type: DataSet

```cql
MATCH (a:DataSet { short_form : 'Ohyama2015' })-[:has_license]->(l:License)
WITH a,l OPTIONAL MATCH (pub:Individual)<-[:has_reference]-(a)
WITH collect ({ miniref: pub.miniref } ) as pub, a,l
OPTIONAL MATCH (s:Site)<-[:hasDbXref]-(a)
WITH collect ({ link: s.iri, label: s.label}) as xrefs, pub, a,l
OPTIONAL MATCH (a)<-[:has_source]-(i:Individual)
     <-[:depicts]-(:Individual)-[irw:in_register_with]->(template:Individual)
     WITH xrefs, pub, a,l, i, irw, template
RETURN collect ({image_sf: i.short_form, image_label: i.label, 
		template: template.label, folder: irw.folder}) as images,
		xrefs, pub, a.short_form, a.label, a.description, 
		l.label, l.license_name
```


### Standardising JSON structure of output

```cql
COLLECT ({ miniref: p.miniref, pmid: p.PMID, FlyBase: p.FlyBase, 
	DOI: p.DOI, ISBN: p.ISBN }) as pub
COLLECT ({ miniref: p.miniref, pmid: p.PMID, FlyBase: p.FlyBase,
	DOI: p.DOI, ISBN: p.ISBN, typ: rp.typ, synonym_scope: 
	rp.scope, synonym: rp.synonym, cat: rp.cat }) AS pub_syn
COLLECT ({ dataset_short_form: ds.short_form, dataset_name :  
	ds.label, license_name: l.label, l.}) as dataset_license
COLLECT ({ link: s.link_base + s.accession, label: s.label }) as xrefs
COLLECT ({ image_sf: i.short_form, image_label: i.label, 
	template: template.label, folder: irw.folder }) as images
        a.short_form as short_form, a.label as label, a.description as description, 
	a.definition, as definition, a.comment as comments, labels(a) as types,
       a.synonym, it.label, l.license_name
```


