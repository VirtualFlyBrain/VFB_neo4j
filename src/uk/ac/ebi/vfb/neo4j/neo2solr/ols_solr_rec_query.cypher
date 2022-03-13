WITH n SKIP 0 LIMIT 2000 
OPTIONAL MATCH (n)-[s:has_reference {typ:'syn'}]->(:pub) 
WITH n, coalesce(collect(DISTINCT s.value[0]), []) + coalesce(n.synonyms, []) as syn
WITH collect(DISTINCT {
     id: coalesce(n.iri,n.short_form,"XXX"), 
     iri: [coalesce(n.iri,n.short_form,"XXX")], 
     short_form: coalesce(n.short_form,"XXX"), 
     shortform_autosuggest:[n.short_form, replace(n.short_form,'_',':'), replace(n.short_form,'_',' ')], 
     obo_id: replace(n.short_form,'_',':'), 
     obo_id_autosuggest:[n.short_form, replace(n.short_form,'_',':'), replace(n.short_form,'_',' ')],
     label: n.label, 
     label_autosuggest: [n.label, replace(n.label,'-',' '), replace(n.label,'_',' ')], 
     synonym: syn, 
     synonym_autosuggest: syn, 
     autosuggest: [n.label] + syn, 
     facets_annotation: labels(n),
     unique_facets: coalesce([] + n.uniqueFacets, [])
     }) AS doc
RETURN REDUCE(output = [], r IN doc | output + r) AS flat
