WITH n SKIP 0 LIMIT 2000 
OPTIONAL MATCH (n)-[s:has_reference {typ:'syn'}]->(:pub) 
WITH collect(DISTINCT {
     id: coalesce(n.iri,n.short_form,"XXX"), 
     iri: [coalesce(n.iri,n.short_form,"XXX")], 
     short_form: coalesce(n.short_form,"XXX"), 
     shortform_autosuggest:[n.short_form, replace(n.short_form,'_',':'), replace(n.short_form,'_',' ')], 
     obo_id: replace(n.short_form,'_',':'), 
     obo_id_autosuggest:[n.short_form, replace(n.short_form,'_',':'), replace(n.short_form,'_',' ')],
     label: n.label, 
     label_autosuggest: [n.label, replace(n.label,'-',' '), replace(n.label,'_',' ')], 
     synonym: coalesce(collect(DISTINCT s.value[0]), n.synonyms, []), 
     synonym_autosuggest: coalesce(collect(DISTINCT s.value[0]), n.synonyms, []), 
     autosuggest: [n.label] + coalesce(collect(DISTINCT s.value[0]), []) + coalesce(n.synonyms, []), 
     facets_annotation: labels(n),
     unique_facets: coalesce([] + n.uniqueFacets, [])
     }) AS doc
RETURN REDUCE(output = [], r IN doc | output + r) AS flat
