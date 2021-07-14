WITH collect(DISTINCT {
     id: coalesce(n.iri,"XXX"),
     short_form: coalesce(n.short_form,"XXX"),
     facets_annotation: labels(n)
     }) AS doc
RETURN REDUCE(output = [], r IN doc | output + r) AS flat
