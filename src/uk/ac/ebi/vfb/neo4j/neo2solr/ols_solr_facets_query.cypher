Match (n:Class) WITH n, CASE labels(n)[0] WHEN "Individual" THEN "vfb:individual:" ELSE "vfb:class:" END as type 
WITH collect(DISTINCT {
     id: coalesce(type + n.iri,"XXX"),
     facets_annotation: labels(n)
	}) AS doc
RETURN REDUCE(output = [], r IN doc | output + r) AS flat
