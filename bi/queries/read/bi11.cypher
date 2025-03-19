MATCH
  (s:PLACE {name: $country})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(a),
  (s:PLACE {name: $country})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(b),
  (s:PLACE {name: $country})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-(c),
  (a)-[k1:KNOWS]-(b),
  (a)-[k2:KNOWS]-(c),
  (b)-[k3:KNOWS]-(c)
  WHERE
  k1.creationDate >= $startDate and k1.creationDate <= $endDate
  and k2.creationDate >= $startDate and k2.creationDate <= $endDate
  and k3.creationDate >= $startDate and k3.creationDate <= $endDate
WITH distinct a, b, c
  WHERE a.id < b.id and b.id < c.id
Return count(a)