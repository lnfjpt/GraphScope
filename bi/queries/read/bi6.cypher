MATCH
  (tag:TAG {name: $tag})<-[:HASTAG]-(message1:COMMENT|POST)<-[:LIKES]-(person2:PERSON),
  (message1:COMMENT|POST)-[:HASCREATOR]->(person1:PERSON)

WITH DISTINCT person1, person2
RETURN
  person1.id AS id,
  sum(person2.popularityScore) AS score
  ORDER BY score DESC, id ASC
  LIMIT 100