MATCH (tag:TAG {name: $tag})
OPTIONAL MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
OPTIONAL MATCH (tag)<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)
WHERE $startDate < message.creationDate
AND message.creationDate < $endDate
WITH
  person,
  CASE WHEN interest IS NOT NULL THEN 1
    ELSE 0
    END AS tag1,
  CASE WHEN message IS NOT NULL THEN 1
    ELSE 0
    END AS tag2
WITH person,
     100 * count(tag1) + count(tag2) AS score
OPTIONAL MATCH (person)-[:KNOWS]-(friend:PERSON)
OPTIONAL MATCH (friend)-[interest:HASINTEREST]->(tag {name: $tag})
OPTIONAL MATCH (friend)<-[:HASCREATOR]-(message:POST|COMMENT)-[:HASTAG]->(tag {name: $tag})
WITH
  person,
  score,
  friend,
  CASE WHEN interest IS NOT NULL THEN 1
    ELSE 0
    END AS tag1,
  CASE WHEN message IS NOT NULL THEN 1
    ELSE 0
    END AS tag2
RETURN
  person.id as id,
  score AS score,
  100 * count(tag1) + count(tag2) AS friendsScore
  ORDER BY
  score + friendsScore DESC,
  id ASC
  LIMIT 100