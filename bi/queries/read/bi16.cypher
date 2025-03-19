MATCH (person1:PERSON)<-[:HASCREATOR]-(msg)-[:HASTAG]->(tag:TAG)
OPTIONAL MATCH (person1)-[:KNOWS]-(person2:PERSON)<-[:HASCREATOR]-(msg)
  WHERE msg.creationDate = $date1 AND tag.name = $name1
  OR msg.creationDate = $date2 AND tag.name = $name2
WITH
  person1,
  person2,
  CASE WHEN msg.creationDate = $date1 AND tag.name = $name1 THEN 1
    ELSE 0
    END as msg1,
  CASE WHEN msg.creationDate = $date2 AND tag.name = $name2 THEN 1
    ELSE 0
    END as msg2
WITH
  person1,
  count(person2) as p2Cnt,
  count(DISTINCT msg1) as msg1Cnt,
  count(DISTINCT msg2) as msg2Cnt
  WHERE p2Cnt <= $max_limit AND msg1Cnt > 0 AND msg2Cnt > 0
RETURN
  person1,
  msg1Cnt,
  msg2Cnt
  ORDER BY
  msg1Cnt + msg2Cnt DESC,
  person1.id ASC
  LIMIT 20;