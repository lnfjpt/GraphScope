MATCH (message)-[:HASROOT*0..2]->(post:POST)-[:HASCREATOR]->(person:PERSON)
  WHERE
    message.creationDate >= $startDate AND message.creationDate <= $endDate
    AND post.creationDate >= $startDate AND post.creationDate <= $endDate
  WITH
    person,
    count(distinct post) as threadCnt,
    count(message) as msgCnt
RETURN
  person.id as id,
  person.firstName,
  person.lastName,
  threadCnt,
  msgCnt
  ORDER BY
  msgCnt DESC,
  id ASC
  LIMIT 100
