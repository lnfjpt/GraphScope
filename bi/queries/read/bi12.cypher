MATCH (person:PERSON)
OPTIONAL MATCH (person)<-[:HASCREATOR]-(message:COMMENT|POST)
WHERE message.length > $lengthThreshold AND message.creationDate > $startDate
MATCH (message)-[:REPLYOF * 0..30]->(post:POST)
WHERE post.language IN $languages

WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
  ORDER BY
  personCount DESC,
  messageCount DESC