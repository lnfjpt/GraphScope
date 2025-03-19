MATCH (tag:TAG {name: $tag})<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)
OPTIONAL MATCH (message)<-[likes:LIKES]-(:PERSON)
WITH
  message AS message,
  person AS person,
  count(likes) AS likeCount
OPTIONAL MATCH (message)<-[:REPLYOF]-(reply:COMMENT)
WITH
  person,
  likeCount,
  count(reply) AS replyCount,
  count(distinct message) AS messageCount
RETURN
  person.id AS id,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
  ORDER BY
  score DESC,
  id ASC
  LIMIT 100