MATCH
  (post:POST)<-[:REPLYOF*1..20]-(comment:COMMENT)
CREATE (comment: COMMENT)-[:HASROOT]->(post: POST)