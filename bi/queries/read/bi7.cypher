MATCH
  (tag:TAG {name: $tag})<-[:HASTAG]-(message:COMMENT|POST),
  (message:COMMENT|POST)<-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)
WHERE NOT (comment:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag})
RETURN
  relatedTag.name AS name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  name ASC
LIMIT 100
