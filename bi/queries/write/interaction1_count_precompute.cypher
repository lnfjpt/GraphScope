MATCH (person1:PERSON)<-[:HASCREATOR]-(:COMMENT|POST)<-[:REPLYOF]-(:COMMENT)-[:HASCREATOR]->(person2:PERSON)
WITH
  person1,
  person2,
  count(person2) as interaction
MATCH (person1)-[knows:KNOWS]-(person2)
SET knows.interaction1_count=interaction