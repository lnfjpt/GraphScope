MATCH (person1:PERSON)<-[:HASCREATOR]-(:COMMENT)-[:REPLYOF]->(:COMMENT|POST)-[:HASCREATOR]->(person2:PERSON)
WITH
  person1,
  person2,
  count(person2) as interaction
MATCH (person1)-[knows:KNOWS]-(person2)
SET knows.interaction2_count=interaction