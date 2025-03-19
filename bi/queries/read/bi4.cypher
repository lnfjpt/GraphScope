MATCH (topForum:FORUM)
  WHERE topForum.creationDate>$date
WITH topForum
  ORDER BY
  topForum.popularity DESC
  LIMIT 100

WITH collect(topForum) AS topForums
UNWIND topForums AS topForum1
MATCH (topForum1:FORUM)-[:HASMEMBER]->(person:PERSON)
OPTIONAL MATCH (person:PERSON)<-[:HASCREATOR]-(message)-[:REPLYOF*0..20]->(post:POST)<-[:CONTAINEROF]-(topForum2:FORUM)
WHERE topForum2 IN topForums

RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  count(message) AS messageCount
  ORDER BY
  messageCount DESC,
  personId ASC
  LIMIT 100