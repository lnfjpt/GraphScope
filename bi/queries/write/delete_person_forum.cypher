LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (person:PERSON {id: row[1]})
OPTIONAL MATCH (person)<-[:HASMODERATOR]-(forum:FORUM)
  WHERE forum.title STARTS WITH 'Album '
  OR forum.title STARTS WITH 'Wall '
  OR forum IS NULL
OPTIONAL MATCH (forum)-[:CONTAINEROF]->(:POST)<-[:REPLYOF*0..30]-(message:COMMENT|POST)
DETACH DELETE person, forum, message