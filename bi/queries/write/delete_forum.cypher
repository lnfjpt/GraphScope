LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (forum:FORUM {id: row[1]})
OPTIONAL MATCH (forum)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..20]-(message:COMMENT|POST)
DETACH DELETE forum, message