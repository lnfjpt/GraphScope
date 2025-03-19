LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (post:POST {id: row[1]})
OPTIONAL MATCH (post)<-[:REPLYOF*0..20]-(message:COMMENT)
DETACH DELETE post, message