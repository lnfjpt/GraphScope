LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (comment:COMMENT {id: row[1]})
OPTIONAL MATCH (comment)<-[:REPLYOF*0..20]-(message:COMMENT)
DETACH DELETE message