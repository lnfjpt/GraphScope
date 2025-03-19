LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (comment:COMMENT {id: row[1]})-[:REPLYOF]->(post:POST {id: row[2]})