LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (comment1:COMMENT {id: row[1]})-[:REPLYOF]->(comment2:COMMENT {id: row[2]})