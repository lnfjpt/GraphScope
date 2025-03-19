LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (comment:COMMENT {id: row[1]})-[:HASTAG]->(tag:TAG {id: row[2]})