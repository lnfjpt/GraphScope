LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (post:POST {id: row[1]})-[:HASTAG]->(tag:TAG {id: row[2]})