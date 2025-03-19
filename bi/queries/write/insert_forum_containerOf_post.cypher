LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (forum:FORUM {id: row[1]})-[:CONTAINEROF]->(post:POST {id: row[2]})