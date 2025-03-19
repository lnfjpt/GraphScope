LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (post:POST {id: row[1]})-[:HASCREATOR]->(person:PERSON {id: row[2]})