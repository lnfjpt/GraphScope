LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {id: row[1]})-[:LIKES]->(post:POST {id: row[2]})