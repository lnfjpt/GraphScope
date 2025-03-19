LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {id: row[1]})-[:HASINTEREST]->(tag:TAG {id: row[2]})