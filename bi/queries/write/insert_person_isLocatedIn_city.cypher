LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {id: row[1]})-[:ISLOCATEDIN]->(city:PLACE {id: row[2]})