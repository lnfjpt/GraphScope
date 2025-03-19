LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (forum:FORUM {id: row[1]})-[:HASMEMBER]->(person:PERSON {id: row[2]})