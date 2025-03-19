LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person1:PERSON {id: row[1]})-[:KNOWS {creationDate: row[0] }]->(person2:PERSON {id: row[2]})