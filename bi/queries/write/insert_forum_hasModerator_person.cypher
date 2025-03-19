LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (forum:FORUM {id: row[1]})-[:HASMODERATOR]->(person:PERSON {id: row[2]})