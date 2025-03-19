LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (post:POST {id: row[1]})-[:ISLOCATEDIN]->(country:PLACE {id: row[2]})