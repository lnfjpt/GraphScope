LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (comment:COMMENT {id: row[1]})-[:ISLOCATEDIN]->(country:PLACE {id: row[2]})