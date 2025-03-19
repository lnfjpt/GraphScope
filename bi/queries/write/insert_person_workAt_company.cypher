LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {id: row[1]})-[:WORKAT {workFrom: row[3]}]->(company:ORGANISATION {id: row[2]})