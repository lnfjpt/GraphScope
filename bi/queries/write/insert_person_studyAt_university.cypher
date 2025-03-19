LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {id: row[1]})-[:STUDYAT {studyFrom: row[3]}]->(university:ORGANISATION {id: row[2]})