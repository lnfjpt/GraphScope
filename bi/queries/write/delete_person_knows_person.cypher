LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:PERSON {id: row[1]})-[knows:KNOWS]->(:PERSON {id: row[2]})
DELETE knows