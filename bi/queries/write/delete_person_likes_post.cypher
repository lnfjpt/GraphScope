LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:PERSON {id: row[1]})-[likes:LIKES]->(:POST {id: row[2]})
DELETE likes