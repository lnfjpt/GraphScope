LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (:COMMENT {id: row[1]})-[:HASCREATOR]->(:PERSON {id: row[2]})