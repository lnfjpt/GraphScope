LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:POST {id: row[1]})-[has_creator:HASCREATOR]->(:PERSON {id: row[2]})
DELETE has_creator