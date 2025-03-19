LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:FORUM {id: row[1]})-[container_of:CONTAINEROF]->(:POST {id: row[2]})
DELETE container_of