LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:POST {id: row[1]})-[is_located_in:ISLOCATEDIN]->(:PLACE {id: row[2]})
DELETE is_located_in