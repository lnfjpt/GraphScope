LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:FORUM {id: row[1]})-[has_member:HASMEMBER]->(:PERSON {id: row[2]})
DELETE has_member