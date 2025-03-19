LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:FORUM {id: row[1]})-[has_moderator:HASMODERATOR]->(:PERSON {id: row[2]})
DELETE has_moderator