LOAD CSV FROM $csv_path AS row FIELDTERMINATOR '|'
MATCH (:COMMENT {id: row[1]})-[reply_of:REPLYOF]->(:COMMENT {id: row[2]})
DELETE reply_of