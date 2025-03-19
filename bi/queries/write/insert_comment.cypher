LOAD CSV WITH HEADERS FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (comment:COMMENT {
  creationDate: row[0],
  id: row[1],
  locationIP: row[2],
  browserUsed: row[3],
  content: row[4],
  length: row[5]
})