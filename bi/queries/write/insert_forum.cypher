LOAD CSV WITH HEADERS FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (forum:FORUM {
  creationDate: row[0],
  id: row[1],
  title: row[2]
})