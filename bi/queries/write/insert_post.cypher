LOAD CSV WITH HEADERS FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (post:POST {
  creationDate: row[0] ,
  id: row[1],
  imageFile: row[2],
  locationIP: row[3],
  browserUsed: row[4],
  language: row[5],
  content: row[6],
  length: row[7]
})