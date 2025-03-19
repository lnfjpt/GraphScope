LOAD CSV WITH HEADERS FROM $csv_path AS row FIELDTERMINATOR '|'
CREATE (person:PERSON {
  creationDate: row[0] ,
  id: row[1],
  firstName: row[2],
  lastName: row[3],
  gender: row[4],
  birthday: row[5],
  locationIP: row[6],
  browserUsed: row[7],
  language: row[8],
  email: row[9]
})