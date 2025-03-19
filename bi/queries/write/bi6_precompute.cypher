MATCH
  (person1:PERSON)<-[:HASCREATOR]-(message:COMMENT|POST)<-[:LIKES]-(person2:PERSON)
WITH person1, count(person2) AS popularityScore
SET person1.popularityScore = popularityScore