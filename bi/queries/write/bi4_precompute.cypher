MATCH (country:PLACE)<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(person:PERSON)<-[:HASMEMBER]-(forum:FORUM)
WITH
  forum,
  country,
  count(person) as personCount
WITH
  forum,
  max(personCount) as popularCount
SET forum.popularity = popularCount