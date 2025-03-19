MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
  WHERE zombie.creationDate < $endDate
WITH country, zombie
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)
WHERE message.creationDate < $endDate
WITH
  country,
  zombie,
  gs.function.datetime($endDate) AS idate,
  zombie.creationDate AS zdate,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (idate.year  - zdate.year )
  + (idate.month - zdate.month)
  + 1 AS months,
  messageCount
  WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)<-[:LIKES]-(likerZombie:PERSON)
WHERE likerZombie IN zombies
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)<-[:LIKES]-(likerPerson:PERSON)
WHERE likerPerson.creationDate < $endDate
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount
RETURN
  zombie.id AS zid,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / totalLikeCount
    END AS zombieScore
  ORDER BY
  zombieScore DESC,
  zid ASC
  LIMIT 100