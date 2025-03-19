MATCH
  (person1:PERSON)-[knows:KNOWS]-(person2:PERSON),
  (person1)-[study_at1:STUDYAT]->(u:ORGANISATION)<-[study_at2:STUDYAT]-(person2)
WITH
  knows,
  min(study_at1.classYear - study_at2.classYear + 1) AS weight
SET knows.bi20_precompute=weight