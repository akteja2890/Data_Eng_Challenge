MERGE flexion_prd.patients  AS m
USING 
(
    select 
cast(patient_id	as STRING) as patient_id,				
cast(fname	as STRING	) as fname ,				
cast(lname	as STRING	) as lname,				
 date_of_birth,				
cast(address	as STRING	) as address,				
cast(city	as STRING	) as city,				
cast(state	as STRING	) as state,		
cast(zipcode	as STRING	) as zipcode,
 date_of_residence,
last_modified_date as last_modified_date,
from flexion_stg.patients 
--where regexp_contains(date_of_birth, r'^\d{1,2}-\d{1,2}-\d{4}') 
-- where regexp_contains(date_of_residence, r'^\d{1,2}-\d{1,2}-\d{4}') 


)
AS s
ON  m.patient_id = s.patient_id 
WHEN MATCHED THEN
  UPDATE SET
    m.fname = s.fname,
    m.lname = s.lname,
    m.date_of_birth = s.date_of_birth,
    m.address = s.address,
    m.city = s.city,
    m.state = s.state,
    m.zipcode = s.zipcode,
    m.date_of_residence = s.date_of_residence,
    m.last_modified_date = s.last_modified_date
WHEN NOT MATCHED THEN
  INSERT (
    patient_id,
    fname,
    lname,
    date_of_birth,
    address,
    city,
    state,
    zipcode,
    date_of_residence,
    last_modified_date
  )
  VALUES (
    s.patient_id,
    s.fname,
    s.lname,
    s.date_of_birth,
    s.address,
    s.city,
    s.state,
    s.zipcode,
    s.date_of_residence,
    s.last_modified_date
  )