insert into flexion_prd.patients
select 
cast(patient_id	as STRING) as patient_id,				
cast(fname	as STRING	) as fname ,				
cast(lname	as STRING	) as lname,				
parse_date("%m-%d-%Y",date_of_birth) as date_of_birth,				
cast(address	as STRING	) as address,				
cast(city	as STRING	) as city,				
cast(state	as STRING	) as state,		
cast(zipcode	as STRING	) as zipcode,
parse_date("%m-%d-%Y",date_of_residence) as date_of_residence,
last_modified_date,
from flexion_stg.patients 
where regexp_contains(date_of_birth, r'^\d{1,2}-\d{1,2}-\d{4}') 
and regexp_contains(date_of_residence, r'^\d{1,2}-\d{1,2}-\d{4}') ;