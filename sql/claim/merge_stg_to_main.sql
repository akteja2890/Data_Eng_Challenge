MERGE flexion_prd.claims  AS m
USING 
(
   select 
cast(claim_id	as STRING) as claim_id,				
cast(patient_id	as STRING	) as patient_id ,				
cast(provider_id	as STRING	) as provider_id,				
cast(visit_type	as STRING	) as visit_type,				
cast(total_cost	as NUMERIC	) as total_cost,				
coverage_type,				
 date_of_service,				
claim_date,				
cast( billed as string) as billed	,				
last_modified_date			
from flexion_stg.claims where 
visit_type in ('physical','routine','illness','surgery')
and coverage_type in (0,50,100)
and regexp_contains(cast(total_cost as string), r'\d.')    
--and regexp_contains(cast(date_of_service as string), r'^\d{1,2}/\d{1,2}/\d{4}') 
and last_modified_date is not null
)
AS s
ON  m.claim_id = s.claim_id 
WHEN MATCHED THEN
  UPDATE SET
    m.patient_id = s.patient_id,
    m.provider_id = s.provider_id,
    m.visit_type = s.visit_type,
    m.total_cost =s.total_cost ,
    m.coverage_type = s.coverage_type,
    m.date_of_service = s.date_of_service,
    m.claim_date = s.claim_date,
    m.billed = s.billed,
    m.last_modified_date = s.last_modified_date
WHEN NOT MATCHED THEN
  INSERT (
    claim_id,
    patient_id,
    provider_id,
    visit_type,
    total_cost,
    coverage_type,
    date_of_service,
    claim_date,
    billed,
    last_modified_date
  )
  VALUES (
    s.claim_id,
    s.patient_id,
    s.provider_id,
    s.visit_type,
    cast(s.total_cost	as NUMERIC	),
    s.coverage_type,
    s.date_of_service,
    s.claim_date,
    s.billed,
    s.last_modified_date
  )