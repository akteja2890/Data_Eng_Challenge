insert into flexion_prd.claims
select 
cast(claim_id	as STRING) as claim_id,				
cast(patient_id	as STRING	) as patient_id ,				
cast(provider_id	as STRING	) as provider_id,				
cast(visit_type	as STRING	) as visit_type,				
cast(total_cost	as NUMERIC	) as total_cost,				
coverage_type,				
parse_date("%m/%d/%Y",date_of_service) as date_of_service,				
claim_date,				
billed	,				
last_modified_date			
from flexion_stg.claims where 
visit_type in ('physical','routine','illness','surgery')
and coverage_type in (0,50,100)
and regexp_contains(total_cost, r'\d.')    
and regexp_contains(date_of_service, r'^\d{1,2}/\d{1,2}/\d{4}') 
and last_modified_date is not null