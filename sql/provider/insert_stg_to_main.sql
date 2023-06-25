INSERT INTO flexion_prd.provider
SELECT
distinct
  provider_id,
  fname,
  lname,
  facility_name,
  address,
  city,
  state,
  cast(zipcode as string) as zipcode,
  specialty_code,
  start_date
FROM flexion_stg.provider
where regexp_contains(cast(zipcode as string), r'\d') 
and specialty_code in ("family_physician","orthopedist","heart_surgeon","physical_therapist")
