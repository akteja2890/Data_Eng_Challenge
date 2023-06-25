MERGE flexion_prd.provider AS main
USING 
(SELECT
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
)
 AS stage
ON main.provider_id = stage.provider_id
WHEN MATCHED THEN
  UPDATE SET
    main.fname = stage.fname,
    main.lname = stage.lname,
    main.facility_name = stage.facility_name,
    main.address = stage.address,
    main.city = stage.city,
    main.state = stage.state,
    main.zipcode = stage.zipcode,
    main.specialty_code = stage.specialty_code,
    main.start_date = stage.start_date
WHEN NOT MATCHED THEN
  INSERT (
    provider_id,
    fname,
    lname,
    facility_name,
    address,
    city,
    state,
    zipcode,
    specialty_code,
    start_date
  )
  VALUES (
    stage.provider_id,
    stage.fname,
    stage.lname,
    stage.facility_name,
    stage.address,
    stage.city,
    stage.state,
    stage.zipcode,
    stage.specialty_code,
    stage.start_date
  )