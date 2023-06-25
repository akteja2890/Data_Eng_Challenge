
CREATE schema flexion_prd;
CREATE schema flexion_stg;

CREATE TABLE flexion_prd.patients
(
  patient_id STRING NOT NULL,
  fname STRING NOT NULL,
  lname STRING,
  date_of_birth DATE NOT NULL,
  address STRING NOT NULL,
  city STRING NOT NULL,
  state STRING NOT NULL,
  zipcode STRING NOT NULL,
  date_of_residence DATE NOT NULL,
  last_modified_date DATE NOT NULL
);

CREATE TABLE flexion_prd.provider
(
  provider_id STRING NOT NULL,
  fname STRING NOT NULL,
  lname STRING NOT NULL,
  facility_name STRING NOT NULL,
  address STRING NOT NULL,
  city STRING NOT NULL,
  state STRING NOT NULL,
  zipcode STRING NOT NULL,
  specialty_code STRING NOT NULL,
  start_date DATE NOT NULL
);




CREATE TABLE flexion_prd.claims
(
  claim_id STRING NOT NULL,
  patient_id STRING NOT NULL,
  provider_id STRING,
  visit_type STRING NOT NULL,
  total_cost NUMERIC NOT NULL,
  coverage_type INT64 NOT NULL,
  date_of_service DATE NOT NULL,
  claim_date DATE NOT NULL,
  billed STRING NOT NULL,
  last_modified_date DATE NOT NULL
);