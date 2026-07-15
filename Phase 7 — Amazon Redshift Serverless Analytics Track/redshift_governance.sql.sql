--Create the database users
CREATE USER data_analyst WITH PASSWORD 'AnalystPass123!';
CREATE USER data_engineer WITH PASSWORD 'EngineerPass123!';

--Create the roles
CREATE ROLE analyst_role;
CREATE ROLE engineer_role;

--Grant the roles to your users
GRANT ROLE analyst_role TO data_analyst;
GRANT ROLE engineer_role TO data_engineer;

--Grant read permissions on your existing table to both roles
GRANT SELECT ON public.dim_customer TO ROLE analyst_role;
GRANT SELECT ON public.dim_customer TO ROLE engineer_role;

--Create the Masking Policy
CREATE MASKING POLICY mask_email
WITH (email VARCHAR(150))
USING ('REDACTED_PII'::VARCHAR);

--Attach the Masking Policy to the email Column
ATTACH MASKING POLICY mask_email
ON dim_customer (email)
TO ROLE analyst_role
PRIORITY 10;

--switch the user session
SET SESSION AUTHORIZATION data_engineer;

SET SESSION AUTHORIZATION data_engineer;

