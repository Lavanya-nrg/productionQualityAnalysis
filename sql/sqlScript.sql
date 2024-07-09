CREATE SCHEMA myschema AUTHORIZATION awsuser;

CREATE TABLE myschema.report1 (
    Production_Unit_Id VARCHAR(50),
    Defective_Percentage_Scissor FLOAT,
    Defective_Percentage_Paper FLOAT,
    Defective_Percentage_Rock FLOAT
);

CREATE TABLE myschema.report2 (
    Production_Unit_Id VARCHAR(50),
    overall_percentage_defective FLOAT
);

CREATE TABLE myschema.report3 (
    production_unit_id VARCHAR(10),
    percentage_items_discarded_double DECIMAL(18, 15)
);

SELECT * from myschema.report1 limit 10;

SELECT * from myschema.report2 limit 10;

SELECT * from myschema.report3 limit 10;