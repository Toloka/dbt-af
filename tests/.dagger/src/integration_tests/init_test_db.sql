CREATE SCHEMA IF NOT EXISTS "raw";

CREATE TABLE "postgres"."raw"."_external_table_to_use_in_source" (
    _field_1 text,
    _field_2 text
);

INSERT INTO "postgres"."raw"."_external_table_to_use_in_source" (_field_1, _field_2)
VALUES
    ('value11', 'value12'),
    ('value21', 'value22');
