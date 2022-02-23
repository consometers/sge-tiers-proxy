BEGIN;

CREATE DOMAIN usage_point_id AS CHAR(14);

CREATE TABLE usage_points (
    id usage_point_id NOT NULL PRIMARY KEY
);

INSERT INTO migrations (version) VALUES (2);

COMMIT;
