BEGIN;

-- size defined in RFC 7622
CREATE DOMAIN bare_jid AS VARCHAR(2049);

CREATE TABLE users (
    bare_jid bare_jid NOT NULL PRIMARY KEY
);

INSERT INTO migrations (version) VALUES (1);

COMMIT;
