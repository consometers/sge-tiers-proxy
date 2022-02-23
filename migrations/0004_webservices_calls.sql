BEGIN;

CREATE TYPE webservices_call_status AS ENUM ('OK', 'FAILED');

CREATE TABLE webservices_calls (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    webservice VARCHAR(100),
    usage_point_id usage_point_id REFERENCES usage_points (id) NOT NULL,
    user_id bare_jid REFERENCES users (bare_jid) NOT NULL,
    consent_id INT NOT NULL,
    consent_begins_at TIMESTAMP WITH TIME ZONE NOT NULL,
    consent_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    called_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- status is intended to be set to NULL before the actual executing the call,
    -- then updated afterward. This allows to check the call validity
    -- by the database before actually executing it.
    status webservices_call_status,
    error TEXT,

    -- consent must be linked to that user
    FOREIGN KEY (consent_id, user_id)
        REFERENCES consents_users (consent_id, user_id),

    -- consent must be linked to that usage point
    FOREIGN KEY (consent_id, usage_point_id)
        REFERENCES consents_usage_points (consent_id, usage_point_id),

    -- request must happen at a date within the consent period
    FOREIGN KEY (consent_id, consent_begins_at, consent_expires_at)
        REFERENCES consents (id, begins_at, expires_at)
        ON UPDATE CASCADE,

    CHECK (called_at >= consent_begins_at),
    CHECK (called_at < consent_expires_at)
);

INSERT INTO migrations (version) VALUES (4);

COMMIT;
