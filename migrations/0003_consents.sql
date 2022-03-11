BEGIN;

-- Must be provided to use some SGE Tiers APIs…
CREATE TYPE consent_issuer_type AS ENUM ('female', 'male', 'company');

CREATE TABLE consents (
    id SERIAL NOT NULL PRIMARY KEY,
    issuer_name TEXT NOT NULL,
    issuer_type consent_issuer_type NOT NULL,
    begins_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (begins_at < expires_at),
    -- Compound key to be used to ensure webservice calls validity
    UNIQUE (id, begins_at, expires_at)
);

CREATE TABLE consents_usage_points (
    consent_id INT REFERENCES consents (id) NOT NULL,
    usage_point_id usage_point_id REFERENCES usage_points (id) NOT NULL,
    comment TEXT,
    PRIMARY KEY (consent_id, usage_point_id)
);

CREATE TABLE consents_users (
    consent_id INT REFERENCES consents (id) NOT NULL,
    user_id bare_jid REFERENCES users (bare_jid) NOT NULL,
    PRIMARY KEY (consent_id, user_id)
);

INSERT INTO migrations (version) VALUES (3);

COMMIT;
