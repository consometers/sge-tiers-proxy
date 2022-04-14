BEGIN;

CREATE TYPE subscription_status AS ENUM ('OK', 'FAILED');

CREATE TABLE subscriptions (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    user_id bare_jid REFERENCES users (bare_jid) NOT NULL,
    series_name TEXT NOT NULL,
    subscribed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    notified_at TIMESTAMP WITH TIME ZONE,

    usage_point_id usage_point_id REFERENCES usage_points (id) NOT NULL,
    consent_id INT NOT NULL,
    consent_begins_at TIMESTAMP WITH TIME ZONE NOT NULL,
    consent_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

    status subscription_status,
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

    CHECK (subscribed_at >= consent_begins_at),
    CHECK (subscribed_at < consent_expires_at),
    CHECK (notified_at >= consent_begins_at),
    CHECK (notified_at < consent_expires_at),

    CONSTRAINT unique_users_series UNIQUE (user_id, usage_point_id, series_name)
);

INSERT INTO migrations (version) VALUES (5);

COMMIT;
