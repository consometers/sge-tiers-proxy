BEGIN;

CREATE TYPE webservices_calls_subscription_type AS ENUM (
    'CONSUMPTION_IDX',
    'CONSUMPTION_CDC_RAW',
    'CONSUMPTION_CDC_CORRECTED',
    'CONSUMPTION_CDC_ENABLE',
    'PRODUCTION_IDX',
    'PRODUCTION_CDC_RAW',
    'PRODUCTION_CDC_CORRECTED',
    'PRODUCTION_CDC_ENABLE'
);

-- Force non null webservice name (it was not intended to be possible)

UPDATE
    webservices_calls
SET
    webservice = 'ConsultationMesuresDetaillees-v2.0'
WHERE webservice IS NULL;

ALTER TABLE
    webservices_calls
ALTER COLUMN
    webservice
        SET NOT NULL;

-- Compound key to be used to ensure subscription cannot expire after consent

ALTER TABLE webservices_calls
    ADD UNIQUE (id, consent_expires_at);

-- TODO(cyril) activation de courbe de charge dans le consentement pour le faire automatiquement
-- TODO(cyril) check segment for periodicity
-- TODO(cyril) usage point infos (segment, niveauOuvertureServices, updated_at)

-- Argument and response of an SGE subscription call

CREATE TABLE webservices_calls_subscriptions (
    id BIGSERIAL NOT NULL PRIMARY KEY,

    webservices_call_id BIGINT REFERENCES webservices_calls (id) NOT NULL,
    consent_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Call parameters, need to be retreived to renew subscription

    -- pointId: can be retreived from webservices_calls
    -- contratId: can be retreived from config

    -- mesureTypeCode
    -- soutirage
    -- injection
    -- mesuresCorrigees
    -- transmissionRecurrente
    -- periodiciteTransmission
    call_type webservices_calls_subscription_type NOT NULL,
    -- mesuresPas: can be retreived from usage point type
    -- accord: can be retreived from consent

    -- Needed to renew

    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Call return, used to unsubscribe
    -- an entry here means a valid subscription, null not allowed
    call_id BIGSERIAL NOT NULL,

    -- consent is the one used to allow the call
    -- prevent its change to break expiration date

    FOREIGN KEY (webservices_call_id, consent_expires_at)
        REFERENCES webservices_calls (id, consent_expires_at)
        ON UPDATE CASCADE,

    -- expiration date cannot exceed consent
    CHECK (expires_at <= consent_expires_at)
);

-- Make subscriptions dependants on those calls
-- a call that is linked to no subscription should be removed
-- a subscription that linked to no call should be fixed
-- a subscription that is linked to a failed call should be fixed

CREATE TABLE subscriptions_calls (
    subscription_id INT REFERENCES subscriptions (id) NOT NULL,
    webservices_calls_subscriptions_id INT REFERENCES webservices_calls_subscriptions (id) NOT NULL,
    PRIMARY KEY (subscription_id, webservices_calls_subscriptions_id)
);

-- Add segment info to usage point, needed to guess some parameters of the
-- subscription webservice

CREATE TYPE usage_point_segment_type AS ENUM (
    'C1',
    'C2',
    'C3',
    'C4',
    'C5',
    'P1',
    'P2',
    'P3',
    'P4'
);

ALTER TABLE usage_points 
ADD COLUMN segment usage_point_segment_type,
ADD COLUMN service_level int;

-- Change consent_issuer_type, gender field does not seem to be mandatory
-- TODO did not manage to remove value female

ALTER TYPE consent_issuer_type RENAME VALUE 'male' TO 'INDIVIDUAL';
ALTER TYPE consent_issuer_type RENAME VALUE 'company' TO 'COMPANY';

UPDATE consents
SET issuer_type = 'INDIVIDUAL'
WHERE issuer_type = 'female';

INSERT INTO migrations (version) VALUES (6);

COMMIT;