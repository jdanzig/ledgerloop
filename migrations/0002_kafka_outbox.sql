-- Outbox for the audit.events egress. A BIGSERIAL tail (id > last_published)
-- skips events: ids are assigned at insert but become visible in commit
-- order, so a lower id can surface after the publisher passed it. Instead the
-- trigger enqueues within the append transaction and the publisher drains
-- committed rows in id order — nothing can be skipped.
CREATE TABLE kafka_outbox (
    event_id BIGINT PRIMARY KEY
);

CREATE FUNCTION enqueue_outbox() RETURNS trigger AS $$
BEGIN
    INSERT INTO kafka_outbox (event_id) VALUES (NEW.id);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER events_outbox
    AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION enqueue_outbox();
