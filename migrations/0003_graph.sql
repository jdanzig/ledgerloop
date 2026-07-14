-- Knowledge graph in relational clothing — deliberately. At this scale,
-- recursive CTEs beat operating a graph database; the ontology is the
-- interesting part, not the storage engine.
CREATE TABLE entities (
    id    UUID PRIMARY KEY,
    type  TEXT NOT NULL,      -- party | contract | obligation | spend_commitment
    attrs JSONB NOT NULL
);

CREATE TABLE edges (
    src   UUID NOT NULL REFERENCES entities(id),
    dst   UUID NOT NULL REFERENCES entities(id),
    type  TEXT NOT NULL,      -- party_to | obligates | supersedes
    attrs JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (src, dst, type)
);

CREATE INDEX entities_type_idx ON entities (type);
CREATE INDEX edges_dst_idx ON edges (dst, type);
