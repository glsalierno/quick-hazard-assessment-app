-- v7 knowledge evidence store (created automatically by KnowledgeStore)
CREATE TABLE IF NOT EXISTS evidence (
    cas TEXT NOT NULL,
    field TEXT NOT NULL,
    provider TEXT NOT NULL,
    value_json TEXT,
    source TEXT,
    confidence TEXT,
    retrieval_date TEXT,
    citation TEXT,
    PRIMARY KEY (cas, field, provider)
);
