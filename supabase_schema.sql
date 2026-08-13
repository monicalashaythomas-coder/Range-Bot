-- Optional — only needed if you set SUPABASE_URL and SUPABASE_KEY.
-- Run once in the Supabase SQL editor before first deploy.

CREATE TABLE IF NOT EXISTS range_symbol_stats (
    key            TEXT PRIMARY KEY,
    wins           INTEGER NOT NULL DEFAULT 0,
    losses         INTEGER NOT NULL DEFAULT 0,
    total_profit   DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

NOTIFY pgrst, 'reload schema';
