-- Run once in your Supabase SQL editor.
-- This schema matches fetch.py exactly (table: datatable).

create table if not exists datatable (
    id         bigserial primary key,
    date       timestamptz not null,
    symbol     text not null,
    company    text,
    open       numeric(18,4),
    high       numeric(18,4),
    low        numeric(18,4),
    close      numeric(18,4),
    turnover   bigint,
    change     numeric(18,4),
    created_at timestamptz default now(),

    unique (symbol, date)
);

-- Useful query indexes.
create index if not exists idx_datatable_symbol_date
    on datatable (symbol, date desc);

create index if not exists idx_datatable_date
    on datatable (date desc);

-- Service role bypasses RLS in Supabase, but this keeps explicit policy intent.
alter table datatable enable row level security;

create policy "service role full access"
    on datatable for all
    using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');