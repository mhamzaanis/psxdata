-- Run once in your Supabase SQL editor.
-- This schema matches fetch.py exactly.

-- 1) Market-level daily summary
create table if not exists public.market_daily_summary (
    id            bigint generated always as identity primary key,
    trade_date    date not null unique,
    kse100_prev   numeric,
    kse100_close  numeric,
    kse100_change numeric,
    kse30_prev    numeric,
    kse30_close   numeric,
    kse30_change  numeric,
    prev_volume   bigint,
    curr_volume   bigint,
    advances      int,
    declines      int,
    unchanged     int,
    flu_no        text,
    created_at    timestamptz not null default now()
);

-- 2) Ticker-level rows
create table if not exists public.datatable (
    id          bigint generated always as identity primary key,
    trade_date  date not null,
    symbol      text not null,
    company     text not null,
    open        numeric,
    high        numeric,
    low         numeric,
    close       numeric,
    turnover    bigint,
    change      numeric,
    section     text,
    created_at  timestamptz not null default now(),

    constraint datatable_symbol_date_uq unique (symbol, trade_date),
    constraint datatable_date_fk
        foreign key (trade_date)
        references public.market_daily_summary (trade_date)
        on delete cascade
);

-- 3) Indexes
create index if not exists idx_market_summary_trade_date
    on public.market_daily_summary (trade_date desc);

create index if not exists idx_datatable_trade_date
    on public.datatable (trade_date desc);

create index if not exists idx_datatable_symbol
    on public.datatable (symbol);

create index if not exists idx_datatable_symbol_date
    on public.datatable (symbol, trade_date desc);

create index if not exists idx_datatable_section
    on public.datatable (section);

comment on table public.market_daily_summary is
    'Daily market summary (KSE100, KSE30, volume, etc.)';

comment on table public.datatable is
    'Ticker-level closing rates';

-- Optional explicit RLS intent for service role automation.
alter table public.market_daily_summary enable row level security;
alter table public.datatable enable row level security;

drop policy if exists "service role full access summary" on public.market_daily_summary;
create policy "service role full access summary"
    on public.market_daily_summary for all
    using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

drop policy if exists "service role full access datatable" on public.datatable;
create policy "service role full access datatable"
    on public.datatable for all
    using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');
