-- Run once in your Supabase SQL editor
-- Stores every equity/fund row from the PSX Closing Rate Summary PDF

create table if not exists psx_daily_prices (
    id            bigserial primary key,
    ticker        text          not null,
    company_name  text,
    sector        text,                        -- e.g. COMMERCIAL BANKS, CEMENT
    trade_date    date          not null,
    turnover      bigint,
    prev_rate     numeric(18,4),
    open_rate     numeric(18,4),
    high          numeric(18,4),
    low           numeric(18,4),
    close         numeric(18,4),
    change        numeric(18,4),
    created_at    timestamptz   default now(),

    unique (ticker, trade_date)
);

-- Fast lookups by ticker and date
create index if not exists idx_psx_ticker_date
    on psx_daily_prices (ticker, trade_date desc);

-- Fast lookups by sector
create index if not exists idx_psx_sector_date
    on psx_daily_prices (sector, trade_date desc);

-- Row Level Security (service role has full access)
alter table psx_daily_prices enable row level security;

create policy "service role full access"
    on psx_daily_prices for all
    using (auth.role() = 'service_role');