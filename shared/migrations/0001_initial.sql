create table if not exists trade_signals (
    signal_id text primary key,
    market_id text not null,
    asset_id text not null,
    payload jsonb not null,
    created_at timestamptz not null default now()
);

create table if not exists execution_reports (
    signal_id text not null,
    order_id text not null,
    status text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    primary key (signal_id, order_id)
);

create table if not exists orders (
    order_id text primary key,
    signal_id text not null,
    market_id text not null,
    asset_id text not null,
    status text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists trades (
    trade_id text primary key,
    order_id text not null,
    signal_id text not null,
    status text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists risk_snapshots (
    id bigserial primary key,
    payload jsonb not null,
    created_at timestamptz not null default now()
);
