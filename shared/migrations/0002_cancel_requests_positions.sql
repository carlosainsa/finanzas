create table if not exists cancel_requests (
    command_id text not null,
    order_id text not null,
    status text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (command_id, order_id)
);

create table if not exists positions (
    market_id text not null,
    asset_id text not null,
    position double precision not null default 0,
    updated_at timestamptz not null default now(),
    primary key (market_id, asset_id)
);

create index if not exists idx_orders_status on orders(status);
create index if not exists idx_orders_signal_id on orders(signal_id);
create index if not exists idx_cancel_requests_command_id on cancel_requests(command_id);
create index if not exists idx_cancel_requests_order_id on cancel_requests(order_id);
