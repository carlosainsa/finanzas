alter table orders
    add column if not exists requested_size double precision,
    add column if not exists filled_size double precision not null default 0,
    add column if not exists remaining_size double precision;

alter table trades
    add column if not exists fill_price double precision,
    add column if not exists fill_size double precision;

create index if not exists idx_trades_order_id on trades(order_id);
