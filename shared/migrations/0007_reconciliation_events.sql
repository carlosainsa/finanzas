create table if not exists reconciliation_events (
    event_id text primary key,
    order_id text,
    signal_id text,
    event_type text not null,
    severity text not null,
    details jsonb not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_reconciliation_events_order_id on reconciliation_events(order_id);
create index if not exists idx_reconciliation_events_event_type on reconciliation_events(event_type);
create index if not exists idx_reconciliation_events_severity on reconciliation_events(severity);
create index if not exists idx_reconciliation_events_created_at on reconciliation_events(created_at desc);
