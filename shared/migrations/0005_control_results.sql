create table if not exists control_results (
    command_id text primary key,
    command_type text not null,
    status text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_control_results_command_type on control_results(command_type);
create index if not exists idx_control_results_status on control_results(status);
create index if not exists idx_control_results_updated_at on control_results(updated_at desc);
