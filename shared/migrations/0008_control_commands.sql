create table if not exists control_commands (
    command_id text primary key,
    command_type text not null,
    status text not null,
    operator text,
    reason text,
    payload jsonb not null,
    created_at_ms bigint not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_control_commands_command_type on control_commands(command_type);
create index if not exists idx_control_commands_status on control_commands(status);
create index if not exists idx_control_commands_created_at_ms on control_commands(created_at_ms desc);
