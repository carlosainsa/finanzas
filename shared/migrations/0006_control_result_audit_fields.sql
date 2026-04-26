alter table control_results
    add column if not exists operator text,
    add column if not exists reason text,
    add column if not exists error text,
    add column if not exists command_created_at_ms bigint,
    add column if not exists completed_at_ms bigint;

create index if not exists idx_control_results_completed_at_ms on control_results(completed_at_ms desc);
