do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'cancel_requests_status_check'
          and conrelid = 'cancel_requests'::regclass
    ) then
        alter table cancel_requests
            add constraint cancel_requests_status_check
            check (status in ('SENT', 'CONFIRMED', 'DIVERGED', 'FAILED'));
    end if;
end $$;
