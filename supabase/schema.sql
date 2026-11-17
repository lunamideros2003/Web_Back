-- =============================================================================
--  Election preconteo: durable audit layer (Supabase / PostgreSQL)
--  Run this file once in the Supabase SQL editor.
--
--  Redis is the source of truth for the live count. This schema is the
--  write-behind audit trail plus the snapshot fallback the public page reads
--  when Redis is unreachable. The backend connects with the service role key,
--  which bypasses Row Level Security.
-- =============================================================================

create extension if not exists pgcrypto;

-- -----------------------------------------------------------------------------
--  Every acta version ever accepted, corrections included.
--  The pair (acta_id, version) is the natural key, so the backend can retry a
--  batch safely with "on conflict do nothing".
-- -----------------------------------------------------------------------------
create table if not exists public.actas (
  acta_id          text        not null,
  version          integer     not null check (version > 0),
  mesa             integer     not null check (mesa > 0),
  puesto           integer     not null check (puesto > 0),
  municipio        integer     not null check (municipio > 0),
  juror_id         text,
  votes            jsonb       not null,
  sufragantes      integer     not null check (sufragantes >= 0),
  total_votes      integer     not null check (total_votes >= 0),
  is_doubtful      boolean     not null default false,
  photo_hash       text,
  idempotency_key  text,
  instance_id      text,
  submitted_at     timestamptz not null default now(),
  constraint actas_pkey primary key (acta_id, version)
);

comment on table public.actas is
  'Immutable audit trail of every acta version processed by the realtime backend.';

create index if not exists actas_municipio_idx on public.actas (municipio);
create index if not exists actas_puesto_idx    on public.actas (puesto);
create index if not exists actas_submitted_idx on public.actas (submitted_at desc);
create index if not exists actas_doubtful_idx  on public.actas (is_doubtful)
  where is_doubtful;
create index if not exists actas_idem_idx      on public.actas (idempotency_key);

-- -----------------------------------------------------------------------------
--  One aggregated snapshot per public topic (1 + 11 municipalities by default).
--  Refreshed every SUPABASE_SNAPSHOT_INTERVAL_MS by the instance that owns the
--  publishing window.
-- -----------------------------------------------------------------------------
create table if not exists public.tally_snapshots (
  topic       text        primary key,
  payload     jsonb       not null,
  seq         text,
  updated_at  timestamptz not null default now()
);

comment on table public.tally_snapshots is
  'Last consistent public state per topic. Served by the backend when Redis is down.';

-- -----------------------------------------------------------------------------
--  Optional human-readable audit view: how many versions each acta needed and
--  whether it was flagged for verification.
-- -----------------------------------------------------------------------------
create or replace view public.acta_audit as
select
  acta_id,
  max(version)                                   as latest_version,
  count(*)                                       as versions_received,
  max(mesa)                                      as mesa,
  max(puesto)                                    as puesto,
  max(municipio)                                 as municipio,
  bool_or(is_doubtful)                           as ever_doubtful,
  max(submitted_at)                              as last_received_at
from public.actas
group by acta_id;

-- -----------------------------------------------------------------------------
--  Row Level Security
--    * The backend uses the service role key and ignores RLS entirely.
--    * Anonymous clients may only read the aggregated snapshots, which is what
--      the public page needs for its degraded-mode fallback. Raw actas are
--      never exposed to anon.
-- -----------------------------------------------------------------------------
alter table public.actas           enable row level security;
alter table public.tally_snapshots enable row level security;

drop policy if exists "public read snapshots" on public.tally_snapshots;
create policy "public read snapshots"
  on public.tally_snapshots
  for select
  to anon, authenticated
  using (true);

-- No policies are declared for public.actas on purpose: without a policy every
-- anon/authenticated access is denied, while the service role still works.

-- -----------------------------------------------------------------------------
--  Retention helper (optional). Call it from a Supabase Cron job if you want
--  the raw audit trail to shrink after the election:
--    select public.prune_actas(interval '30 days');
-- -----------------------------------------------------------------------------
create or replace function public.prune_actas(retention interval)
returns integer
language plpgsql
security definer
as $$
declare
  removed integer;
begin
  delete from public.actas where submitted_at < now() - retention;
  get diagnostics removed = row_count;
  return removed;
end;
$$;
