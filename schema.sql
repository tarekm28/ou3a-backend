create table if not exists users (
  user_id text primary key,
  created_at timestamptz default now()
);

create table if not exists trips (
  trip_id text primary key,
  user_id text references users(user_id),
  start_time timestamptz,
  end_time timestamptz,
  sample_count int,
  created_at timestamptz default now()
);

create table if not exists trip_raw (
  trip_id text primary key references trips(trip_id),
  payload jsonb not null
);

create table if not exists detections (
  detection_id bigserial primary key,
  trip_id text references trips(trip_id) on delete cascade,
  ts timestamptz not null,
  latitude double precision,
  longitude double precision,
  intensity double precision
);
create index if not exists idx_detections_ts on detections(ts);
create index if not exists idx_detections_geo on detections(latitude, longitude);

create table if not exists pothole_clusters (
  cluster_id text primary key,
  latitude double precision,
  longitude double precision,
  hits int,
  users int,
  last_ts timestamptz,
  avg_intensity double precision,
  exposure double precision,
  confidence double precision,
  priority double precision,
  updated_at timestamptz default now()
);
create index if not exists idx_clusters_priority on pothole_clusters(priority desc);
create index if not exists idx_clusters_confidence on pothole_clusters(confidence desc);
