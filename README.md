# Fishbowl

Toolkit for processing S3 + Presto ETL pipelines.

## Running

Build a release.

```
mix deps.get
mix release
```

```
./_build/dev/rel/fishbowl/bin/fishbowl eval 'Etl.Main.main(["-t", "pharos_fn2hash", "1", "2020-05-12"])'

./entrypoint.sh -t pharos_fn2hash 1 2020-05-12
```

The build process is the same for running with Docker.

```
docker build fishbowl:latest .
```

```
docker run \
  --env AWS_ACCESS_KEY_ID=minio \
  --env AWS_SECRET_ACCESS_KEY=minio123 \
  --env AWS_ENDPOINT_URL=http://localhost:9000 \
  --network=host \
  --rm \
  -it \
  fishbowl:latest \
  -t pharos_fn2hash 1 2020-05-12
```

Distribution Statement "A" (Approved for Public Release, Distribution
Unlimited).
