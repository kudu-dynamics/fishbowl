# --------------------------------------------------------------------------- #

FROM elixir:1.11.3 AS build

COPY config /app/config
COPY lib /app/lib
COPY mix.exs /app/
COPY mix.lock /app/

ENV MIX_ENV=prod

WORKDIR /app/

RUN \
  mix local.hex --force && \
  mix local.rebar --force && \
  mix deps.get && \
  mix release

# --------------------------------------------------------------------------- #

# DEV: Use slim instead of alpine as there are no prebuilts for pandas.
FROM elixir:1.11.3-slim AS RELEASE

COPY requirements.txt ./

RUN \
  apt update && \
  apt install --no-install-recommends -y \
    gcc g++ libsnappy-dev python3-dev python3-pip python3-setuptools \
    python3 && \
  python3 -m pip --no-cache-dir install --compile -r requirements.txt && \
  apt purge -y \
    gcc g++ libsnappy-dev python3-dev python3-pip python3-setuptools && \
  apt autoremove -y && \
  apt autoclean && \
  rm -rf /var/lib/apt/lists/* && \
  rm requirements.txt && \
  useradd --create-home app

WORKDIR /home/app

COPY --from=build /app/_build/prod/rel/fishbowl .
COPY entrypoint.sh parquet.py ./

RUN \
  chown -R app: ./ && \
  chmod +x entrypoint.sh

USER app

ENV MIX_ENV=prod

ENTRYPOINT ["./entrypoint.sh"]

# --------------------------------------------------------------------------- #