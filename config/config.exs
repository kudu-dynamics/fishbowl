import Config

# Customize the S3 library's HTTP settings
config :ex_aws, :hackney_opts,
  follow_redirect: true,
  recv_timeout: 60_000 * 60

# Customize the S3 library's retry behavior
config :ex_aws, :retries,
  max_attempts: 10,
  base_backoff_in_ms: 10,
  max_backoff_in_ms: 60_000 * 5

config :logger, :console, format: "$time $metadata[$level] $levelpad$message\n"

if config_env() == :test do
  config :logger,
    level: :info
end
