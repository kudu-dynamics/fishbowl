#!/bin/sh

set -eu

# Entrypoint script for use in the Dockerfile.

# Set the REL_DIR variable to point to the release directory containing the
# bin folder that needs to be run.
case "${MIX_ENV:-dev}" in
  prod)
    REL_DIR=.
    ;;

  staging)
    REL_DIR=./_build/dev/rel/fishbowl
    ;;

  dev)
    REL_DIR=./_build/dev/rel/fishbowl

    # Rebuild every run.
    echo "y" | mix release
    ;;

  *)
    echo "specify valid MIX_ENV: prod|staging|dev"
    exit 1
    ;;
esac

args=$(
  echo "$@" | \
  sed 's/ /" "/g' | \
  sed -E 's/ /, /g' | \
  sed -E 's/^(.*)$/"\1"/'
)

"${REL_DIR}/bin/fishbowl" eval 'Etl.Main.main('["${args}"]')'
