#!/bin/bash
# rclone with the remote "garage:", configured from the Garage key file in
# $GARAGE_ENV (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_ENDPOINT).
set -eu
set -a
. "${GARAGE_ENV:?the Garage key file}"
set +a
export RCLONE_CONFIG_GARAGE_TYPE=s3 RCLONE_CONFIG_GARAGE_PROVIDER=Other \
  RCLONE_CONFIG_GARAGE_ENDPOINT="$S3_ENDPOINT" RCLONE_CONFIG_GARAGE_REGION="$AWS_REGION" \
  RCLONE_CONFIG_GARAGE_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" RCLONE_CONFIG_GARAGE_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  RCLONE_CONFIG_GARAGE_FORCE_PATH_STYLE=true RCLONE_CONFIG_GARAGE_NO_CHECK_BUCKET=true
# No config file: the users the backfill runs as may have no home.
export RCLONE_CONFIG=/dev/null
exec "${RCLONE:-rclone}" "$@"
