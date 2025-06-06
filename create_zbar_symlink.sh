#!/usr/bin/env bash
#
# If Heroku placed libzbar.so under /.apt, link it into /usr/lib so pyzbar can find it.

APT_LIB="/app/.apt/usr/lib/x86_64-linux-gnu"
TARGET_DIR="/usr/lib/x86_64-linux-gnu"

# Only proceed if APT_LIB/libzbar.so exists and /usr/lib/.../libzbar.so does not yet exist
if [ -d "$APT_LIB" ] && [ -f "$APT_LIB/libzbar.so" ] && [ ! -f "$TARGET_DIR/libzbar.so" ]; then
  ln -s "$APT_LIB/libzbar.so" "$TARGET_DIR/libzbar.so"
fi
