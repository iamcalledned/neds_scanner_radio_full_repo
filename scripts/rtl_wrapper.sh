# /home/ned/scripts
#!/bin/bash
# %i comes in as the port (12000, 12001, etc)
PORT=$1
# Calculate device index: 12000 -> 0, 12001 -> 1, etc.
INDEX=$((PORT - 12000))

echo "Starting RTL_TCP on Port $PORT using Device Index $INDEX"
exec /usr/local/bin/rtl_tcp -d $INDEX -a 127.0.0.1 -p $PORT
