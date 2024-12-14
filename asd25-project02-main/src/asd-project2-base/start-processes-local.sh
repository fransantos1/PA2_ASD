#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "Please indicate a number of processes of at least one."
  exit 0
fi

i=0
base_p2p_port=34000
base_server_port=35000

# Clean up old logs and ensure no stale processes remain
rm -rf logs/*
pkill -f 'asdProj2.jar'

sleep 2

read -p "------------- Press enter to start. After starting, press enter to kill all servers --------------------"

# Calculate the full membership list
full_membership=""
i=0
while [ $i -lt $processes ]; do
  if [ -z "$full_membership" ]; then
    full_membership="localhost:$(($base_p2p_port + $i))"
  else
    full_membership="$full_membership,localhost:$(($base_p2p_port + $i))"
  fi
  i=$(($i + 1))
done

# Launch each process with specific membership configurations.
i=0
while [ $i -lt $processes ]; do
  p2p_port=$(($base_p2p_port + $i))
  server_port=$(($base_server_port + $i))

  # Check if ports are free
  if lsof -i:$p2p_port || lsof -i:$server_port; then
    echo "Error: Port $p2p_port or $server_port already in use."
    exit 1
  fi

  if [ $i -eq 0 ]; then
    # The first node gets the full membership list
    membership="$full_membership"
  else
    # Other nodes exclude themselves from their membership list
    membership=""
    j=0
    while [ $j -lt $processes ]; do
      if [ $i -ne $j ]; then
        if [ -z "$membership" ]; then
          membership="localhost:$(($base_p2p_port + $j))"
        else
          membership="$membership,localhost:$(($base_p2p_port + $j))"
        fi
      fi
      j=$(($j + 1))
    done
  fi

  java -DlogFilename=logs/node$p2p_port -cp target/asdProj2.jar Main -conf config.properties address=localhost p2p_port=$p2p_port server_port=$server_port initial_membership=$membership 2>&1 | sed "s/^/[$p2p_port] /" &
  echo "Launched process on P2P port $p2p_port, server port $server_port, membership: $membership"

  sleep 1
  i=$(($i + 1))
done

sleep 2
read -p "------------- Press enter to kill servers. --------------------"

pkill -f 'asdProj2.jar'

echo "All processes done!"
