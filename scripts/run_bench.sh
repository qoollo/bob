#!/bin/bash
tmux \
    new-session  "cargo run $1 --bin bobd  -- -c cluster.yaml -n  node.yaml; read -p 'Press enter to continue'" \; \
    split-window "sleep 5; cargo run $1 --bin bobp -- -c 100000; read -p 'Press enter to continue'"
