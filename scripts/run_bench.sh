#!/bin/bash
tmux \
    new-session  "cargo run $1 --bin bobd; read -p 'Press enter to continue'" \; \
    split-window "sleep 5; cargo run $1 --bin bobp; read -p 'Press enter to continue'"
