#!/bin/bash

# Start backend server
python3 backend/server.py &

# Start Telegram bot
python3 bot.py &

wait
