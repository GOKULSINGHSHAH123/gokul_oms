#!/bin/bash

PM2=/usr/bin/pm2

PYTHON=/home/ubuntu/gokul_oms/venv/bin/python3

EXE_CLIENT=/home/ubuntu/gokul_oms/Executor_Client/exec_client.py
EXE_ERROR=/home/ubuntu/gokul_oms/Executor_Error/exec_error.py
EXE_PENDING=/home/ubuntu/gokul_oms/Executor_Pending/exec_pending.py
EXE_THROTTLER=/home/ubuntu/gokul_oms/executor_throttler/main.py
FIREWALL=/home/ubuntu/gokul_oms/pipeline/firewall.py
SYNC=/home/ubuntu/gokul_oms/pipeline/sync.py

$PM2 start $PYTHON --name "firewall"    -- $FIREWALL
$PM2 start $PYTHON --name "sync"        -- $SYNC
$PM2 start $PYTHON --name "exe_client"  --cwd /home/ubuntu/gokul_oms/Executor_Client -- $EXE_CLIENT group1 worker1
$PM2 start $PYTHON --name "exe_error"   --cwd /home/ubuntu/gokul_oms/Executor_Error  -- $EXE_ERROR group1 worker1
# $PM2 start $PYTHON --name "exe_pending" -- $EXE_PENDING
$PM2 start $PYTHON --name "throttler"   --cwd /home/ubuntu/gokul_oms/executor_throttler -- $EXE_THROTTLER


$PM2 save