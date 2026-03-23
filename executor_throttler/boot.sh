#!/bin/bash

cd "/root/executor_throttler"
/usr/local/bin/pm2 delete Exec_Throttler
/usr/local/bin/pm2 start 'main.py' --namespace='Exec_Throttler' --interpreter='/root/executor_throttler/venv/bin/python3' --name 'Exec_Throttler' --time