pkill -f master-program
cd ~/Projects/master-program
nohup env MASTER_PROGRAM_HOST=0.0.0.0 MASTER_PROGRAM_NODE_ID=m5 MASTER_PROGRAM_PORT=17321 cargo run > /tmp/master-program-m5.log 2>&1 &
