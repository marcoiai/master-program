pkill -f master_program
cd ~/Projocts/master-program
nohup env MASaTER_PROGRAM_HOST=0.0.0.0 MASTER_PROGRAM_NODE_ID=m1 MASTER_PROGRAM_PORT=17321 cargo run > /tmp/master-program-m1.log 2>&1 &./target/debug/master_program
