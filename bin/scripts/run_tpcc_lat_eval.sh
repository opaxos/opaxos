#!/bin/bash 
# script to run latency overhead evaluation for the emulated TPCC workload
# the final results are a .csv file containing latency for each transaction type
# in TPCC workload.

source variables_cap.sh
crt_time=$(date +"%Y%m%d%H%M%S")
result_dir=results/${crt_time}_tpcc_lat
tracefile_loc=/usr/local/griya/bin/tpcc_tracefile.csv

EXPERIMENTS=(
    "paxos encrypt"
    "paxos"
    "opaxos shamir"
    "opaxos ssms"
)

# by default we run the client only in the first machine
CLIENT_MACHINE="${MACHINES[${CLIENT_MACHINES[0]}]}"

function prepare_result_dir {
    mkdir -p $result_dir
}

function kill_prev_instances {
    PRE_CMDS=(
        'sudo killall ./server'                     # kill the previous process, if any, using the process name 'server'
        "sudo fuser -k ${INTERNAL_PORT}/tcp"        # kill the previous process, if any, using the tcp port
        'sudo rm -rf /tmp/rpc_*.sock'               # remove previous socket file, if any
        'sudo rm -rf /tmp/paxi_*'                   # remove previous persistent storage
    )

    # killing other server process, if any
    echo "> killing previous instances ..."
    kill_prev_pids=""
    for c in "${PRE_CMDS[@]}"; do
        for nid in "${SERVER_MACHINES[@]}"; do
            n="${MACHINES[$nid]}"
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n "${c} > /dev/null 2>&1" &
            kill_prev_pids="$kill_prev_pids $!"
        done
    done
    wait $kill_prev_pids
}

# prepare the config file
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encrypt (for paxos)
function prepare_config {
    cfg_loc="${result_dir}/cfg_$1_$2.json"
    cp dist_latency.template $cfg_loc
    echo "> preparing the configuration file ..."
    echo "   loc: $cfg_loc"

    # insert the internal address
    temp=""; i=1
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${HOSTS[$nid]}"
        temp="$temp\"1.$i\": \"tcp://$HOST:$INTERNAL_PORT\", "
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_INTERNAL_ADDRESS%${temp}%g" $cfg_loc

    # insert the public address
    temp=""; i=1
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${HOSTS[$nid]}"
        temp="$temp\"1.$i\": \"tcp://$HOST:$PUBLIC_PORT\", "
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_PUBLIC_ADDRESS%${temp}%g" $cfg_loc

    # insert the roles, the proposer is the first machine
    temp=""; i=1
    for nid in "${SERVER_MACHINES[@]}"; do
        if [ $i -eq 1 ]; then
            temp="$temp\"1.$i\": \"proposer,acceptor,learner\", "
        else
            temp="$temp\"1.$i\": \"acceptor,learner\", "
        fi
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_ROLES%${temp}%g" $cfg_loc

    # insert the value size (this can be random, since it will be ignored)
    sed -i "" "s%X_SIZE%50%g" $cfg_loc

    # insert the number of request (this can be random, since it will be ignored)
    sed -i "" "s%X_NUM_REQUEST%5%g" $cfg_loc

    # insert the protocol info
    case $1 in
        paxos)
            temp="\"name\": \"paxos\"";;
        opaxos)
            case $2 in
                shamir)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"shamir\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
                ssms)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"ssms\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
            esac
            ;;
    esac
    sed -i "" "s%X_PROTOCOL%${temp}%g" $cfg_loc

    # insert duration time (this can be random, since it will be ignored)
    sed -i "" "s%X_DURATION%0%g" $cfg_loc

    # upload the config to all the machines (including client machine)
    echo "> upload the configuration file to all machines ..."
    upload_pids=()
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${MACHINES[$nid]}"
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json &
        upload_pids+=($!)
    done
    scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/config.json &
    upload_pids+=($!)
    for pid in "${upload_pids[@]}"; do
        wait $pid
        echo "      upload process $pid exit with status $?"
    done

    # move the config to the intended location (including client machine)
    echo "> move the configuration file to the execution path ..."
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST "sudo mv ~/config.json /usr/local/griya/bin/config.json"
    done
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE "sudo mv ~/config.json /usr/local/griya/bin/config.json"
}

# run paxos/opaxos instances in all the machines
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encryption flag (for paxos)
function run_instances {
    i=1
    PIDS=()
    ALG=$1

    # run the server (paxos/opaxos instance) in each machine
    echo "> runing instances in all machines ..."
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${MACHINES[$nid]}"
        ID=1.${i}

        if [ ! -z "$2" ] && [ "$2" == "encrypt" ]; then
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST \
                "cd /usr/local/griya/bin; \
                sudo ./server -id ${ID} -algorithm ${ALG} \
                -client_type tcp -client_action tpcc -log_level error -log_stdout \
                -encrypt -config config.json -req-tracefile ${tracefile_loc}" \
                &
        else
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST \
                "cd /usr/local/griya/bin; \
                sudo ./server -id ${ID} -algorithm ${ALG} \
                -client_type tcp -client_action block -log_level error -log_stdout \
                -config config.json -req-tracefile ${tracefile_loc}" \
                &
        fi
        
        # store the pid of the ssh background process so we can kill it later
        PIDS+=($!)

        i=$(($i+1))
    done

    # wait until all the instances in all the machines are ready
    sleep 1
}

# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encrypt for (paxos)
function gather_results {
    echo "> gathering the latency data ..."
    
    # gather the latency data (tpcc_latency.csv)
    target_loc="${result_dir}/latency_tpcc_$1_$2.csv"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE:/usr/local/griya/bin/tpcc_latency.csv $target_loc

}

# run a single client in the first client machine
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encryption flag (for paxos)
function run_client {
    ALG=$1
    
    echo "> running the client and record the latencies ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action tpcc -log_level info -config config.json -log_stdout \
        -req-tracefile ${tracefile_loc}"
}

# kill the instances in all the machines by killing the ssh background process
function kill_instances {
    for pid in "${PIDS[@]}"; do
        kill $pid
    done
    kill_pids=""
    for nid in "${SERVER_MACHINES[@]}"; do
        n="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "sudo killall ./server > /dev/null 2>&1; \
            cd /usr/local/griya/bin; sudo ./cleanup.sh > /dev/null 2>&1; \
            sudo rm -f encode_time latency client.log > /dev/null 2>&1" &
        kill_pids="$kill_pids $!"
    done
    wait $kill_pids
}


# begin the measurements
prepare_result_dir
for E in "${EXPERIMENTS[@]}"; do
    echo "=> Run a measurement (protocol: $E)"
    kill_prev_instances
    prepare_config $E
    run_instances $E
    run_client $E
    gather_results $E
    kill_instances
        
    echo ""
    sleep 1
done