#!/bin/bash 
# script to run the capacity overhead evaluation
# the final results are a .csv file containing the avg. latencies and resp. rate with
# increasing load
# usage: ./run_cap_bigval_eval.sh <value_size_in_bytes>
#   example: ./run_cap_bigval_eval.sh 1000

source variables.sh
crt_time=$(date +"%Y%m%d%H%M%S")

EXPERIMENTS=(
    "paxos encrypt"
    "paxos"
    "opaxos shamir"
    "opaxos ssms"
)
LOADS=()
NUM_CLIENTS=50
DURATION=20
VAL_SIZE_BYTE=$1
NUM_REPETITION=1
CLIENT_MACHINE="${MACHINES[${CLIENT_MACHINES[0]}]}"

function gen_loads {
    load_start=1000
    load_end=100000
    load_inc=2000

    LOADS+=( 100 )
    while [ $load_start -le $load_end ]; do
        LOADS+=($load_start)
        load_start=$(($load_start+$load_inc))
    done
}

function kill_prev_instances {
    PRE_CMDS=(
        'sudo killall ./server'                     # kill the previous process, if any, using the process name 'server'
        "sudo fuser -k ${INTERNAL_PORT}/tcp"        # kill the previous process, if any, using the tcp port
        'sudo killall ./client'                     # kill the previous client process, if any, using the process name 'server'
        'sudo rm -rf /tmp/rpc_*.sock'               # remove previous socket file, if any
    )

    # killing other server process, if any
    echo "> killing previous instances ..."
    for c in "${PRE_CMDS[@]}"; do
        for n in "${MACHINES[@]}"; do
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n "${c} > /dev/null 2>&1" &
        done
        wait
    done
}

function prepare_result_dir {
    mkdir -p results/${crt_time}_bigval_capacity
    echo "runtime(s), algo, label, target_load(req/s), load(req/s), response_rate(req/s), load_diff, avg_lat(ms)" > results/${crt_time}_bigval_capacity/capacity.csv
}

# prepare the config file
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encrypt (for paxos)
# $3 the intended load target with all the clients
function prepare_config {
    cfg_loc="results/${crt_time}_bigval_capacity/cfg_$1_$2_$3.json"
    warmup_cfg_loc="results/${crt_time}_bigval_capacity/cfg_$1_$2_$3_warmup.json"
    cp dist_capacity.template $cfg_loc
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

    # insert the number of clients
    sed -i "" "s%X_NUM_CLIENTS%${NUM_CLIENTS}%g" $cfg_loc

    # insert the value size
    sed -i "" "s%X_VAL_SIZE%${VAL_SIZE_BYTE}%g" $cfg_loc
    
    # insert the intended load
    load_per_client=$(($3/$NUM_CLIENTS))
    sed -i "" "s%X_LOAD_PER_CLIENT%${load_per_client}%g" $cfg_loc

    # insert the duration time
    sed "s%X_DURATION%1%g" $cfg_loc > $warmup_cfg_loc
    sed -i "" "s%X_DURATION%${DURATION}%g" $cfg_loc

    # upload the config to all the machines, including the client machine
    echo "> upload the configuration file to all machines ..."
    upload_pids=()
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${MACHINES[$nid]}"
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json &
        upload_pids+=($!)
    done
    scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/config.json &
    upload_pids+=($!)
    scp -q -i $SSH_KEY_LOC $warmup_cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/warmup_config.json &
    upload_pids+=($!)
    for pid in "${upload_pids[@]}"; do
        wait $pid
        # echo "      upload process $pid exit with status $?"
    done

    # move the config to the intended location (including the client machine)
    echo "> move the configuration file to the execution path ..."
    for nid in "${SERVER_MACHINES[@]}"; do
        HOST="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST "sudo mv ~/config.json /usr/local/griya/bin/config.json"
    done
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE "sudo mv ~/config.json /usr/local/griya/bin/config.json"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE "sudo mv ~/warmup_config.json /usr/local/griya/bin/warmup_config.json"
}

# run paxos/opaxos instances in all the machines
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing scheme, or encrypt flag
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
                -client_type tcp -client_action pipeline -log_level error -log_stdout \
                -encrypt -config config.json" \
                &
        else
            ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@$HOST \
                "cd /usr/local/griya/bin; \
                sudo ./server -id ${ID} -algorithm ${ALG} \
                -client_type tcp -client_action pipeline -log_level error -log_stdout \
                -config config.json" \
                &
        fi
        
        # store the pid of the ssh background process so we can kill it later
        PIDS+=($!)

        i=$(($i+1))
    done

    # wait until all the instances in all the machines are ready
    sleep 1
}

# run multiple clients in the first machine
# $1 is the protocol name (paxos or opaxos)
function run_clients {
    ALG=$1

    echo "> warming up ..."
    ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action pipeline -log_level error -config warmup_config.json -log_stdout"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE \
        "cd /usr/local/griya/bin; \
        sudo ./cleanup.sh  > /dev/null 2>&1; sudo rm -f latency encode_time"
    
    echo "> runing the clients in the client machine ..."
    ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action pipeline -log_level info -config config.json"
}

# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos), or encrypt (for paxos)
# $3 the intended load target with all the clients
function gather_results {
    echo "> gathering the data ..."
    
    # gather the latency data
    target_loc="results/${crt_time}_bigval_capacity/latency_$1_$2_$3.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE:/usr/local/griya/bin/latency $target_loc

    # rename the output file to client.log
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE \
        'cd /usr/local/griya/bin; ls -t client.*.log | head -n 1 | sudo xargs -I{} mv {} client.log'

    # gather the cients' log
    target_loc="results/${crt_time}_bigval_capacity/clients_$1_$2_$3.log"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE:/usr/local/griya/bin/client.log $target_loc

    # parse the load, throughput, and avg latency from the log
    actual_load=$(grep 'request-rate' ${target_loc} | awk '{sum += $NF} END {print sum}')
    throughput=$(grep 'Throughput' ${target_loc} | awk '{print $NF}')
    load_diff_percent=$(echo "scale=4; ($actual_load-$throughput)/$actual_load*100" | bc )
    avg_lat=$(grep 'mean' ${target_loc} | awk '{print $NF}')
    intended_actual_load_diff=$(echo "$3-$actual_load" | bc )
    
    echo "   intended load  : $3 req/s"
    echo "   performed load : $actual_load req/s"
    echo "   response rate  : $throughput req/s"
    echo "   load diff      : $load_diff_percent %"
    echo "   avg latency    : $avg_lat ms"

    echo "$DURATION, $1, $1_$2, $3, $actual_load, $throughput, $load_diff_percent, $avg_lat" >> results/${crt_time}_bigval_capacity/capacity.csv
}

# kill the instances in all the machines by killing the ssh background process
function kill_instances {
    for pid in "${PIDS[@]}"; do
        kill $pid
    done
    kill_pids=()
    for nid in "${SERVER_MACHINES[@]}"; do
        n="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "sudo killall ./server > /dev/null 2>&1;" &
        kill_pids+=($!)
    done
    for pid in "${kill_pids[@]}"; do
        wait $pid
    done
}

function cleanup {
    for nid in "${SERVER_MACHINES[@]}"; do
        n="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "cd /usr/local/griya/bin; sudo ./cleanup.sh > /dev/null 2>&1; \
            sudo rm -f encode_time client.log" &
    done
    sleep 1
}

gen_loads
prepare_result_dir

# begin the measurements
rep_id=1
while [ $NUM_REPETITION -gt 0 ]; do
    for E in "${EXPERIMENTS[@]}"; do
        protocol=$(echo $E | awk '{print $1;}')
        ss_alg=$(echo $E | awk '{print $2;}')
        load_diff_percent=0

        # running all the instances
        kill_prev_instances
        prepare_config "$protocol" "$ss_alg" 0
        run_instances $E
        
        # vary load sent by the clients
        for LD in "${LOADS[@]}"; do
            echo "=> Run a measurement (repetition#: $rep_id, protocol: $E, load: $LD req/s)"
            prepare_config "$protocol" "$ss_alg" $LD
            sleep 3
            run_clients "$protocol"
            gather_results "$protocol" "$ss_alg" $LD
            cleanup

            echo ""

            # break if the previous load diff is greater 
            # than or equal to 5%, check in gather_results()
            if [[ "$(bc <<< "$load_diff_percent >= 5")" == "1" ]]; then
                break
            fi

            # break if the intended and the actual performed load
            # differs by 5000 req/s as the client fail to send more load.
            # check `intended_actual_load_diff` calculation in gather_results()
            if [[ "$(bc <<< "$intended_actual_load_diff >= 5000")" == "1" ]]; then
                break
            fi

        done

        kill_instances

    done

    NUM_REPETITION=$(($NUM_REPETITION-1))
    rep_id=$(($rep_id+1))
done
