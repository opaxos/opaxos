#!/bin/bash 
# this file contains customizable variables
# that will be used in the other scripts
# to run the evaluations

SSH_KEY_LOC=""                                  # ssh key location
SSH_USERNAME="user"                             # ssh username
MACHINES=(                                      # list of machines to be accessed via ssh, thus they must be public addresses
    "ms1319.utah.cloudlab.us"
    "ms1343.utah.cloudlab.us"
    "ms1322.utah.cloudlab.us"
    "ms1309.utah.cloudlab.us"
    "ms1304.utah.cloudlab.us"
    "ms1340.utah.cloudlab.us"
    "ms1315.utah.cloudlab.us"
    "ms1306.utah.cloudlab.us"
)
                                                # list of host used for consensus instance (e.g internal network IP address or names)
HOSTS=(                                         # they can be the same as MACHINES[]
    "10.10.1.1"
    "10.10.1.2"
    "10.10.1.3"
    "10.10.1.4"
    "10.10.1.5"
    "10.10.1.6"
    "10.10.1.7"
    "10.10.1.8"
)
SERVER_MACHINES=(0 1 2 3 4)                     # id of machines used for the paxos or opaxos instances, id starts with 0
CLIENT_MACHINES=(5)                             # id of machines used for the client, id starts with 0
# CORE_ID="19,39,17,37,15,35,13,33"             # cpu cores reserved for the paxos or opaxos instance, cpu id starts with 0
CORE_ID="0,8,1,9,2,10,3,11"
INTERNAL_PORT=1735
PUBLIC_PORT=7080
GO_DOWNLOAD_FILE="go1.19.linux-amd64.tar.gz"
GO_DOWNLOAD_URL="https://go.dev/dl/${GO_DOWNLOAD_FILE}"
MAIN_BRANCH="main"

# WARNING! Remove this on release!
GITHUB_ACCESS_KEY=""

# ===== EMULATED NETWORK CONFIG =====
# note that, the injected latency = the target latency - the actual latency.
# e.g if we want the RTT to be 5ms, and the actual RTT is 0.3ms, then 
#     the injected latency should be 4.7ms.
# INJECTED_LATENCY_x_y means adding delay for IP traffic from x to y, where x 
#     and y are the machine id stored in MACHINES[]. The same applies to BW_x_y.
#     The injected delay and bw rate limit happen in the sender side.
INTERFACE_NAME="eno1d1"
# INJECTED_LATENCY_0_1="10.1ms"
# INJECTED_LATENCY_0_2="10.1ms"
# INJECTED_LATENCY_1_3="14.4ms"
# INJECTED_LATENCY_1_4="16.8ms"
# INJECTED_LATENCY_1_5="15.4ms"
BW_0_1=1000                                      # the bandwidth rate limit unit is mbps (not mib/s)
BW_1_0=1000
BW_0_2=1000
BW_2_0=1000
BW_0_3=1000
BW_3_0=1000
BW_0_4=1000
BW_4_0=1000
BW_0_5=1000
BW_5_0=1000

