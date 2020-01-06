# this is a quick and dirty migration of runemomniagg2.sh to the 
# --enable-demo mode of aggregate testing
function kill_netperfs {
    echo "Killing netperfs"
    pkill -ALRM netperf
    NETPERF_PROC=$(pgrep -P 1 -f netperf)

    if [ -n "$NETPERF_PROC" ]; 
    then
        kill $NETPERF_PROC; 
    fi

    pgrep -P 1 -f netperf > /dev/null
    while [ $? -eq 0 ]
    do
    sleep 1
    pgrep -P 1 -f netperf > /dev/null
    done
}

function run_cmd {

    echo "BURST SIZE IS $BURST"

    NOW=`date +%s.%N`
    echo "Starting netperfs at $NOW for $TEST" | tee $TESTLOG
    i=0;

# the starting point for our load level pauses
    PAUSE_AT=1


    while [ $i -lt $MAX_INSTANCES ]
    do
    TARGET=${REMOTE_HOSTS[`expr $i % $NUM_REMOTE_HOSTS`]}
    echo "Starting netperfs on localhost targeting ${TARGET} for $TEST" | tee -a $TESTLOG
    id=`printf "%.5d" $i`
    echo "ID: $id"
    $NETPERF -H $TARGET $NETPERF_CMD 2>&1 > netperf_${TEST}_${id}_to_${TARGET}.out &

    # give it a moment to get going
    sleep 1

    i=`expr $i + 1`

    if [ $i  -eq $PAUSE_AT ] && [ $i -ne $MAX_INSTANCES ]
    then
        NOW=`date +%s.%N`
        echo "Pausing for $DURATION seconds at $NOW with $i netperfs running for $TEST" | tee -a $TESTLOG
        sleep $DURATION
        PAUSE_AT=`expr $PAUSE_AT \* 2`
        NOW=`date +%s.%N`
        echo "Resuming at $NOW for $TEST" | tee -a $TESTLOG
    fi    
    done

    NOW=`date +%s.%N`
    echo "Netperfs started by $NOW for $TEST" | tee -a $TESTLOG

#wait for our test duration
    sleep $DURATION

#kludgey but this sleep should mean that another interim result will be emitted
    sleep 3

# stop all the netperfs
    NOW=`date +%s.%N`
    echo "Netperfs stopping $NOW for $TEST" | tee -a $TESTLOG
    kill_netperfs
    
    NOW=`date +%s.%N`
    echo "Netperfs stopped $NOW for $TEST" | tee -a $TESTLOG

}

# very much like run_cmd, but it runs the tests one at a time rather
# than in parallel.  We keep the same logging strings to be compatible
# (hopefully) with the post processing script, even though they don't
# make all that much sense :)

function run_cmd_serial {

    NOW=`date +%s.%N`
    echo "Starting netperfs at $NOW for $TEST" | tee $TESTLOG
    i=0;

# the starting point for our load level pauses
    PAUSE_AT=1


    while [ $i -lt $NUM_REMOTE_HOSTS ]
    do
    TARGET=${REMOTE_HOSTS[`expr $i % $NUM_REMOTE_HOSTS`]}
    echo "Starting netperfs on localhost targeting ${TARGET} for $TEST" | tee -a $TESTLOG
    id=`printf "%.5d" $i`
    $NETPERF -H $TARGET $NETPERF_CMD 2>&1 > netperf_${TEST}_${id}_to_${TARGET}.out &

    # give it a moment to get going
    sleep 1

    i=`expr $i + 1`

    NOW=`date +%s.%N`
    echo "Pausing for $DURATION seconds at $NOW with $i netperfs running for $TEST" | tee -a $TESTLOG
    # the plus two is to make sure we have a full set of interim
    # results.  probably not necessary here but we want to be
    # certain
    sleep `expr $DURATION + 1`
    kill_netperfs
    NOW=`date +%s.%N`
    THEN=`echo $NOW | awk -F "." '{printf("%d.%d",$1-1,$2)}'`
    echo "Resuming at $THEN for $TEST" | tee -a $TESTLOG

    done

    NOW=`date +%s.%N`
    echo "Netperfs started by $NOW for $TEST" | tee -a $TESTLOG

# stop all the netperfs - of course actually they have all been
# stopped already, we just want the log entries
    NOW=`date +%s.%N`
    echo "Netperfs stopping $NOW for $TEST" | tee -a $TESTLOG
    kill_netperfs
    NOW=`date +%s.%N`
    echo "Netperfs stopped $NOW for $TEST" | tee -a $TESTLOG
}

# here then is the "main" part

if [ ! -f ./remote_hosts ]
then
    echo "This script requires a remote_hosts file"
    exit -1
fi
. ./remote_hosts

# how many processors are there on this system
NUM_CPUS=`grep processor /proc/cpuinfo | wc -l`

# the number of netperf instances we will run will be up to 2x the
# number of CPUs
MAX_INSTANCES=`expr $NUM_CPUS \* 2`

# but at least as many as there are entries in remote_hosts
if [ $MAX_INSTANCES -lt $NUM_REMOTE_HOSTS ]
then
    MAX_INSTANCES=$NUM_REMOTE_HOSTS
fi

# allow the netperf binary to be used to be overridden 
NETPERF=${NETPERF:="netperf"}

if [ $NUM_REMOTE_HOSTS -lt 2 ]
then
    echo "The list of remote hosts is too short.  There must be at least 2."
    exit -1
fi

# we assume that netservers are already running on all the load generators

DURATION=60
# do not have a uuidgen? then use the one in netperf
MY_UUID=`uuidgen`
# with top-of-trunk we could make this 0 and run forever
# but two hours is something of a failsafe if the signals
# get lost
LENGTH="-l 7200"
OUTPUT="-o all"

DO_STREAM=0;
DO_MAERTS=0;
# NOTE!  The Bidir test depends on being able to set a socket buffer
# size greater than 13 * 64KB or 832 KB or there is a risk of the test
# hanging.  If you are running linux, make certain that
# net.core.[r|w]mem_max are sufficiently large
DO_BIDIR=0;
DO_RRAGG=1;
DO_RR=0;
DO_ANCILLARY=0;

# UDP_RR for TPC/PPS using single-byte transactions. we do not use
# TCP_RR any longer because any packet losses or other matters
# affecting the congestion window will break our desire that there be
# a one to one correspondence between requests/responses and packets.
if [ $DO_RRAGG -eq 1 ]; then
    BURST=`find_max_burst.sh ${REMOTE_HOSTS[0]}`
    if [ $BURST -eq -1 ]; then
        # use a value that find_max_burst will not have picked
        BURST=9
        echo "find_max_burst.sh returned -1 so picking a burst of $BURST"
    fi
    TEST="tps"
    TESTLOG="netperf_tps.log"
    NETPERF_CMD="-D 0.5 -c -C -f x -P 0 -t omni $LENGTH -v 2 -- -r 1 -b $BURST -e 1 -T udp -u $MY_UUID $OUTPUT"
    run_cmd
fi

# Bidirectional using burst-mode TCP_RR and large request/response size
if [ $DO_BIDIR -eq 1 ]; then
    TEST="bidirectional"
    TESTLOG="netperf_bidirectional.log"
    NETPERF_CMD="-D 0.5 -c -C -f m -P 0 -t omni $LENGTH -v 2 -- -r 64K -s 1M -S 1M -b 12 -u $MY_UUID $OUTPUT"
    run_cmd
fi

# TCP_STREAM aka outbound with a 64K send size
# the netperf command is everything but netperf -H mumble
if [ $DO_STREAM -eq 1 ];then
    TEST="outbound"
    TESTLOG="netperf_outbound.log"
    NETPERF_CMD="-D 0.5 -c -C -f m -P 0 -t omni $LENGTH -v 2 -- -m 64K -u $MY_UUID $OUTPUT"
    run_cmd
fi

# TCP_MAERTS aka inbound with a 64K send size - why is this one last?
# because presently when I pkill the netperf of a "MAERTS" test, the
# netserver does not behave well and it may not be possible to get it
# to behave well.  but we will still have all the interim results even
# if we don't get the final results, the useful parts of which will be
# the same as the other tests anyway
if [ $DO_MAERTS -eq 1 ]; then
    TEST="inbound"
    TESTLOG="netperf_inbound.log"
    NETPERF_CMD="-D 0.5 -c -C -f m -P 0 -t omni $LENGTH -v 2 -- -m ,64K -u $MY_UUID $OUTPUT"
    run_cmd
fi

# A single-stream of synchronous, no-burst TCP_RR in an "aggregate"
# script?  Yes, because the way the aggregate tests work, while there
# is a way to see what the performance of a single bulk transfer was,
# there is no way to see a basic latency - by the time
# find_max_burst.sh has completed, we are past a burst size of 0
if [ $DO_RR -eq 1 ]; then
    if [ $DURATION -lt 60 ]; then
    DURATION=60
    fi
    TEST="sync_tps"
    TESTLOG="netperf_sync_tps.log"
    NETPERF_CMD="-D 0.5 -c -C -f x -P 0 -t omni $LENGTH -v 2 -- -r 1 -u $MY_UUID $OUTPUT"
    run_cmd_serial
fi


# now some ancillary things which may nor may not work on your platform
if [ $DO_ANCILLARY -eq 1 ];then
    dmidecode 2>&1 > dmidecode.txt
    uname -a 2>&1 > uname.txt
    cat /proc/cpuinfo 2>&1 > cpuinfo.txt
    cat /proc/meminfo 2>&1 > meminfo.txt
    ifconfig -a 2>&1 > ifconfig.txt
    netstat -rn 2>&1 > netstat.txt
    dpkg -l 2>&1 > dpkg.txt
    rpm -qa 2>&1 > rpm.txt
    cat /proc/interrupts 2>&1 > interrupts.txt
    i=0
    while [ $i -lt `expr $NUM_REMOTE_HOSTS - 1` ]
    do
    traceroute ${REMOTE_HOSTS[$i]} > traceroute_${REMOTE_HOSTS[$i]}.txt
    i=`expr $i + 1`
    done
fi