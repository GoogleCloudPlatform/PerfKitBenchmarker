#!/usr/bin/tclsh
puts "SETTING CONFIGURATION"

proc wait_to_complete { seconds } {
  set x 0
  set timerstop 0
  while {!$timerstop} {
   incr x
   after 1000
    if { ![ expr {$x % 60} ] } {
    set y [ expr $x / 60 ]
    puts "Timer: $y minutes elapsed"
    }
   update
   if {  [ vucomplete ] || $x eq $seconds } { set timerstop 1 }
  }
  return
}
puts "SETTING CONFIGURATION"

vudestroy
dbset db mssqls
diset connection mssqls_azure {{IS_AZURE}}
diset connection mssqls_server {{DATABASE_IP}}
diset connection mssqls_linux_server {{DATABASE_IP}}
diset connection mssqls_port {{DATABASE_PORT}}
diset connection mssqls_pass {{DATABASE_PASSWORD}}
diset connection mssqls_uid {{DATABASE_USER}}
diset connection mssqls_tcp true
diset tpcc mssqls_count_ware {{NUM_WAREHOUSE_TPC_C}}
diset tpcc mssqls_num_vu {{VIRTUAL_USERS_TPC_C}}
diset tpcc mssqls_allwarehouse {{ALL_WAREHOUSE_TPC_C}}
diset tpcc mssqls_timeprofile {{TIME_PROFILE_TPC_C}}
if {{{LOG_TRANSACTIONS}}} {
  tcset logtotemp 1
  tcset timestamps 1
  tcset refreshrate 1
}
diset tpcc mssqls_driver timed
diset tpcc mssqls_rampup {{RAMPUP_TPC_C}}
diset tpcc mssqls_duration {{DURATION_TPC_C}}
vuset logtotemp 1


puts "Loading script"
loadscript

puts "TEST SEQUENCE STARTED"
vudestroy
puts "{{VIRTUAL_USERS_TPC_C}} VU TEST"
vuset vu {{VIRTUAL_USERS_TPC_C}}
vucreate
vurun
if {{{LOG_TRANSACTIONS}}} {
  tcstart
  tcstatus
}
wait_to_complete {{WAIT_TO_COMPLETE}}
vudestroy
if {{{LOG_TRANSACTIONS}}} {
  tcstop
}
puts "TEST SEQUENCE COMPLETE"
