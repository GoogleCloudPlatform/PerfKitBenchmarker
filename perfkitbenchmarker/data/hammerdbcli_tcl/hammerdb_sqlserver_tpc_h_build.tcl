#!/usr/bin/tclsh

proc runtimer { seconds } {
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
puts "SETTING BUILD CONFIGURATION"

dbset db mssqls
diset connection mssqls_azure {{IS_AZURE}}
vudestroy
dbset bm TPC-H
diset connection mssqls_server {{DATABASE_IP}}
diset connection mssqls_host {{DATABASE_IP}}
diset connection mssqls_linux_server {{DATABASE_IP}}
diset connection mssqls_port {{DATABASE_PORT}}
diset connection mssqls_pass {{DATABASE_PASSWORD}}
diset connection mssqls_uid {{DATABASE_USER}}
diset connection mssqls_tcp true
diset connection mssqls_authentication sql
diset tpch mssqls_driver timed
diset tpch mssqls_refresh_on 1
diset tpch mssqls_scale_fact {{SCALE_FACTOR_TPC_H}}
diset tpch mssqls_maxdop 1
diset tpch mssqls_num_tpch_threads {{VIRTUAL_USERS_TPC_H}}
diset tpch mssqls_total_querysets 1
diset tpch mssqls_colstore 0
buildschema
puts " Schema built successfully"

runtimer {{BUILD_TIMEOUT}}
puts "calling vudestory"
vudestroy
puts "called vudestroy"
puts *test_build_sequence_complete_sentence*
