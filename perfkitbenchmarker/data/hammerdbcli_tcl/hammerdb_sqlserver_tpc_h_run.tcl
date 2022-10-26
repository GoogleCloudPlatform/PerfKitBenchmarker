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
puts "SETTING RUN CONFIGURATION"
dbset db mssqls
diset connection mssqls_azure {{IS_AZURE}}
dbset bm TPC-H

diset connection mssqls_server {{DATABASE_IP}}
diset connection mssqls_host {{DATABASE_IP}}
diset connection mssqls_linux_server {{DATABASE_IP}}
diset connection mssqls_port {{DATABASE_PORT}}
diset connection mssqls_pass {{DATABASE_PASSWORD}}
diset connection mssqls_uid {{DATABASE_USER}}

diset connection mssqls_tcp true
diset tpch mssqls_driver timed
diset tpch mssqls_refresh_on 1
diset tpch mssqls_scale_fact {{SCALE_FACTOR_TPC_H}}
diset tpch mssqls_maxdop 1
diset tpch mssqls_num_tpch_threads {{VIRTUAL_USERS_TPC_H}}
diset tpch mssqls_total_querysets 1
diset tpch mssqls_colstore false
vudestroy
puts "called vudestroy"
loadscript
puts " load script completed"
vudestroy
puts "SEQUENCE STARTED"
vuset vu {{VIRTUAL_USERS_TPC_H}}
puts "vu set done"
vucreate
puts "vu create done"
vurun
puts "vurun done"
runtimer 120000
puts "second time completed"
after 120000
puts " delay for 5 seconds"
puts "RUN SEQUENCE COMPLETE"
