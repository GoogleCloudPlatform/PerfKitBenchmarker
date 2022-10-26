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

#!/usr/bin/tclsh

puts "SETTING BUILD CONFIGURATION"

dbset db pg
vudestroy
dbset bm TPC-H
diset tpch pg_tpch_superuser {{DATABASE_USER}}
diset tpch pg_tpch_superuserpass {{DATABASE_PASSWORD}}
diset tpch pg_user temp
diset tpch pg_tpch_pass {{DATABASE_PASSWORD}}
diset connection  pg_port  {{DATABASE_PORT}}
diset connection pg_host {{DATABASE_IP}}
diset connection pg_azure {{IS_AZURE}}
diset tpch pg_scale_fact {{SCALE_FACTOR_TPC_H}}

diset tpch pg_num_tpch_threads {{VIRTUAL_USERS_TPC_H}}

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

