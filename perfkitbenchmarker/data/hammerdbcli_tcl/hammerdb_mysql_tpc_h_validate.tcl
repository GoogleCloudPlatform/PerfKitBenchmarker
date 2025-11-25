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

dbset db mysql
vudestroy
dbset bm TPC-H
diset tpch mysql_tpch_user {{DATABASE_USER}}
diset tpch mysql_tpch_pass {{DATABASE_PASSWORD}}
diset connection  mysql_port  {{DATABASE_PORT}}
diset connection mysql_host {{DATABASE_IP}}
diset tpch mysql_scale_fact {{SCALE_FACTOR_TPC_H}}
diset tpch mysql_tpch_storage_engine innodb

diset tpch mysql_num_tpch_threads {{VIRTUAL_USERS_TPC_H}}

loadscript
puts " load script completed"
vudestroy
vuset vu {{VIRTUAL_USERS_TPC_H}}
puts "vu set done"
vucreate
puts "vu create done"
puts "VALIDATION STARTED"
checkschema
runtimer 60
puts "second time completed"
after 60
puts "VALIDATION COMPLETE"
