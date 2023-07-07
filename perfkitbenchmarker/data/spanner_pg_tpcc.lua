#!/usr/bin/env sysbench

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- https://github.com/Percona-Lab/sysbench-tpcc/releases/tag/v2.2
-- ----------------------------------------------------------------------

require("tpcc_common")
require("tpcc_run")
require("tpcc_check")

function thread_init()

  drv = sysbench.sql.driver()
  con = drv:connect()

  set_isolation_level(drv,con)

  if drv:name() == "mysql" then
    con:query("SET autocommit=0")
  end

  if sysbench.cmdline.command == 'run' then
    for i = 1, sysbench.opt.tables do
      prepared_statements_for_run(i)
    end
   end
end

function event()
  local max_trx =  sysbench.opt.enable_purge == "yes" and 24 or 23
  local trx_type = sysbench.rand.uniform(1,max_trx)
  if trx_type <= 10 then
    trx="new_order"
  elseif trx_type <= 20 then
    trx="payment"
  elseif trx_type <= 21 then
    trx="orderstatus"
  elseif trx_type <= 22 then
    trx="delivery"
  elseif trx_type <= 23 then
    trx="stocklevel"
  elseif trx_type <= 24 then
    trx="purge"
  end

-- Execute transaction
  _G[trx]()

end

function sysbench.hooks.before_restart_event(err)
  con:query("ROLLBACK")
end

function sysbench.hooks.report_intermediate(stat)
  if sysbench.opt.report_csv == "yes" then
    sysbench.report_csv(stat)
  else
    sysbench.report_default(stat)
  end
end


-- vim:ts=4 ss=4 sw=4 expandtab
