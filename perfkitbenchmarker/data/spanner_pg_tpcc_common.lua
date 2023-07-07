-- -----------------------------------------------------------------------------
-- Common code for TPCC benchmarks.
-- https://github.com/Percona-Lab/sysbench-tpcc/releases/tag/v2.2
-- -----------------------------------------------------------------------------

ffi = require("ffi")

ffi.cdef[[
void sb_counter_inc(int, sb_counter_type);
typedef uint32_t useconds_t;
int usleep(useconds_t useconds);
]]


function init()
   assert(event ~= nil,
          "this script is meant to be included by other TPCC scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, run, cleanup, help")
end

MAXITEMS=100000
DIST_PER_WARE=10
CUST_PER_DIST=3000
-- We use the following constants to control the batch size for loading
-- individual tables. In the tests, we found that small batch sizes perform
-- better, e.g., we reduce ITEMS_PER_QUERY from 1000 to 50. Also, small batch
-- sizes perform better than updating one record each time.
ITEMS_PER_QUERY=50
CUST_PER_QUERY=10
HISTORY_PER_QUERY=100
ORDERS_PER_QUERY=100
ORDER_LINE_PER_QUERY=60
STOCK_PER_QUERY=10

-- Command line options
sysbench.cmdline.options = {
   scale =
      {"Scale factor (warehouses)", 100},
   tables =
      {"Number of tables", 1},
   use_fk =
      {"Use foreign keys", 1},
   force_pk =
      {"Force using auto-inc PK on history table", 0},
   trx_level =
      {"Transaction isolation level (RC, RR or SER)", "SER"},
   enable_purge =
      {"Use purge transaction (yes, no)", "no"},
   report_csv =
      {"Report output in csv (yes, no)", "no"},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   mysql_table_options =
      {"Extra table options, if MySQL is used. e.g. 'COLLATE latin1_bin'", ""},
   enable_pg_compat_mode =
      {"If enabled, does not allow for any non-PG compatible changes.", 0},
   enable_cluster =
      {"If enabled, load data via a cluster of machines.", 0},
   start_scale =
      {"Inclusive start scale factor (warehouses) for data loading", 1},
   end_scale =
      {"Inclusive end scale factor (warehouses) for data loading", 100},
}

function sleep(n)
   -- We pass microseconds into usleep.
   ffi.C.usleep(n * 1000 * 1000)
end

function now()
   return os.date("!%Y-%m-%dT%TZ")
end

function log(s)
   print(string.format("%s %s", now(), s))
end

function cantor(a, b)
   return (a + b + 1) * (a + b) / 2 + b
end

-- The hash function is used to decouple the dependency between orders and
-- order_lines when loading the data. This is necessary for multi-threading the
-- warehouse loading. In the original script, there is `a_counts` dictionary to
-- store the number of order lines that an order has. This is shared by the
-- loading of orders and order_lines which causes a problem when multi-threading
-- a warehouse. The thread to create order lines does not create its order. So
-- the order has a different number being set rather than the actual number of
-- order lines. By removing `a_counts` dictionary, we use a hash function to get
-- a pseudo random number. hash(warehouse_num, d_id, o_id) can always return the
-- same number when inserting orders and order lines.
function hash(a, b, c)
   return cantor(a, cantor(b, c))
end

-- Create the tables and Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --scale > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   local show_query="SHOW TABLES"
   local is_leader = 0

   if drv:name() == "mysql" then
      con:query("SET FOREIGN_KEY_CHECKS=0")
   end

   con:query("SET STATEMENT_TIMEOUT TO '1200s'")

   if sysbench.tid == 0 and sysbench.opt.enable_pg_compat_mode == 1 then
      log("Enable PG compat mode...\n")
   end

   if sysbench.tid == 0 and sysbench.opt.enable_cluster == 1 then
      log("Enable the cluster mode.\n")
   end

   if sysbench.opt.start_scale == 1 then
      is_leader = 1
   end

   if sysbench.tid == 0 and is_leader == 1 then
      log("This is a leader.\n")
   end

   if sysbench.opt.enable_cluster == 0 or (sysbench.opt.enable_cluster == 1 and is_leader == 1) then
      -- create tables in parallel table per thread
      for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
         sysbench.opt.threads do
         create_tables(drv, con, i)
      end
   end

   -- make sure all tables are created before we load data

   -- 30 seconds was not enough for tables to finish creating. Other worker
   -- threads had already started sending requests.
   log("Waiting on tables \n")
   sleep(120)

   for i = 1, sysbench.opt.tables do
      -- Create prepared statements which can only be created once for each thread/connection
      prepare_statements(con, i)
      if sysbench.opt.enable_cluster == 0 or (sysbench.opt.enable_cluster == 1 and is_leader == 1) then
         -- Only the leader machine loads the item table
         load_item_tables(drv, con, sysbench.tid, sysbench.opt.threads, i)
      end
   end

   if sysbench.opt.enable_cluster == 0 then
      for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.scale, sysbench.opt.threads do
         load_tables(drv, con, sysbench.tid, i)
      end
   else
      for i = sysbench.tid % sysbench.opt.threads + sysbench.opt.start_scale, sysbench.opt.end_scale, sysbench.opt.threads do
         load_tables(drv, con, sysbench.tid, i)
      end
   end

   log(string.format("thread_id:%d is done", sysbench.tid))
end

-- Check consistency
-- benefit from executing with --threads > 1 as long as --scale > 1
function cmd_check()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.scale,
   sysbench.opt.threads do
     check_tables(drv, con, i)
   end

end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
   check = {cmd_check, sysbench.cmdline.PARALLEL_COMMAND}
}


function create_tables(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   local tinyint_type="int"
   local datetime_type="timestamptz"

   if drv:name() == "mysql" or drv:name() == "attachsql" or
      drv:name() == "drizzle"
   then
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
      extra_table_options = sysbench.opt.mysql_table_options or ""
      tinyint_type="tinyint"
      datetime_type="datetime"
   end

   log(string.format("Creating tables: %d\n", table_num))

   query = string.format([[
   CREATE TABLE IF NOT EXISTS warehouse%d (
   w_id int not null,
   w_name varchar(10),
   w_street_1 varchar(20),
   w_street_2 varchar(20),
   w_city varchar(20),
   w_state varchar(2),
   w_zip varchar(9),
   w_tax decimal,
   w_ytd decimal,
   primary key (w_id)
   ) %s %s;]],
        table_num, engine_def, extra_table_options)

   query = query .. string.format([[
   create table IF NOT EXISTS district%d (
   d_id ]] .. tinyint_type .. [[ not null,
   d_w_id int not null,
   d_name varchar(10),
   d_street_1 varchar(20),
   d_street_2 varchar(20),
   d_city varchar(20),
   d_state varchar(2),
   d_zip varchar(9),
   d_tax decimal,
   d_ytd decimal,
   d_next_o_id int,
   primary key (d_w_id, d_id)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

-- CUSTOMER TABLE

   query = query .. string.format([[
   create table IF NOT EXISTS customer%d (
   c_id int not null,
   c_d_id int not null,
   c_w_id int not null,
   c_first varchar(16),
   c_middle varchar(2),
   c_last varchar(16),
   c_street_1 varchar(20),
   c_street_2 varchar(20),
   c_city varchar(20),
   c_state varchar(2),
   c_zip varchar(9),
   c_phone varchar(16),
   c_since ]] .. datetime_type .. [[,
   c_credit varchar(2),
   c_credit_lim bigint,
   c_discount decimal,
   c_balance decimal,
   c_ytd_payment decimal,
   c_payment_cnt int,
   c_delivery_cnt int,
   c_data text,
   PRIMARY KEY(c_w_id, c_d_id, c_id)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

-- HISTORY TABLE
   local hist_auto_inc=""
   -- Spanner requires a primary key to be defined.
   local hist_pk=",PRIMARY KEY(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date)"
   if sysbench.opt.force_pk == 1 then
      -- AUTO_INCREMENT is not supported.
      hist_auto_inc="id int NOT NULL AUTO_INCREMENT,"
      hist_pk=",PRIMARY KEY(id)"
   end
   query = query .. string.format([[
   create table IF NOT EXISTS history%d (
        %s
   h_c_id int,
   h_c_d_id ]] .. tinyint_type .. [[,
   h_c_w_id int,
   h_d_id ]] .. tinyint_type .. [[,
   h_w_id int,
   h_date ]] .. datetime_type .. [[,
   h_amount decimal,
   h_data varchar(24) %s
   ) %s %s;]],
      table_num, hist_auto_inc, hist_pk, engine_def, extra_table_options)

   query = query .. string.format([[
   create table IF NOT EXISTS orders%d (
   o_id int not null,
   o_d_id ]] .. tinyint_type .. [[ not null,
   o_w_id int not null,
   o_c_id int,
   o_entry_d ]] .. datetime_type .. [[,
   o_carrier_id ]] .. tinyint_type .. [[,
   o_ol_cnt ]] .. tinyint_type .. [[,
   o_all_local ]] .. tinyint_type .. [[,
   PRIMARY KEY(o_w_id, o_d_id, o_id)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

-- NEW_ORDER table

   query = query .. string.format([[
   create table IF NOT EXISTS new_orders%d (
   no_o_id int not null,
   no_d_id ]] .. tinyint_type .. [[ not null,
   no_w_id int not null,
   PRIMARY KEY(no_w_id, no_d_id, no_o_id)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

   query = query .. string.format([[
   create table IF NOT EXISTS order_line%d (
   ol_o_id int not null,
   ol_d_id ]] .. tinyint_type .. [[ not null,
   ol_w_id int not null,
   ol_number ]] .. tinyint_type .. [[ not null,
   ol_i_id int,
   ol_supply_w_id int,
   ol_delivery_d ]] .. datetime_type .. [[,
   ol_quantity ]] .. tinyint_type .. [[,
   ol_amount decimal,
   ol_dist_info varchar(24),
   PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

-- STOCK table

   query = query .. string.format([[
   create table IF NOT EXISTS stock%d (
   s_i_id int not null,
   s_w_id int not null,
   s_quantity int,
   s_dist_01 varchar(24),
   s_dist_02 varchar(24),
   s_dist_03 varchar(24),
   s_dist_04 varchar(24),
   s_dist_05 varchar(24),
   s_dist_06 varchar(24),
   s_dist_07 varchar(24),
   s_dist_08 varchar(24),
   s_dist_09 varchar(24),
   s_dist_10 varchar(24),
   s_ytd decimal,
   s_order_cnt int,
   s_remote_cnt int,
   s_data varchar(50),
   PRIMARY KEY(s_w_id, s_i_id)
   ) %s %s;]],
      table_num, engine_def, extra_table_options)

   local i = table_num

   query = query .. string.format([[
   create table IF NOT EXISTS item%d (
   i_id int not null,
   i_im_id int,
   i_name varchar(24),
   i_price decimal,
   i_data varchar(50),
   PRIMARY KEY(i_id)
   ) %s %s;]],
      i, engine_def, extra_table_options)

   log(string.format("Adding indexes %d ... \n", i))
   query = query .. "CREATE INDEX idx_customer"..i.." ON customer"..i.." (c_w_id,c_d_id,c_last,c_first);"
   query = query .. "CREATE INDEX idx_orders"..i.." ON orders"..i.." (o_w_id,o_d_id,o_c_id,o_id);"
   query = query .. "CREATE INDEX fkey_stock_2"..i.." ON stock"..i.." (s_i_id);"
   query = query .. "CREATE INDEX fkey_order_line_2"..i.." ON order_line"..i.." (ol_supply_w_id,ol_i_id);"
   query = query .. "CREATE INDEX fkey_history_1"..i.." ON history"..i.." (h_c_w_id,h_c_d_id,h_c_id);"
   query = query .. "CREATE INDEX fkey_history_2"..i.." ON history"..i.." (h_w_id,h_d_id );"
   if sysbench.opt.use_fk == 1 then
      log(string.format("Adding FK %d ... \n", i))
      query = query .. "ALTER TABLE new_orders"..i.." ADD CONSTRAINT fkey_new_orders_1_"..i.." FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES orders"..i.."(o_w_id,o_d_id,o_id);"
      query = query .. "ALTER TABLE orders"..i.." ADD CONSTRAINT fkey_orders_1_"..i.." FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer"..i.."(c_w_id,c_d_id,c_id);"
      query = query .. "ALTER TABLE customer"..i.." ADD CONSTRAINT fkey_customer_1_"..i.." FOREIGN KEY(c_w_id,c_d_id) REFERENCES district"..i.."(d_w_id,d_id);"
      query = query .. "ALTER TABLE history"..i.." ADD CONSTRAINT fkey_history_1_"..i.." FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer"..i.."(c_w_id,c_d_id,c_id);"
      query = query .. "ALTER TABLE history"..i.." ADD CONSTRAINT fkey_history_2_"..i.." FOREIGN KEY(h_w_id,h_d_id) REFERENCES district"..i.."(d_w_id,d_id);"
      query = query .. "ALTER TABLE district"..i.." ADD CONSTRAINT fkey_district_1_"..i.." FOREIGN KEY(d_w_id) REFERENCES warehouse"..i.."(w_id);"
      query = query .. "ALTER TABLE order_line"..i.." ADD CONSTRAINT fkey_order_line_1_"..i.." FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES orders"..i.."(o_w_id,o_d_id,o_id);"
      query = query .. "ALTER TABLE order_line"..i.." ADD CONSTRAINT fkey_order_line_2_"..i.." FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock"..i.."(s_w_id,s_i_id);"
      query = query .. "ALTER TABLE stock"..i.." ADD CONSTRAINT fkey_stock_1_"..i.." FOREIGN KEY(s_w_id) REFERENCES warehouse"..i.."(w_id);"
      query = query .. "ALTER TABLE stock"..i.." ADD CONSTRAINT fkey_stock_2_"..i.." FOREIGN KEY(s_i_id) REFERENCES item"..i.."(i_id);"
   end

   log("Send all DDLs in a single query.\n")
   con:query(query)

   log('Finished creating tables\n')
end

function prepare_statements(con, table_num)
   -- Each connection needs to claim prepared statements
   con:query("PREPARE insert_item" .. table_num .. " as INSERT INTO item" .. table_num .." (i_id, i_im_id, i_name, i_price, i_data) values($1,$2,$3,$4,$5);")
   con:query("PREPARE insert_customer" .. table_num .. " as INSERT INTO customer" .. table_num .. [[
      (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
         c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt,
            c_data) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21 );]])
   con:query("PREPARE insert_history" .. table_num .. "(bigint,bigint,bigint,bigint,bigint,varchar)"..
      " as INSERT INTO history" .. table_num ..
      [[(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) values($1, $2, $3, $4, $5, Now(), 10.0, $6 );]])
   con:query("PREPARE insert_order" .. table_num .. " as INSERT INTO orders" .. table_num .. [[
      (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) values($1, $2, $3, $4, $5, $6, $7, $8);]])
   con:query("PREPARE insert_stock" .. table_num .. " as INSERT INTO stock" .. table_num ..
      " (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, "..
      " s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) "..
      "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);")
   con:query("PREPARE insert_order_line"..table_num.." as INSERT INTO order_line" .. table_num .. [[
      (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
            ol_quantity, ol_amount, ol_dist_info ) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 );]])
end

function load_item_tables(drv, con, tid, threads, table_num)
   log('Start - loading the table `item' .. table_num .. '` by thread #' .. tid .. '\n')
   local i = table_num

   -- Break up inserting 100k items to multiple queries.
   assert(MAXITEMS % ITEMS_PER_QUERY == 0)
   num_queries = MAXITEMS / ITEMS_PER_QUERY

   for k = tid % threads, num_queries - 1, threads do
      local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
      for j = 1 + (k * ITEMS_PER_QUERY), (k + 1) * ITEMS_PER_QUERY do
         local i_im_id = sysbench.rand.uniform(1,10000)
         local i_price = sysbench.rand.uniform_double()*100+1
         -- i_name is not generated as prescribed by standard, but we want to provide a better compression
         local i_name  = string.format("item-%d-%f-%s", i_im_id, i_price, sysbench.rand.string("@@@@@"))
         local i_data  = string.format("data-%s-%s", i_name, sysbench.rand.string("@@@@@"))

         query = string.format(
            "EXECUTE insert_item"..i.." (%d,%d,'%s',%f,'%s');",
            j, i_im_id, i_name:sub(1,24), i_price, i_data:sub(1,50))
         buf[#buf+1] = query
      end
      if sysbench.opt.enable_pg_compat_mode ~= 1 then
         buf[#buf+1] = "RUN BATCH;"
      end
      con:query(table.concat( buf ))
   end
   log('End - loading the table `item' .. table_num .. '` by thread #' .. tid .. '\n')
end

function set_isolation_level(drv,con)
   if drv:name() == "mysql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE-READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ-COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end

        isolation_variable=con:query_row("SHOW VARIABLES LIKE 't%_isolation'")

        con:query("SET SESSION " .. isolation_variable .. "='".. isolation_level .."'")
   end

   if drv:name() == "pgsql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end

        con:query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " .. isolation_level )
   end

end



function load_tables(drv, con, tid, warehouse_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   -- 30 sec timeout generally too short. Set to something arbitrarily long.
   con:query("SET STATEMENT_TIMEOUT TO '1200s'")

   for table_num = 1, sysbench.opt.tables do

   log(string.format("loading tables: %d for warehouse: %d by thread %d\n", table_num, warehouse_num, tid))

-- WAREHOUSE TABLE
      con:bulk_insert_init("INSERT INTO warehouse" .. table_num ..
         " (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) values")

      query = string.format([[(%d, '%s','%s','%s', '%s', '%s', '%s', %f,300000)]],
         warehouse_num, sysbench.rand.string("name-@@@@@"), sysbench.rand.string("street1-@@@@@@@@@@"),
         sysbench.rand.string("street2-@@@@@@@@@@"), sysbench.rand.string("city-@@@@@@@@@@"),
         sysbench.rand.string("@@"),sysbench.rand.string("zip-#####"),sysbench.rand.uniform_double()*0.2 )
      con:bulk_insert_next(query)
      con:bulk_insert_done()

-- DISTRICT TABLE
      con:bulk_insert_init("INSERT INTO district" .. table_num ..
         " (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) values")

      for d_id = 1 , DIST_PER_WARE do

         query = string.format([[(%d, %d, '%s','%s','%s', '%s', '%s', '%s', %f,30000,3001)]],
            d_id, warehouse_num, sysbench.rand.string("name-@@@@@"), sysbench.rand.string("street1-@@@@@@@@@@"),
            sysbench.rand.string("street2-@@@@@@@@@@"), sysbench.rand.string("city-@@@@@@@@@@"),
            sysbench.rand.string("@@"),sysbench.rand.string("zip-#####"),sysbench.rand.uniform_double()*0.2 )
         con:bulk_insert_next(query)

      end
      con:bulk_insert_done()

-- CUSTOMER TABLE
   assert(CUST_PER_DIST % CUST_PER_QUERY == 0)
   num_queries = CUST_PER_DIST / CUST_PER_QUERY
   for d_id = 1 , DIST_PER_WARE do
      for c_iter = 0, num_queries - 1 do
         local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
         for c_id = 1 + (c_iter * CUST_PER_QUERY), (c_iter + 1) * CUST_PER_QUERY do
            local c_last

            if c_id <= 1000 then
               c_last = Lastname(c_id - 1)
            else
               c_last = Lastname(NURand(255, 0, 999))
            end

            query = string.format([[EXECUTE insert_customer]] .. table_num .. [[ (%d, %d, %d, '%s','OE','%s','%s', '%s', '%s', '%s', '%s','%s','%s','%s',50000,%f,-10.0,10.0,1,0,'%s' );]],
               c_id,
               d_id,
               warehouse_num,
               sysbench.rand.string("first-"..string.rep("@",sysbench.rand.uniform(2,10))),
               c_last,
               sysbench.rand.string("street1-@@@@@@@@@@"),
               sysbench.rand.string("street2-@@@@@@@@@@"),
               sysbench.rand.string("city-@@@@@@@@@@"),
               sysbench.rand.string("@@"),
               sysbench.rand.string("zip-#####"),
               sysbench.rand.string(string.rep("#",16)),
               now(),
               (sysbench.rand.uniform(1,100) > 10) and "GC" or "BC",
               sysbench.rand.uniform_double()*0.5,
               string.rep(sysbench.rand.string("@"), sysbench.rand.uniform(300,500))
            )
            buf[#buf+1] = query
         end
         if sysbench.opt.enable_pg_compat_mode ~= 1 then
            buf[#buf+1] = "RUN BATCH;"
         end
         con:query(table.concat( buf ))
      end
   end

-- HISTORY TABLE
   assert(CUST_PER_DIST % HISTORY_PER_QUERY == 0)
   num_queries = CUST_PER_DIST / HISTORY_PER_QUERY
   for d_id = 1 , DIST_PER_WARE do
      for c_iter = 0, num_queries - 1 do
         local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
         for c_id = 1 + (c_iter * HISTORY_PER_QUERY), (c_iter + 1) * HISTORY_PER_QUERY do
            query = string.format([[EXECUTE insert_history]] .. table_num .. [[ (%d, %d, %d, %d, %d, '%s' );]],
               c_id,
               d_id,
               warehouse_num,
               d_id,
               warehouse_num,
               string.rep(sysbench.rand.string("@"),sysbench.rand.uniform(12,24))
            )
            buf[#buf+1] = query
         end
         if sysbench.opt.enable_pg_compat_mode ~= 1 then
            buf[#buf+1] = "RUN BATCH;"
         end
         local final_string = table.concat( buf )
         con:query(final_string)
      end
   end

-- ORDERS TABLE

   local tab = {}

   for i = 1, 3000 do
      tab[i] = i
   end

   for i = 1, 3000 do
      local j = math.random(i, 3000)
      tab[i], tab[j] = tab[j], tab[i]
   end

   assert(CUST_PER_DIST % ORDERS_PER_QUERY == 0)
   num_queries = CUST_PER_DIST / ORDERS_PER_QUERY
   for d_id = 1 , DIST_PER_WARE do
      for o_incr = 0, num_queries - 1 do
         local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
         for o_id = 1 + (o_incr * ORDERS_PER_QUERY) , (1 + o_incr) * ORDERS_PER_QUERY do
            query = string.format([[EXECUTE insert_order]] .. table_num .. [[ (%d, %d, %d, %d, '%s', %s, %d, 1 );]],
               o_id,
               d_id,
               warehouse_num,
               tab[o_id],
               now(),
               o_id < 2101 and sysbench.rand.uniform(1,10) or "NULL",
               hash(warehouse_num, d_id, o_id) % 11 + 5
            )
            buf[#buf+1] = query
         end
         if sysbench.opt.enable_pg_compat_mode ~= 1 then
            buf[#buf+1] = "RUN BATCH;"
         end
         con:query(table.concat( buf ))
      end
   end

-- STOCK table

   assert(MAXITEMS % STOCK_PER_QUERY == 0)
   num_queries = MAXITEMS / STOCK_PER_QUERY
   for s_incr = 0, num_queries - 1 do
      local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
      for s_id = 1 + (s_incr * STOCK_PER_QUERY) , (1 + s_incr) * STOCK_PER_QUERY do
         query = string.format([[EXECUTE insert_stock]] .. table_num .. [[ (%d, %d, %d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',0,0,0,'%s');]],
            s_id, warehouse_num, sysbench.rand.uniform(10,100),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),24),
            string.rep(sysbench.rand.string("@"),sysbench.rand.uniform(26,50)))
         buf[#buf+1] = query
      end
      if sysbench.opt.enable_pg_compat_mode ~= 1 then
         buf[#buf+1] = "RUN BATCH;"
      end
      con:query(table.concat( buf ))
   end

-- NEW_ORDERS table

      -- Hitting transaction limit 20000
      con:query(string.format("INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id) SELECT o_id, o_d_id, o_w_id FROM orders%d WHERE o_id>2100 and o_id<=2300 and o_w_id=%d", table_num, table_num, warehouse_num))
      con:query(string.format("INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id) SELECT o_id, o_d_id, o_w_id FROM orders%d WHERE o_id>2300 and o_id<=2500 and o_w_id=%d", table_num, table_num, warehouse_num))
      con:query(string.format("INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id) SELECT o_id, o_d_id, o_w_id FROM orders%d WHERE o_id>2500 and o_id<=2700 and o_w_id=%d", table_num, table_num, warehouse_num))
      con:query(string.format("INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id) SELECT o_id, o_d_id, o_w_id FROM orders%d WHERE o_id>2700 and o_id<=3000 and o_w_id=%d", table_num, table_num, warehouse_num))

-- ORDER_LINE table

   assert(CUST_PER_DIST % ORDER_LINE_PER_QUERY == 0)
   num_queries = CUST_PER_DIST / ORDER_LINE_PER_QUERY
   for d_id = 1 , DIST_PER_WARE do
      for o_iter = 0, num_queries - 1 do
         local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
         for o_id = 1 + (o_iter * ORDER_LINE_PER_QUERY), (o_iter + 1) * ORDER_LINE_PER_QUERY do
            o_ol_cnt = hash(warehouse_num, d_id, o_id) % 11 + 5
            for ol_id = 1, o_ol_cnt do
               query = string.format([[EXECUTE insert_order_line]]..table_num..[[ (%d, %d, %d, %d, %d, %d, %s, 5, %f, '%s' );]],
                  o_id,
                  d_id,
                  warehouse_num,
                  ol_id,
                  sysbench.rand.uniform(1, MAXITEMS),
                  warehouse_num,
                  o_id < 2101 and string.format("'%s'", now()) or "NULL",
                  o_id < 2101 and 0 or sysbench.rand.uniform_double()*9999.99,
                  string.rep(sysbench.rand.string("@"),24)
               )
               buf[#buf+1] = query
            end
         end
         if sysbench.opt.enable_pg_compat_mode ~= 1 then
            buf[#buf+1] = "RUN BATCH;"
         end
         con:query(table.concat( buf ))
      end
   end

  end

end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
   con:query("SET AUTOCOMMIT=0")

end

function thread_done()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   if drv:name() == "mysql" then
      con:query("SET FOREIGN_KEY_CHECKS=0")
   end

   for i = 1, sysbench.opt.tables do
      log(string.format("Dropping tables '%d'...", i))
      -- drop constraints first
      log(string.format("Deleting indexes %d ... \n", i))
      con:query("DROP INDEX idx_customer"..i)
      con:query("DROP INDEX idx_orders"..i)
      con:query("DROP INDEX fkey_stock_2"..i)
      con:query("DROP INDEX fkey_order_line_2"..i)
      con:query("DROP INDEX fkey_history_1"..i)
      con:query("DROP INDEX fkey_history_2"..i)
      con:query("DROP TABLE IF EXISTS history" .. i )
      con:query("DROP TABLE IF EXISTS new_orders" .. i )
      con:query("DROP TABLE IF EXISTS order_line" .. i )
      con:query("DROP TABLE IF EXISTS orders" .. i )
      con:query("DROP TABLE IF EXISTS customer" .. i )
      con:query("DROP TABLE IF EXISTS district" .. i )
      con:query("DROP TABLE IF EXISTS stock" .. i )
      con:query("DROP TABLE IF EXISTS item" .. i )
      con:query("DROP TABLE IF EXISTS warehouse" .. i )
   end
end

function Lastname(num)
  local n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}

  name =n[math.floor(num / 100) + 1] .. n[ math.floor(num / 10)%10 + 1] .. n[num%10 + 1]

  return name
end

local init_rand=1
local C_255
local C_1023
local C_8191

function NURand (A, x, y)
   local C

   if init_rand
   then
      C_255 = sysbench.rand.uniform(0, 255)
      C_1023 = sysbench.rand.uniform(0, 1023)
      C_8191 = sysbench.rand.uniform(0, 8191)
      init_rand = 0
   end

   if A==255
   then
      C = C_255
   elseif A==1023
   then
      C = C_1023
   elseif A==8191
   then
      C = C_8191
   end

   -- return ((( sysbench.rand.uniform(0, A) | sysbench.rand.uniform(x, y)) + C) % (y-x+1)) + x;
   return ((( bit.bor(sysbench.rand.uniform(0, A), sysbench.rand.uniform(x, y))) + C) % (y-x+1)) + x;
end

-- vim:ts=4 ss=4 sw=4 expandtab
