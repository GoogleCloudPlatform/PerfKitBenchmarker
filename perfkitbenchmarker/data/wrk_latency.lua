-- Copyright 2015 Google Inc. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Write stats to stdout in CSV format, for simpler parsing by PKB.
function done(summary, latency, requests)
        io.write("==CSV==\n")
        io.write("variable,value,unit\n")
        for _, p in pairs({ 5, 25, 50, 75, 90, 99, 99.9 }) do
                n = latency:percentile(p)
                io.write(string.format("p%g latency,%g,ms\n", p, n / 1000))
        end
        local errors = summary.errors.connect + summary.errors.read +
            summary.errors.write + summary.errors.timeout
        local data = {
                {'bytes transferred', summary.bytes, 'bytes'},
                {'errors', errors, 'n'},
                {'requests', summary.requests, 'n'},
                {'throughput',
                 summary.requests / summary.duration * 1e6,
                 'requests/sec'}
        }
        for _, p in pairs(data) do
                io.write(string.format("%s,%g,%s\n", p[1], p[2], p[3]))
        end
end
