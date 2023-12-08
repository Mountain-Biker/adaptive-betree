# Designed test results

## Test 1: Approach 1 (without shortening betree)
### workload 1 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/0%
#### fixed epsilon = 0.4
[comment]: <> (./test_logging_restore -m test -C 4096 -S false -z 256 -f 16 -e 0.4 -a 7 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_0.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 4096, max_node_size = 256, min_flush_size = 16, time = 12.1235, split_counter = 649

#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 4096 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_0.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 4096, max_node_size = 256, min_flush_size = 16, time = 12.163, split_counter = 649


### workload 2 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/10%
#### fixed epsilon = 0.4
[comment]: <> (./test_logging_restore -m test -C 4096 -S false -z 256 -f 16 -e 0.4 -a 7 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_10.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 4096, max_node_size = 256, min_flush_size = 16, time = 14.9471, split_counter = 2947

#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 4096 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_10.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 4096, max_node_size = 256, min_flush_size = 16, time = 14.8502, split_counter = 2550


### workload 3 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/20%
#### fixed epsilon = 0.4
[comment]: <> (./test_logging_restore -m test -C 16384 -S false -z 256 -f 16 -e 0.4 -a 7 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_20.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 16, time = 18.7797, split_counter = 5608

#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 16384 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_20.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 16, time = 16.7225, split_counter = 4713


## Test 2: Approach 2 (shortening betree)
### workload 1 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/0%
#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_0.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 16, time = 10.0848, split_counter = 649
[comment]: <> (parameters before shortening the Be-tree: number of leaves: 535, total leaves height: 2140, average betree nodes height(before shortening betree): 4; parameters after shortening the Be-tree: number of leaves: 535, total leaves height: 1137, average betree nodes height(before shortening betree): 2.12523)


### workload 2 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/10%
#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_10.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 16, time = 11.9279, split_counter = 2570

### workload 3 : write_heavy(100k) + read_heavy(4m), write_ratio = 100%/20%
#### adpative betree: write_heavy_epsilon = 0.4, read_heavy_epsilon = 0.8, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r10m_wratio_100_20.txt -t 4100000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 16, time = 14.043, split_counter = 2570


## Test 3. min flush size
### workload 1 : write_heavy(1m) , write_ratio = 100%
#### fixed epsilon = 0.4
[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 2 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 1000000 -c 50000000 -p 50000000)
(1) cache_size = 16384, max_node_size = 256, min_flush_size = 2, time = 6.57437, split_counter = 6516, average_height = 5

[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 4 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 1000000 -c 50000000 -p 50000000)
(2) cache_size = 16384, max_node_size = 256, min_flush_size = 4, time = 6.5211, split_counter = 6516, average_height = 5


[comment]: <> (./test_logging_restore -m test -C 16384 -S true -z 256 -f 8 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 1000000 -c 50000000 -p 50000000)
(3) cache_size = 16384, max_node_size = 256, min_flush_size = 8, time = 7.28027, split_counter = 6516, average_height = 5


[comment]: <> (./test_logging_restore -m test -C 256 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 200000 -c 50000000 -p 50000000)
(4) cache_size = 8192, max_node_size = 256, min_flush_size = 16, time = 28.0943, split_counter = 15316, average_height = 4


[comment]: <> (./test_logging_restore -m test -C 1024 -S true -z 256 -f 32 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 400000 -c 50000000 -p 50000000)
(5) cache_size = 8192, max_node_size = 256, min_flush_size = 32, time = 28.5819, split_counter = 15316, average_height = 4


[comment]: <> (./test_logging_restore -m test -C 1024 -S true -z 256 -f 64 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 400000 -c 50000000 -p 50000000)
(6) cache_size = 8192, max_node_size = 256, min_flush_size = 64, time = , split_counter = , average_height = 


[comment]: <> (./test_logging_restore -m test -C 1024 -S true -z 256 -f 128 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w1m_r10m_wratio_100_0.txt -t 400000 -c 50000000 -p 50000000)
(7) cache_size = 8192, max_node_size = 256, min_flush_size = 128, time = 26.2146, split_counter = 9253, average_height = 5


## Test 4: workloads pattern changes twice
### workload 1: 100k write-heavy + 4M read-heavy + 1M write-heavy， write ratio = 100%/0/100%
#### 4.1.1 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.15, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.15 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(3) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 40.5851, split_counter = 18315, average_height = 29.1299, max_height = 30, pivots_size_at_the_end = 2


#### 4.1.2 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.15, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.15 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 41.8315, split_counter = 18266, average_height = 30, max_height = 30, pivots_size_at_the_end = 2


#### 4.1.3 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.3, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.3 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(3) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 31.1118, split_counter = 8789, average_height = 7.12993, max_height = 8, pivots_size_at_the_end = 5


#### 4.1.4 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.3, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.3 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 29.3214, split_counter = 8690, average_height = 7, max_height = 7, pivots_size_at_the_end = 5


#### 4.1.5 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.4, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(3) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 27.7789, split_counter = 7444, average_height = 5.12993, max_height = 6, pivots_size_at_the_end = 9


#### 4.1.6 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.4, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 65536 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w1m_wratio_100_0_100.txt -t 5100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 25.6904, split_counter = 7390, average_height = 5, max_height = 5, pivots_size_at_the_end = 9





### workload 2: 100k write-heavy + 4M read-heavy + 4M write-heavy， write ratio = 100%/0/100%
#### 4.2.1 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.15, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.15 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(3) cache_size = 2662144, max_node_size = 256, min_flush_size = 16, time = 149.354, split_counter = 65586, average_height = 40.1274, max_height = 41, pivots_size_at_the_end = 2


#### 4.2.2 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.15, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.15 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(1) cache_size = 2662144, max_node_size = 256, min_flush_size = 16, time = 149.55, split_counter = 65636, average_height = 40, max_height = 40, pivots_size_at_the_end = 2


#### 4.2.3 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.3, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.3 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(3) cache_size = 2662144, max_node_size = 256, min_flush_size = 16, time = 53.1571, split_counter = 31570, average_height = 8.12744, max_height = 9, pivots_size_at_the_end = 5


#### 4.2.4 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.3, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.3 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 54.7891, split_counter = 31468, average_height = 8, max_height = 8, pivots_size_at_the_end = 5


#### 4.2.5 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.4, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(3) cache_size = 2662144, max_node_size = 256, min_flush_size = 16, time = 45.9441, split_counter = 26687, average_height = 6.12744, max_height = 7, pivots_size_at_the_end = 9


#### 4.2.6 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.4, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.4 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 48.2258, split_counter = 26693, average_height = 6, max_height = 6, pivots_size_at_the_end = 9


#### 4.2.7 adpative betree (with shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.45, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S true -z 256 -f 16 -e 0.4 -a 0 -w 0.45 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(3) cache_size = 2662144, max_node_size = 256, min_flush_size = 16, time = 42.4919, split_counter = 24273, average_height = 4.12744, max_height = 5, pivots_size_at_the_end = 16


#### 4.2.8 adpative betree (without shortening): original_epsilon = 0.4, read_heavy_epsilon = 0.8, write_heavy_epsilon = 0.5, workload_predictor_granularity = 500
[comment]: <> (./test_logging_restore -m test -C 2662144 -S false -z 256 -f 16 -e 0.4 -a 0 -w 0.5 -r 0.8 -d tmpdir -i test_input_w100k_r4m_w10m_wratio_100_0_100.txt -t 8100000 -c 50000000 -p 50000000)
(1) cache_size = 65536, max_node_size = 256, min_flush_size = 16, time = 42.9942, split_counter = 24205, average_height = 4, max_height = 4, pivots_size_at_the_end = 16
