1. Master: [port][data_dir]
python mr_master.py 4242 file
python mr_master.py 15000 file

2. Worker: [master_ip:port] [self_ip] [self_port]
python mr_worker.py 0.0.0.0:4242 10000
python mr_worker.py bass08.cs.usfca.edu:15000 15001


3. Job: [master_ip:port] [function] [split_size] [reducer_num] [input_filename] [output_base_name]
python mr_job.py 0.0.0.0:4242 wordcount 500000 3 input3.txt output
python mr_job.py bass08.cs.usfca.edu:150000 wordcount 500000 3 bigtext.txt output

4. Collect: [output_base_name] [output_filename] [data_dir]
python mr_collect.py output output_all file

5. Sequential: [function] [data_dir] [input_file] [output_file]
python mr_seq.py wordcount file bigtext.txt seq.txt


ssh stargate.cs.usfca.edu
/home2/blu2/MapReduce




