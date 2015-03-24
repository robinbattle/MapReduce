1. Master: [port][data_dir]
python mr_master.py 4242 file

2. Worker: [master_ip:port] [port]
python mr_worker.py 0.0.0.0:4242 10000

3. Job: [master_ip:port] [function] [split_size] [reducer_num] [input_filename] [output_base_name]
python mr_job.py 0.0.0.0:4242 wordcount 500000 3 input3.txt output

4. Collect: [output_base_name] [output_filename] [data_dir]
python mr_collect.py output output_all file

5. Sequential: [function] [data_dir] [input_file] [output_file]
python mr_seq.py wordcount file input3.txt seq.txt



