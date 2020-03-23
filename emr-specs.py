import math
ec2_cores = 16 # per instance
ec2_ram = 64 # per instance
ec2_instances = 4 # data node count
spark_executor_cores = 5
num_executors_per_ec2 = math.floor((ec2_cores-1)/5)
total_executor_ram = math.floor((ec2_ram-1)/num_executors_per_ec2)
spark_executor_ram = math.floor(total_executor_ram * 0.90)
spark_yarn_executor_ram_overhead = math.ceil(total_executor_ram * 0.10)
spark_driver_cores = spark_executor_cores
spark_executor_instances = (num_executors_per_ec2 * ec2_instances) - 1
spark_default_parallelism = (spark_executor_instances * spark_executor_cores) * 2
