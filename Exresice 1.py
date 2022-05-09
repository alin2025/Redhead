'''
Get the amount of used space on your file system.
'''
import pyarrow as pa

fs = pa.hdfs.connect(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

used_space = pa.hdfs.HadoopFileSystem.disk_usage(fs,'hdfs://Cnt7-naya-cdh63:8020/')
print(str(round(used_space / pow(1024,2), 0)) + ' MB')
print(used_space)