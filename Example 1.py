'''
Get the capacity of file system on a human readable format
'''

import pyarrow as pa

fs = pa.hdfs.connect(

    host='10.128.0.2', #internal-ip
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

cap = pa.hdfs.HadoopFileSystem.get_capacity(fs)

print(str(round(cap / pow(1024,2), 0)) + ' MB')
#print(cap)



