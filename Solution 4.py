'''
In this exercise we will create local text files, upload them into HDFS and then play with them a little bit.
1. Create several text files in a folder called my_files on your local OS (Do NOT create this folder manually.
 Use Python's os.mkdir()). The content of the files should be simple textual input
 from the user (use input()), e.g. 'my email is abc@gmail.com'.
2. Check if /tmp/sqoop/staging2 already exists in your HDFS, and if not - create it.
3. Upload to staging2 only the files that contain the character @.
4. Delete the entire directory staging2.
'''
import pyarrow as pa
import os

# 1. Create several text files in a folder called my_files on your local OS (Do NOT create this folder manually.
#  Use Python's os.mkdir()). The content of the files should be simple textual input
#  from the user (use input()), e.g. 'my email is abc@gmail.com'.

# os.mkdir('my_files')
# for i in range(5):
#     with open(f'my_files/file{i}.txt', 'w') as f:
#         f.write(input())

# 2. Check if /tmp/sqoop/staging2 already exists in your HDFS, and if not - create it.
#
fs = pa.hdfs.connect(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    driver='libhdfs',
    extra_conf=None)
#
path = '/tmp/sqoop/staging2'
#
# if fs.exists(path):
#     print(f"The directory {path} already exists!")
# else:
#     fs.mkdir(f'hdfs://Cnt7-naya-cdh63:8020{path}',
#              create_parents=True)
#     print(f'The directory {path} was created!')

# 3. Upload to staging2 only the files that contain the character @.

for f_name in os.listdir('my_files'):
    with open(f'my_files/{f_name}') as f:
        text = f.read()
        if '@' in text:
            fs.upload(f'hdfs://Cnt7-naya-cdh63:8020{path}/{f_name}', f)

fs.ls(f'hdfs://Cnt7-naya-cdh63:8020{path}')

fs.delete(f'hdfs://Cnt7-naya-cdh63:8020{path}', recursive=True)
