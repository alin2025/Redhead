'''
In this exercise we will create local text files, upload them into HDFS and then play with them a little bit.

1. Create several text files in a folder called my_files on your local OS (Do NOT create this folder manually.
 Use Python's os.mkdir()). The content of the files should be simple textual input
 from the user (use input()), e.g. 'my email is abc@gmail.com'.
2. Check if /tmp/sqoop/staging2 already exists in your HDFS, and if not - create it.
3. Upload to staging2 only the files that contain the character @.
4. Delete the entire directory staging2.

'''

