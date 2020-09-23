# LittleFS
Deduplication based filesystem using FUSE

# About
Data deduplication is a technique for eliminating duplicate copies of repeating data, by storing a copy of the repeated block only once (Read more: [Data Deplication](https://en.wikipedia.org/wiki/Data_deduplication)). This project aims to create a data-duplication based filesystem with the aid of the [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) interface.    
We partition the file into fixed sized blocks and store the hash computed using SHA-256 in a sqlite database.   
No hash collision has been handled, as chances are of a collision occuring is negligible (Read more: [Birthday attack](https://en.wikipedia.org/wiki/Birthday_attack)).   
**NOTE:**
This project is solely for educational purposes.

# Dependencies:
- [fusepy](https://github.com/fusepy/fusepy)

# Syntax
```
LittleFS.py [-h] [-bs BLOCK_SIZE] [-db DATABASE_FILE] root mount

positional arguments:
  root                  root directory
  mount                 directory to be mounted as the LittleFS

optional arguments:
  -h, --help            show this help message and exit
  -bs BLOCK_SIZE, --block_size BLOCK_SIZE
                        Block size for deduplication check. NOTE that this
                        cannot changed once the filesystem is initialised for
                        the first time
  -db DATABASE_FILE, --database_file DATABASE_FILE
```

# Sample Usage:

```
$ mkdir rootdir/
$ mkdir mountdir/
$ python3 LittleFS.py rootdir/ mountdir/
```
