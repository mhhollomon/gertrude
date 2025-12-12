# gertrude
python database using the file system as storage.

I am creating this as an exercise. I would not use it for anything remotely
important.

I may also create a version where the data and indexes are stored in gdbm files.

# Data layout
A gertrude database is a directory

## Top level (Database) level
### gertrude.conf
JSON file with top level configuration. Keys include :

- schema_version - The version of the data layout used.
- gertrude_version - The version of gertrude that created the database.

### tables
A directory that contains a sub directory for each table.

## Table level
### config
JSON file with config information for the table. Not sure yet what this will
contain.

### data
Directory that contains the actual data heap. See below.

### index
Directory that contains subdirectories for each of the indecies. See below.

## data (heap) directory
Each row is represented by a JSON (currently) file. Each row is assigned a 20 
character heap_id using [nanoid](https://github.com/puyuan/py-nanoid) using
the custom alphabet of `123456789ABCDEFGHIJKLMNPQRSTUVWXYZ`

Only upper case letters are used to give a fighting chance this works on case
insensitive file systems (eg windows).

The 20 characters of the heap_id are split into 3 parts (`/XX/YY/[16 chars]`)
- An upper directory name of the first 2 characters
- A lower directory name of the next 2 characters
- A file name of the last 16 characters.

In theory, this means each directory could have 1100+ entries. So, may need to
rethink this.

Note that because of the structure, there is no "primary key" - all indexes are
equivalent in terms of speed (all suck).

### row delete
The file is deleted and the indexes are updated.

### row update
A new hash_id is assigned and all indexes are updated.

## Index subdirectory
The subdirectory is named after the index itself. A B+ Tree structure is maintained.
A tree node is represented by a subdirectory.

If the node is a leaf node, then a file named `leaf` is present and is a JSON
containing a list of list. The inner lists have two entries. The first is the key
and the second is the value. The value is a hash_id.

If the node is an internal node, then a file named `internal` is present and is a
JSON file containing a list of lists. The inner lists have two entries. The first
is a key and the second is a subdirectory. All keys in the noted directory will
be less than the search key.

To ease spliting the root node. The index parent directory will be empty except 
for a directory call `root` which will contain the root of the tree

## Example layout
- my-database
    - gertrude.conf
    - tables
        - table1
            - config
            - data
                - X1
                    - 23
                        - M3IJW1290DEV2APF
                - 29
                    - MJ
                        - 9JI7BB6HW6253D12
            - index
                - my_index
                    - root
                        - internal
                        - 01
                            - leaf
                        - 02
                            - internal
                            - 01
                                - leaf
        - table2
            - ...