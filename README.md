# gertrude
python database where each row is represented by a file.

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
JSON file with config information for the table. Includes column information
- name
- type
- options

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
A tree node is represented by a file.

The root node is called simply `root` - other nodes are assigned a 20 char nanoid
similar to heap_id.

An internal node is a tuple :
- kind indicator (internal)
- A List of Tuples with :
    - key
    - node name

A leaf node is a tuple :
- kind indicator (leaf)
- A predecessor node name
- A successor node name
- A List of Tuples with :
    - key
    - heap_id of the record

Fan out will be 1000 (probably).

I may set the root to have infinite fan out so that the tree is never more than
2 layers deep. Even at 1000, 2 layers give you 500,000 records (with 50% fill)

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
                    - 23JDEF456RE2
                    - 765SDFPO0912
        - table2
            - ...