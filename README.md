# gertrude
python database where each row is represented by a file.

I am creating this as an exercise. I would not use it for anything remotely
important.

# API
## Database 

### creation
```python
import gertrude
db = gertrude.Database.create(db_path='/path/to/my_db', comment='This will be an awesome db')
```
#### db_path
This can be either a string or a `pathlib.Path`.

This is the directory where you want the database to be created. If the directory
does not exist, it will be created. If it **does** exist, it must be empty.

#### comment
An optional string describing the database.

### Opening an existing database
```python
import gertrude
db = gertrude.Database.open(db_path='/path/to/my_db', mode='rw')
```
#### db_path
This can be either a string or a `pathlib.Path`.

This is the directory that contins the `gertrude` database you wish to open.

#### mode
One of the string `'rw'` (for read/write) or `'ro'` (for read-only).

## Tables

### Table creation
```python
from gertrude import cspec
db.add_table(name="my_table", spec=[cspec('col1', 'str', pk=True), cspec('col2', 'int')])
```
#### name
The name of the table. Must match the regular expression :
`^[a-zA-Z0-9_-]+$'`

#### spec
This is the column specifications.

Each column is specified by the fields :
- The column name - Must match the same regular expression as table names.
- The column type - Currently only 'str', 'int', 'bool', and 'float' are supported.
  Note that the types are strings not the actual type class.
- Options. The current options are :
    - unique (bool, False) - Only a single row may contain any particular value. An
      index will automatically be created to enforce this constraint.
    - nullable (bool, True) - The field may be null.
    - pk (bool, False) - 'Primary Key' - This is the same as the combination of 
      `unique=True, nullable=False`

### Index creation
```python
db.add_index(table_name="my_table", index_name="my-index", column="col2")
```

`index_name` must match the same regular expression as table names. It must
be unique to the table.

A given column may only have one index.

# Data layout
A gertrude database is a directory.

## Top level (Database) level
### gertrude.conf
JSON file with top level configuration. Keys include :

- schema_version - The version of the data layout used.
- gertrude_version - The version of gertrude that created the database.
- comment.

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
Directory that contains subdirectories for each of the indexes. See below.

## data (heap) directory
Each row is represented by a msgpack file. Each row is assigned a 20
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
A tree node is represented by a file. Each file is given an integer id that that is
also it namel

The root node is `000`.

An internal node is a dataclass :
- kind indicator (I)
- The node id (an integer)
- A List of Tuples with :
    - key
    - descendent node id

A leaf node is a dataclase :
- kind indicator (L)
- The node id
- A List of Tuples with :
    - key
    - heap_id of the record.

Index nodes are managed by an LRU Cache that is shared across all indexes
in the database.

Fan out will be 1000 (probably).

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
                    - 000
                    - 020
                    - 130
        - table2
            - ...

## TODO
- Make mode 'ro' actually do something.
- Start building a query engine.
- Create a configuration framework.
- Figure out multi-key indexes.
- Honor Fan-out on internal nodes during post insert index creation.
- Fix delete table
    - unregister indexes from cache