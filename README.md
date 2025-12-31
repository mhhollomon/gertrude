# gertrude
python database where each row is represented by a file. query language is a
mix of something like `pyspark` and `SQL`

I am creating this as an exercise. I would not use it for anything remotely
important.

# Building
Not currently on pypi or such, so will need to use the following to build
and install.

```sh
$ git clone https://github.com/mhhollomon/gertrude
$ cd gertrude
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -r requirements.txt
$ flit build
# cd to your project
# and activate its virtual environment
$ . .venv/bin/activate
$ pip install /path/to/gertrude/dist/gertrude-0.0.1-py2.py3-none-any.whl
```

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

Operations not allowed in read-only mode
- add_table
- drop_table
- add_index
- drop_index
- insert
- delete

## Tables

### Table creation
```python
from gertrude import cspec
db.add_table(name="my_table", spec=[cspec('col1', 'str', pk=True), cspec('col2', 'int')])
```
#### name
The name of the table. Must match the regular expression :
`^[a-zA-Z_][a-zA-Z0-9_]*$'`

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

### Table deletion
```python
db.drop_table(table_name="my_table")
```
Raises a ValueError if the database is in Read-Only mode or if the table does not exist.

### Index creation
```python
db.add_index(table_name="my_table", index_name="my_index", column="col2")
```

`index_name` must match the same regular expression as table names. It must
be unique to the table.

A given column may only have one index.

### Index Deletion
```python
db.drop_index(table_name="my_table", index_name="my_index")
```

Raises a ValueError if the database is in Read-Only mode or if the table
or index does not exist.

### Automatic Index Creation
If the FieldSpec of a column has either `pk` or `unique` as True, an
index will be automatically created for that column at table creation time.
The "pk" index will have the name `pk_${column_name}`. A "unique" index
will have the name `unq_${column_name}`.

### table
Returns an object of class Table representing the given table.
```python
table_obj = db.table(table_name="my_table")
```

Using the object after the table has been dropped will raise an exception.

```python
db = Database.create("my_db")
db.add_table("my_table")
table_obj = db.table(table_name="my_table")
db.drop_table("my_table")
table_obj.index_list() ## !!! Exception Raised
```


### Table List
Returns a list of table names in the database.
```python
table_names = db.table_list()
```

## Table Object

### index_list
Returns a list of the index names.

### Add Index
c.f. [Database add_index()](#index-creation)

### Drop Index
c.f. [Database drop_index()](#index-deletion)

### Insert
Insert a row as a dictionary or tuple. If done as a tuple, the values must be
in the same order as in the column spec used when creating.

A `ValueError` exception is raised if the database is in Read-only mode or the
table has been dropped.

```python
table = db.add_table("my_table", [cspec("int_col", "int"), cpsec("str_col", "str")])

# insert as dictionary
table.insert({'str_col' : 'Hello', 'int_col' : 5})

# insert as tuple or list
table.insert((6, "Also hello"))

# Keywords
table.insert(int_col=7, str_col="Goodbye")
```

### get_spec
Returns a tuple of FieldSpec namedtuples containing the column specifications.

### Scan
Generator that returns all rows in the table. Each row will be a NamedTuple with
the columns as attributes.

The order will be effectively random.

```python
table = db.add_table("my_table", [cspec("col1", "int")])
table.insert({'col1' : 1})
table.insert({'col1' : 2})

for row in table.scan() :
    print(f"col1 = {row.col1}")
```

### index_scan()
Scan the table in index order rather than table order. Also, a key may be given
as a bound.

```python
from gertrude import KeyBound

table = db.table("my_table")

# Scan all the rows in the table, but in index order.
for r in table.index_scan("my_index")
    ...

# Scan for rows that are less than or equal to the key
for r in table.index_scan("my_index", key=42, op="<=")
    ...
# Scan for rows that are strictly less than the key
for r in table.index_scan("my_index", key=42, op="<")
    ...
# Scan for rows that are greater than or equal to the key
for r in table.index_scan("my_index", key=42, op=">=")
    ...
# Scan for rows that are strictly greater than the key
for r in table.index_scan("my_index", key=42, op=">")
    ...
# Scan for rows that are equal to the key
for r in table.index_scan("my_index", key=42, op="=")
    ...
```

The operator name equivalents `le`, `lt`, `ge`, `gt`, `eq` may also be used.

### delete()
Delete a row using an object. Method returns `True` if a row was deleted.
```python
table = db.add_table("my_table", [cspec("col1", "int")])
table.insert({'col1' : 1})
table.insert({'col1' : 2})

table.delete({'col1' : 1})
```
## Query
Queries always start from the database object.
```python
db.add_table("my_table", [
  cspec("first_name", "str"),
  cspec("last_name", "str"),
  cspec("dept", "str"),
  cspec("salary", "float"),
  cspec("bonus", "float")
])
query = db.query("my_table").filter("dept = 'sales'")\
    .add_column("name", "last_name + ', ' + first_name")\
    .sort("name")\
    .select("name", ("total comp", "salary + bonus"))

data = query.run()
```

The query planner is still very primitive and mostly works through
the query steps as the are given in the query.

However, it will optimize to use an index scan rather than a whole
table scan if the first operation is a filter that only accesses one
column on which there is an index.
```python
db.add_table("my_table", [cspec("id", "int", unique=True), cpsec("name", "str")])
q = db.query("my_table").filter("id > 3")
```
In the above case, the automatically created index on `id` will be used.

It will **not** use the index if there is a compound filter (e.g. "id < 3 and id > 10")
or if some other operation comes before the filter - even if it is another filter.

```python
db.add_table("my_table", [cspec("id", "int", unique=True), cpsec("name", "str")])

# None of these will use the index scan
q = db.query("my_table").filter("id > 3 and id < 10")
q = db.query("my_table").filter("name = 'bob'").filter("id > 3")

```

### filter

A filter is an expression that yields a boolean. Rows are kept if
the filter returns True for the row. c.f. expressions below

### add_column/select
These two shape the output data. Select will only output those quantities
(table columns and computed columns) mentioned. `add_column` keeps all
fields currently in the data and adds the one specified. It can override
an existing column if needed.

the spec for `select` is one or more of the following.
- The name of a column currently in the data
- A tuple consisting of :
    - The name of a column to place in the data.
    - An expression string to compute the value.

### sort
List of one or more keys in the data on which to sort.

### expression

- bare column name
- column name in double quotes. (This can be used to select
  columns that were created by select or add_column that don't
  match the criteria for a column name).
- "+ - * /" normal operator precedence applies. '+' can be used
  with string to concatenate.
- Any of the above in parentheses.
- Comparision operators "=", "<", ">", "<=", "=>", "!="
- logical operators "and", "or"
- "not" operator - Note that this binds rather tightly, so most expression will
  need to use paretheses.
  The operators shortcut.

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
the custom alphabet of `0123456789ABCDEF`

Only upper case letters are used to give a fighting chance this works on case
insensitive file systems (eg windows).

The 20 characters of the heap_id are split into 3 parts (`/XX/YY/[16 chars]`)
- An upper directory name of the first 2 characters
- A lower directory name of the next 2 characters
- A file name of the last 16 characters.

Note that because of the structure, there is no "primary key" - all indexes are
equivalent in terms of speed.

### row delete
The file is deleted and the indexes are updated.

### row update
A new hash_id is assigned and all indexes are updated.

## Index subdirectory
The subdirectory is named after the index itself. A B+ Tree structure is maintained.
A tree node is represented by a file. Each file is given an integer id that that is
also it name.

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
                - 51
                    - 23
                        - E3IFD1290DEA2ACF
                - 2F
                    - 10
                        - 9F07BB6EF6253D12
            - index
                - my_index
                    - 000
                    - 020
                    - 130
        - table2
            - ...

## TODO
- joins
- allow reordering of filters when safe to do so.
- expressions
    - case statement
    - between

- Create a configuration framework.
- Figure out multi-key indexes.
- Honor Fan-out on internal nodes during post insert index creation.
- Handle missing insert values.
- Check typing on insert values.
- Create a way to give defaults for inserts.

## Technologies
- [Flit](https://flit.pypa.io/en/stable/) - Python packaging/building.
- [Nanoid](https://github.com/puyuan/py-nanoid) is used to generate row heap IDs.
- [msgpack](https://github.com/msgpack/msgpack-python/) is used to store data.
- [lark](https://lark-parser.readthedocs.io/en/stable/index.html) for parsing
  query expressions.
- [pytest](https://docs.pytest.org/en/stable/) for automated testing.