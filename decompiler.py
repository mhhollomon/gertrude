#!/usr/bin/env python
from argparse import ArgumentParser
import json
from pathlib import Path

import msgpack

from gertrude.table import FieldSpec

class Explorer:
    def __init__(self, db_path : Path) :
        self.db_path = db_path

    def info(self, table_name : str | None = None, index_name : str | None = None) :

        print(self.db_path)
        config = json.loads((self.db_path / "gertrude.conf").read_text())
        print(f"Comment: {config['comment']}")
        print("Tables:")
        p = '  '
        for table in self.db_path.glob("tables/*") :
            print(f"{p}{table.name}")
            print(f"{p}{p}Fields:")
            for field in json.loads((table / "config").read_text()) :
                fieldspec = FieldSpec(*field)
                print(f"{p}{p}{p}{fieldspec}")
                #print(f"{p}{p}{p}{field['name']} ({field['type']})")
            print(f"{p}{p}Indexes:")
            for index in table.glob("index/*") :
                config = json.loads((index / "config").read_text())
                print(f"{p}{p}{p}{index.name} : {config}")

    def table_info(self, table_name : str) :
        table = self.db_path / "tables" / table_name
        print(table)
        print("Fields:")
        for field in json.loads((table / "config").read_text()) :
            fieldspec = FieldSpec(*field)
            print(f"  {fieldspec}")
        print("Indexes:")
        for index in table.glob("index/*") :
            config = json.loads((index / "config").read_text())
            print(f"  {index.name} : {config}")

        print("Data:")
        for f in (table/"data").rglob('*') :
            if f.is_file() :
                print(f)
                data = msgpack.unpackb(f.read_bytes())
                print(f"  {data}")

    def index_info(self, table_name : str, index_name : str) :
        table = self.db_path / "tables" / table_name
        index = table / "index" / index_name

        config = json.loads((index / "config").read_text())
        print(f"config = {config}")
        scanptrs = json.loads((index / "scan").read_text())
        print(f"scanptrs = {scanptrs}")

        root_path = index / "root"
        print("Root Node:")
        with open(root_path, "rb") as f :
            data = msgpack.unpackb(f.read())
            print(data)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("db", type=str)
    parser.add_argument("--index", type=str, default=None)
    parser.add_argument("--table", type=str, default=None)
    args = parser.parse_args()

    db_path = Path(args.db)

    if not db_path.exists() :
        print(f"Database {args.db} does not exist")
        exit(1)

    explorer = Explorer(db_path)

    if args.table is not None :
        explorer.table_info(table_name=args.table)
    elif args.index is not None :
        (table, index) = args.index.split(".")
        explorer.index_info(table_name=table, index_name=index)
    else :
        explorer.info()
