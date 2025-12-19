#!/usr/bin/env python

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
module_dir = os.path.join(script_dir, '..')
sys.path.insert(0, module_dir)

from argparse import ArgumentParser
from collections import deque
import json
from pathlib import Path
from typing import cast

import msgpack

from gertrude.table import FieldSpec
from gertrude.index import _NODE_TYPE_LEAF, InternalNode, LeafNode

class Explorer:
    def __init__(self, db_path : Path) :
        self.db_path = db_path

    def info(self) :

        print(self.db_path)
        config = json.loads((self.db_path / "gertrude.conf").read_text())
        print(f"Comment: {config['comment']}")
        int_id_path = self.db_path / "int_id"
        if int_id_path.exists() :
            data = msgpack.unpackb(int_id_path.read_bytes())
            print(f"int_id: {data}")
        print("Tables:")
        p = '  '
        for table in self.db_path.glob("tables/*") :
            print(f"{p}{table.name}")
            config = json.loads((table / "config").read_text())
            print(f"{p}{p}id = {config['id']}")
            print(f"{p}{p}Fields:")
            for field in config["spec"] :
                fieldspec = FieldSpec(*field)
                print(f"{p}{p}{p}{fieldspec}")
            print(f"{p}{p}Indexes:")
            for index in table.glob("index/*") :
                config = json.loads((index / "config").read_text())
                print(f"{p}{p}{p}{index.name} : {config}")

    def table_info(self, table_name : str) :
        table = self.db_path / "tables" / table_name
        print(table)
        config = json.loads((table / "config").read_text())
        print(f"id = {config['id']}")
        print("Fields:")
        for field in config["spec"] :
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

        block_list = deque()
        block_list.append(0)
        print("Index Tree:")
        while len(block_list) > 0 :
            block_id = block_list.popleft()
            block_path = index / f"{block_id:03}"
            with open(block_path, "rb") as f :
                raw_data = msgpack.unpackb(f.read())
                if raw_data['k'] == _NODE_TYPE_LEAF :
                    node = LeafNode(**(cast (dict, raw_data)))
                    print(f"  {block_path} ({node.k}, {node.n})")
                    for (key, heap_id) in node.d :
                        print(f"    {key} -> {heap_id}")
                else :
                    node = InternalNode(**(cast (dict, raw_data)))
                    print(f"  {block_path} ({node.k}, {node.n})")
                    for (key, block_id) in node.d :
                        print(f"    {key} -> {block_id}")
                        block_list.append(block_id)

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
