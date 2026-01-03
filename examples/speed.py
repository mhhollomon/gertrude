#!/usr/bin/env python

import sys
import os
import time
from datetime import timedelta
import argparse
script_dir = os.path.dirname(os.path.abspath(__file__))
module_dir = os.path.join(script_dir, '..')
sys.path.insert(0, module_dir)

from pathlib import Path
import random
import shutil
import gertrude

import logging
# logging.basicConfig(level=logging.INFO)
# x = logging.getLogger("gertrude.database")
# x.setLevel(logging.DEBUG)
# x.propagate = True
# x = logging.getLogger("gertrude.index")
# x.setLevel(logging.DEBUG)
# x.propagate = True

def build_db(db_path : Path, fanout : int, cache_size : int) -> gertrude.Database :
    if db_path.exists() :
        shutil.rmtree(db_path)

    db = gertrude.Database.create(db_path,
                                  index_fanout=fanout,
                                  index_cache_size=cache_size
                                  )

    table = db.add_table("test", [
        gertrude.FieldSpec("num", 'int', {}),
    ])
    index = table.add_index("test_index", "num")

    return db


def run_test(db : gertrude.Database, size : int) :

    table = db.table("test")
    index = table.index("test_index")


    array = [i for i in range(size)]
    random.shuffle(array)

    build_start_time = time.perf_counter()
    for num in array :
        table.insert({"num" : num})
    build_end_time = time.perf_counter()

    #index.print_tree()

    random.shuffle(array)
    query_start_time = time.perf_counter()
    for num in array :
        data = list(table.index_scan(name="test_index", key=num, op="eq"))
        if not data :
            raise RuntimeError(f"Could not find {num}")
        assert data[0]["num"] == num
    query_end_time = time.perf_counter()

    random.shuffle(array)
    query2_start_time = time.perf_counter()
    for num in array :
        data = list(index.scan(key=num, op="eq"))
        if not data :
            raise RuntimeError(f"Could not find {num}")
    query2_end_time = time.perf_counter()

    print(f"Table/Index Build time: {str(timedelta(seconds=build_end_time - build_start_time))}")
    print(f"Query with data time  : {str(timedelta(seconds=query_end_time - query_start_time))}")
    print(f"Query only index time : {str(timedelta(seconds=query2_end_time - query2_start_time))}")

    print("Cache stats:")
    print(db.cache_stats)

if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100)
    parser.add_argument("--fanout", type=int, default=80)
    parser.add_argument("--cache-size", type=int, default=128)
    args = parser.parse_args()
    db = build_db(Path("./output"), args.fanout, args.cache_size)
    run_test(db, args.size,)