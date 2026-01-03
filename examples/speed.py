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
#logging.basicConfig(level=logging.DEBUG)

def run_test(db_path : Path, size : int) :

    if db_path.exists() :
        shutil.rmtree(db_path)

    db = gertrude.Database.create(db_path)

    table = db.add_table("test", [
        gertrude.FieldSpec("num", 'int', {}),
    ])
    index = table.add_index("test_index", "num")

    array = [i for i in range(size)]
    random.shuffle(array)

    build_start_time = time.perf_counter()
    for num in array :
        table.insert({"num" : num})
    build_end_time = time.perf_counter()

    index.print_tree()

    random.shuffle(array)
    query_start_time = time.perf_counter()
    for num in array :
        data = list(table.index_scan(name="test_index", key=num, op="eq"))
        if not data :
            raise RuntimeError(f"Could not find {num}")
        assert data[0]["num"] == num
    query_end_time = time.perf_counter()

    print(f"Build time: {str(timedelta(seconds=build_end_time - build_start_time))}")
    print(f"Query time: {str(timedelta(seconds=query_end_time - query_start_time))}")

    print("Cache stats:")
    print(db.cache_stats)

if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100)
    args = parser.parse_args()
    run_test(Path("./output"), args.size)