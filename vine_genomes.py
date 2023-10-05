#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import ndcctools.taskvine as vine
import random
import argparse
import getpass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="vine_genomes.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--name",
        nargs="?",
        type=str,
        help="name to assign to the manager.",
        default=f"vine-genome-{getpass.getuser()}",
    )
    parser.add_argument(
        "--port",
        nargs="?",
        type=int,
        help="port for the manager to listen for connections. If 0, pick any available.",
        default=9123,
    )
    parser.add_argument(
        "--disable-peer-transfers",
        action="store_true",
        help="disable transfers among workers.",
        default=False,
    )
    parser.add_argument(
        "--max-concurrent-transfers",
        nargs="?",
        type=int,
        help="maximum number of concurrent peer transfers",
        default=3,
    )
    args = parser.parse_args()

    m = vine.Manager(port=args.port)
    m.set_name(args.name)

    if args.disable_peer_transfers:
        m.disable_peer_transfers()
    
    if args.max_concurrent_transfers:
        m.tune("worker-source-max-transfers", args.max_concurrent_transfers)

    print("Declaring individuals tasks...")

    tarname = "ALL.chr1.250000.vcf.gz"
    input_tar = m.declare_file(tarname, cache="always")
    individuals = m.declare_file("bin/individuals.py", cache="always")
    columns = m.declare_file("columns.txt", cache="always")

    c = 1
    total = 250000
    n_workers = 2500
    t_per_worker = 100
    domain = range(1, 275000, 100)

    individuals_outputs = []
    individuals_outnames = []

    for i in range(n_workers):
        input_data = f"ALL.chr{c}.250000.vcf"
        start = domain[i]
        stop = domain[i+1] - 1

        outfile = m.declare_temp()
        outname = f"chr{c}n-{start}-{stop}.tar.gz"
        individuals_outputs.append(outfile)
        individuals_outnames.append(outname)

        t = vine.Task(
            command=f"gunzip -f {tarname}; python3 individuals.py {input_data} {c} {start} {stop} {total}",
            inputs={
                individuals: {"remote_name": "individuals.py"},
                input_tar: {"remote_name": f"{tarname}"},
                columns: {"remote_name": "columns.txt"},
            },
            outputs={
                outfile: {"remote_name":f"chr{c}n-{start}-{stop}.tar.gz"},
            },
            cores=1,
        )

        task_id = m.submit(t)
        print(f"submitted task {t.id}: {t.command}")

    print("Waiting for individuals tasks to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.completed():
                print(
                    f"task {t.id} completed with an execution error,  {t.std_output}"
                )
            else:
                print(f"task {t.id} failed with status {t.result}")

    print("Individual tasks complete!")

    print("Declaring individual_merge task")

    individuals_merge = m.declare_file("bin/individuals_merge.py")
    merged_output = m.declare_file(f"chr{c}n.tar.gz")

    t = vine.Task(command=f"python3 individuals_merge.py {c} {' '.join(individuals_outnames)}")
    for f,n in zip(individuals_outputs, individuals_outnames):
        t.add_input(f, n)

    t.add_input(individuals_merge, "individuals_merge.py")
    t.add_output(merged_output, f"chr{c}n.tar.gz")

    task_id = m.submit(t)
    
    print("Waiting for merge task to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful:
                print(f"task {t.id} succeeded with {t.std_output}")
            elif t.completed():
                print(
                    f"task {t.id} completed with an execution error,  {t.std_output}"
                )
            else:
                print(f"task {t.id} failed with status {t.result} {t.std_output}")
    
    print("all tasks complete!")


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
