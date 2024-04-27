import json

import subprocess
import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)


def run_command_from_directory(command, directory):
    # Save the current working directory
    original_directory = os.getcwd()

    try:
        # Change to the specified directory
        os.chdir(directory)

        # Run the command
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        # Print the output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
    finally:
        # Change back to the original directory
        os.chdir(original_directory)

    return result.returncode


with open(os.path.join(script_dir, "graph_info.json"), "r") as file:
    graph_info = json.load(file)

hilbert = graph_info["hilbert"]
hilbert_blocks = graph_info["hilbert_blocks"]
vlgpm_out_path = graph_info["vlgpm path"]

# output_type = "vlgpm"

# Generate VLGPM 
for graph in graph_info["graphs"]:
    out_path = os.path.join(vlgpm_out_path + f"{hilbert_blocks}", graph["store name"])
    params_path = ""
    if "param path" in graph:
        params_path = f'-p {graph["param path"]}'
    command = f' ./bin/importer -s {graph["raw path"]} {params_path} -d {out_path} -b {hilbert_blocks} {"-u" if graph["undirected"] else ""} {"-m" if graph["multiple edges"] else ""} -t {graph["type"]} -i {graph["input type"]} -o vlgpm -h'
    print(command)
    # run_command_from_directory(command, parent_dir)


# Generate KUZU CSV
for graph in graph_info["graphs"]:
    out_path = os.path.join(vlgpm_out_path + "-kuzu-csv", graph["store name"])
    params_path = ""
    if "param path" in graph:
        params_path = f'-p {graph["param path"]}'
    command = f' ./bin/importer -s {graph["raw path"]} {params_path} -d {out_path} {"-u" if graph["undirected"] else ""} {"-m" if graph["multiple edges"] else ""} -t {graph["type"]} -i {graph["input type"]} -o kuzu-csv'
    print(command)
    # run_command_from_directory(command,parent_dir)

# Generate Labelled Graph
for graph in graph_info["graphs"]:
    out_path = os.path.join(vlgpm_out_path + "-label", graph["store name"])
    params_path = ""
    if "param path" in graph:
        params_path = f'-p {graph["param path"]}'
    command = f' ./bin/importer -s {graph["raw path"]} {params_path} -d {out_path} {"-u" if graph["undirected"] else ""} {"-m" if graph["multiple edges"] else ""} -t {graph["type"]} -i {graph["input type"]} -o label'
    print(command)
    # run_command_from_directory(command,parent_dir)
