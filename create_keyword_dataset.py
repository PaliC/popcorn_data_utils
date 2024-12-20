import argparse
import ast
import hashlib
import json
import os
import uuid
from collections import defaultdict

import astor

from cleaning_utils import fuzzy_filter, remove_duplicates


def filter_metadata_for_query(metadata, keyword, download_dir):
    dict_entry_name = f"{keyword} present"
    viable_metadata = [entry for entry in metadata if dict_entry_name in entry]
    license_metadata = [entry for entry in metadata if entry["file_name"] == "LICENSE"]
    repo_to_license_hash_map = defaultdict(lambda: [])
    repo_to_file_paths_map = defaultdict(lambda: [])
    for lisence in license_metadata:
        file_text = open(
            f"{download_dir}/{lisence['repo_name']}/{lisence['file_path']}", "r"
        ).read()
        repo_to_license_hash_map[lisence["repo_name"]].append(
            hashlib.sha256(file_text.encode("utf-8")).hexdigest()
        )
        repo_to_file_paths_map[lisence["repo_name"]].append(lisence["file_path"])

    query_metadata = [
        entry for entry in viable_metadata if entry[dict_entry_name] == True
    ]
    for entry in query_metadata:
        entry["license_hash"] = repo_to_license_hash_map["repo_name"]
        entry["licence_paths"] = repo_to_file_paths_map["repo_name"]

    return query_metadata


def extract_triton_functions_from_file(file_path):
    """
    Extract all functions decorated with @triton.jit from a Python file.

    Args:
        file_path (str): Path to the Python file

    Returns:
        list: List of function source code strings that are decorated with @triton.jit
    """

    # if not .py file, return empty list
    if not file_path.endswith(".py"):
        return []
    try:
        with open(file_path, "r") as file:
            tree = ast.parse(file.read())
    except SyntaxError as e:
        return []

    triton_functions = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            has_triton_jit = False
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Attribute):
                    if (
                        decorator.attr == "jit"
                        and isinstance(decorator.value, ast.Name)
                        and decorator.value.id == "triton"
                    ):
                        has_triton_jit = True
                        break

            if has_triton_jit:
                # Get the complete function source code including decorators
                function_source = astor.to_source(node)
                # Add the @triton.jit decorator explicitly since astor might not preserve it
                function_text = "@triton.jit\n" + function_source
                triton_functions.append(function_text)
    return triton_functions


# todo: figure out how to generalize this to none triton queries
def create_triton_dataset_from_metadata(metadata, downloads_dir):
    dataset = []
    # create download dir if it doesn't exist
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
    for entry in metadata:
        triton_functions = extract_triton_functions_from_file(
            f"{downloads_dir}/{entry['repo_name']}/{entry['file_path']}"
        )
        for function in triton_functions:
            # copy over entry
            dataset_entry = entry.copy()
            dataset_entry["uuid"] = str(uuid.uuid4())
            dataset_entry["input"] = function
            dataset.append(dataset_entry)
    return dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_name", type=str, default="triton")
    parser.add_argument("--keyword", type=str, default="@triton.jit")
    args = parser.parse_args()

    metadata_file = f"github_data/{args.query_name}/github_metadata.json"
    metadata_json = json.load(open(metadata_file, "r"))
    downloads_dir = f"github_downloads/{args.query_name}"
    query_dir = f"github_data/{args.query_name}"
    query_metadata_file = f"{query_dir}/query_filtered_metadata.json"
    dataset_file = f"datasets/{args.query_name}/dataset.json"
    dataset_dedup_file = f"datasets/{args.query_name}/dataset_dedup.json"
    dataset_filtered_file = f"datasets/{args.query_name}/dataset_filtered.json"
    if not os.path.exists(query_metadata_file):
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        keyword_filtered_metadata = filter_metadata_for_query(
            metadata, args.keyword, downloads_dir
        )
        json.dump(
            keyword_filtered_metadata,
            open(query_metadata_file, "w"),
        )
    else:
        with open(query_metadata_file, "r") as f:
            keyword_filtered_metadata = json.load(f)
    if not os.path.exists(dataset_file):
        if not os.path.exists(f"datasets/{args.query_name}"):
            os.makedirs(f"datasets/{args.query_name}")

        dataset = create_triton_dataset_from_metadata(
            keyword_filtered_metadata, downloads_dir
        )
        json.dump(dataset, open(dataset_file, "w"))
    else:
        dataset = json.load(open(dataset_file, "r"))
    print(f"Dataset size: {len(dataset)}")
    if not os.path.exists(dataset_dedup_file) or True:
        dataset_dedup = remove_duplicates(dataset)
        json.dump(dataset_dedup, open(dataset_dedup_file, "w"))
    else:
        dataset_dedup = json.load(open(dataset_dedup_file, "r"))
    print(f"Dataset size after dedup: {len(dataset_dedup)}")
    if not os.path.exists(dataset_filtered_file) or True:
        dataset_filtered = fuzzy_filter(dataset_dedup)
        json.dump(dataset_filtered, open(dataset_filtered_file, "w"))
    else:
        dataset_filtered = json.load(open(dataset_filtered_file, "r"))
    print(f"Dataset size after fuzzy filter: {len(dataset_filtered)}")


if __name__ == "__main__":
    main()
