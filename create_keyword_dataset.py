import argparse
import json

def filter_metadata_for_query(metadata, keyword):
    dict_entry_name = f"{keyword} present"
    viable_metadata = [entry for entry in metadata if dict_entry_name in entry]
    license_metadata = [entry for entry in metadata if entry["file_name"] == "LICENSE"]
    repo_to_license_map = {entry["repo_name"]: entry["license"] for entry in license_metadata}
    query_metadata = [entry for entry in viable_metadata if entry[dict_entry_name] == True]
    for entry in query_metadata:
        entry["license"] = repo_to_license_map.get(entry["repo_name"], "N/A")
    return query_metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_name", type=str, default="triton")
    parser.add_argument("--keyword", type=str, default="@triton.jit")
    args = parser.parse_args()

    metadata_file = f"github_data/{args.query_name}/github_metadata.json"
    downloads_dir = f"github_downloads/{args.query_name}"
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    keyword_filtered_metadata = filter_metadata_for_query(metadata, args.keyword)
    print(len(keyword_filtered_metadata))
    print(len(metadata))


if __name__ == "__main__":
    main()
