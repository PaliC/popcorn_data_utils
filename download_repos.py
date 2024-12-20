import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests
from extract_metadata import get_git_files_metadata
from tqdm import tqdm

IS_TESTING = os.getenv("IS_TESTING", "false") == "true"


def get_github_response(query, token=None, page=1, per_page=100):
    base_url = "https://api.github.com/search/code"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    params = {"q": query, "page": page, "per_page": per_page}
    response = requests.get(base_url, headers=headers, params=params)
    # Handle rate limiting
    if response.status_code == 403:
        reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
        wait_time = max(reset_time - time.time(), 0) + 1  # Add 1 second to be safe
        # print(f"Rate limit exceeded. Waiting {wait_time:.0f} seconds...")
        time.sleep(wait_time)
        return get_github_response(query, token, page, per_page)

    # Handle other errors
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(f"caused by: {query}")
        print(response.json())
        raise Exception(f"Error: {response.status_code}")
    return response.json()


def create_github_metadata(base_query: str, query_dir: str):
    metadata = {}
    cur_uuid = 0
    if not os.path.exists(f"github_data/{query_dir}"):
        raise ValueError(f"github_data/{query_dir} does not exist")


def get_search_queries_from_sizes(
    base_query: str, min_size: int, max_size: int, token: str = None
):

    valid_queries = []

    def do_single_query(query):
        response = get_github_response(query, token)
        return response

    cur_max_size = min_size + 1500
    cur_min_size = min_size
    while True:
        query = (
            f"{base_query} in:file language:python size:{cur_min_size}..{cur_max_size}"
        )
        response = do_single_query(query)
        total_count = response.get("total_count", 0)
        if total_count > 500:
            print(f"Found {total_count} results for {query} which is too many")
            cur_max_size = cur_min_size + ((cur_max_size - cur_min_size) // 2)
            continue
        elif total_count > 0:
            print(f"Found {total_count} results for {query}")
            valid_queries.append(query)
        elif total_count == 0:
            print(f"Found {total_count} results for {query} which is too few")
            print(f"response: {response}")
        if cur_max_size >= max_size:
            break
        cur_min_size = cur_max_size + 1
        cur_max_size = min(cur_max_size * 2, max_size)
    return valid_queries


def search_github_repos(query, token=None):
    """
    Search GitHub repositories for files containing specific string.

    Args:
        search_query (str): The string to search for
        token (str, optional): GitHub personal access token for higher rate limits

    Returns:
        list: List of dictionaries containing repository information
    """

    all_repos = set()  # Use set to avoid duplicates
    page = 1

    while True:
        data = get_github_response(query=query, token=token, page=page)
        # print(f"Found {len(data.get('items', []))} items")
        # print total count
        total_count = data.get("total_count", 0)
        if total_count > 500:
            print(f"total_count: {total_count} for {query}")

        for item in data.get("items", []):
            repo = item["repository"]
            repo_info = {
                "name": repo["full_name"],
                "url": repo["html_url"],
                "description": repo["description"],
                "stars": repo.get("stargazers_count", 0),
                "last_updated": repo.get("updated_at", ""),
                "file_path": item["path"],
                "file_url": item["html_url"],
            }
            all_repos.add(
                tuple(repo_info.items())
            )  # Convert dict to tuple for set storage
        # Check if we've processed all pages
        if len(data.get("items", [])) < 100:
            break

        page += 1

    # Convert back to list of dictionaries
    return [dict(repo_tuple) for repo_tuple in all_repos]


# def search_range_github


def dedup_repos(repos):
    unique_repos = []
    unique_repo_names = set()
    for repo in repos:
        if repo["name"] in unique_repo_names:
            continue
        unique_repos.append(repo)
        unique_repo_names.add(repo["name"])
    return unique_repos


def find_repos(
    base_search_query: str, min_size: int, max_size: int, query_dir: str = "triton"
):
    # Replace with your GitHub token if you have one
    token = os.getenv("GITHUB_TOKEN")
    # call an error if token is not set
    if not token:
        raise ValueError(
            "GITHUB_TOKEN is not set please run `export GITHUB_TOKEN=<your_token>`"
        )
    if not os.path.exists("github_data"):
        os.makedirs("github_data")
    file_folder = f"github_data/{query_dir}"
    if not os.path.exists(file_folder):
        os.makedirs(file_folder)
    # check if triton_queries.json exists
    if os.path.exists(f"{file_folder}/github_queries.json"):
        with open(f"{file_folder}/github_queries.json", "r") as f:
            search_queries = json.load(f)
    else:
        search_queries = get_search_queries_from_sizes(
            base_search_query, min_size, max_size, token
        )
        with open(f"{file_folder}/github_queries.json", "w") as f:
            json.dump(search_queries, f)
    if os.path.exists(f"{file_folder}/github_repos.json"):
        all_repos = json.load(open(f"{file_folder}/github_repos.json", "r"))
    else:
        all_repos = []
        for query in tqdm(search_queries):
            repos = search_github_repos(query, token)
        all_repos.extend(repos)
    print(f"Found {len(all_repos)} repos")
    unique_repos = dedup_repos(all_repos)
    print(f"Found {len(unique_repos)} unique repos")
    with open(f"{file_folder}/github_repos.json", "w") as f:
        json.dump(unique_repos, f)
    return unique_repos


def download_repo(repo, download_dir: str = "triton"):
    org = repo["name"].split("/")[0]
    repo_name = repo["name"].split("/")[1]
    if not os.path.exists(f"{download_dir}/{org}"):
        os.makedirs(f"{download_dir}/{org}")
    if os.path.exists(f"{download_dir}/{org}/{repo_name}"):
        return
    os.system(
        f"git clone --quiet --filter=blob:limit=5m git@github.com:{org}/{repo_name}.git {download_dir}/{org}/{repo_name} &> /dev/null"
    )


def download_repos(repos, query_dir: str = "triton"):
    download_dir = f"github_downloads/{query_dir}"
    if not os.path.exists("github_downloads"):
        os.makedirs("github_downloads")
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    if IS_TESTING:
        repos = repos[:8]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            tqdm(
                executor.map(lambda repo: download_repo(repo, download_dir), repos),
                total=len(repos),
            )
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, default="@triton.jit")
    parser.add_argument("--min_size", type=int, default=0)
    parser.add_argument("--max_size", type=int, default=5000000)
    parser.add_argument("--query_dir", type=str, default="triton")
    args = parser.parse_args()
    repos = find_repos(args.query, args.min_size, args.max_size, args.query_dir)
    download_repos(repos, args.query_dir)
    if not os.path.exists(f"github_data/{args.query_dir}/github_metadata.json"):
        metadata = get_git_files_metadata(
            f"github_downloads/{args.query_dir}", args.query
        )
        print(f"Found {len(metadata)} files")
        with open(f"github_data/{args.query_dir}/github_metadata.json", "w") as f:
            json.dump(metadata, f)


if __name__ == "__main__":
    main()
