import argparse
import os
import uuid
from datetime import datetime
from pathlib import Path

import git
from tqdm import tqdm


def get_git_files_metadata(directory, query_str=None):
    """
    Recursively search for git repositories in the given directory and extract metadata
    for all files within them.

    Args:
        directory (str): Root directory to start the search from

    Returns:
        list: List of dictionaries containing metadata for each file in git repositories
    """
    metadata_list = []

    # Convert directory to absolute path
    root_dir = os.path.abspath(directory)

    # Walk through all subdirectories
    for current_dir, dirs, files in tqdm(
        os.walk(root_dir), desc="Processing repositories for metadata"
    ):
        # Check if current directory is a git repository
        if ".git" in dirs:
            try:
                # Initialize repo object
                repo = git.Repo(current_dir)
                repo_name = get_repo_name(repo)

                # Get all files tracked by git
                for item in repo.head.commit.tree.traverse():
                    # Skip if item is not a blob (i.e., not a file)
                    if not isinstance(item, git.Blob):
                        continue

                    # Get file path relative to repo root
                    file_path = Path(item.path)

                    # Extract metadata
                    metadata = {
                        "uuid": str(uuid.uuid4()),
                        "file_name": file_path.name,
                        "file_extension": file_path.suffix,
                        "repo_name": repo_name,
                        "commit_time": get_last_commit_time(repo, item.path),
                        "file_path": str(file_path),
                        "commit_hash": repo.head.commit.hexsha,
                    }
                    if query_str is not None:
                        # check if the file contains the query string
                        with open(os.path.join(current_dir, item.path), "r") as f:
                            file_contents = f.read()
                        if query_str in file_contents:
                            metadata[f"{query_str} present"] = True
                        else:
                            metadata[f"{query_str} present"] = False

                    metadata_list.append(metadata)

            except git.exc.InvalidGitRepositoryError:
                print(f"Warning: Invalid git repository at {current_dir}")
                continue
            except Exception as e:
                print(f"Error processing repository at {current_dir}: {str(e)}")
                continue

    return metadata_list


def get_repo_name(repo):
    """
    Extract repository name from remote URL or directory name.

    Args:
        repo (git.Repo): Repository object

    Returns:
        str: Repository name in format "org/repository"
    """
    try:
        # Try to get remote URL
        remote_url = repo.remotes.origin.url

        # Handle different URL formats
        if remote_url.endswith(".git"):
            remote_url = remote_url[:-4]

        # Extract org/repo from URL
        if "github.com" in remote_url:
            parts = remote_url.split("github.com/")[-1].split("/")
            if len(parts) >= 2:
                return f"{parts[-2]}/{parts[-1]}"
    except:
        pass

    # Fallback to directory name if remote URL parsing fails
    return os.path.basename(repo.working_dir)


def get_last_commit_time(repo, file_path):
    """
    Get the timestamp of the last commit that modified the given file.

    Args:
        repo (git.Repo): Repository object
        file_path (str): Path to file relative to repo root

    Returns:
        float: Unix timestamp of last commit
    """
    try:
        # Get the last commit that modified this file
        for commit in repo.iter_commits(paths=file_path):
            return commit.committed_date

    except Exception as e:
        print(f"Error getting commit time for {file_path}: {str(e)}")
        return None

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, default="triton")
    args = parser.parse_args()
    metadata = get_git_files_metadata(f"{args.dir}", query_str="@triton.jit")
    print(metadata)
