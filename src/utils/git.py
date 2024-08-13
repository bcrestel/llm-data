import subprocess


def get_current_git_commit_short() -> str:
    """From ChatGPT

    Returns:
        str: git commit in short format
    """
    try:
        # Run the git command to get the short commit hash
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )

        # Strip any trailing newlines or spaces
        short_commit_hash = result.stdout.strip()
        return short_commit_hash
    except subprocess.CalledProcessError as e:
        print(f"Error retrieving the current Git commit: {e}")
        return None
