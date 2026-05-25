from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def test_required_project_files_exist():
    required_files = [
        ROOT_DIR / "docker-compose.yml",
        ROOT_DIR / "requirements.txt",
        ROOT_DIR / "run_producer.py",
        ROOT_DIR / "run_consumer.py",
        ROOT_DIR / "run_producer_duplicates.py",
    ]

    for file_path in required_files:
        assert file_path.exists(), f"Missing required file: {file_path}"


def test_required_project_directories_exist():
    required_dirs = [
        ROOT_DIR / "producer",
        ROOT_DIR / "consumer",
        ROOT_DIR / "common",
        ROOT_DIR / "alerts",
        ROOT_DIR / "output",
    ]

    for dir_path in required_dirs:
        assert dir_path.exists(), f"Missing required directory: {dir_path}"