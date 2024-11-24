#import os
import http.client
import json
import hashlib
import mimetypes
import subprocess  # added import for subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Constants
CHUNK_SIZE = 8192
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_ERROR = "ERROR"
MAX_WORKERS = 16

# Initialize log at the top level to ensure it's available globally
log = None


class Log:
    def __init__(self, endpoint_url):
        """
        Initialize the Log class.
        Args:
            endpoint_url (str): The endpoint URL for sending logs.
        """
        self.endpoint_url = endpoint_url.replace("http://", "").replace("https://", "")

    def send_log_message(self, level, message):
        """
        Sends a log message to the specified endpoint.
        Args:
            level (str): The log level (e.g., "INFO", "ERROR").
            message (str): The log message to send.
        Returns:
            str: The response from the log server.
        """
        protocol = "http://" if "http://" in self.endpoint_url else "https://"
        conn = http.client.HTTPConnection(self.endpoint_url)
        payload = json.dumps({"level": level, "message": message})
        headers = {'Content-Type': 'application/json'}
        conn.request("POST", "/log", payload, headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return data

    def info(self, message):
        """
        Logs an informational message.
        Args:
            message (str): The message to log.
        """
        self.send_log_message(LOG_LEVEL_INFO, message)

    def error(self, message):
        """
        Logs an error message.
        Args:
            message (str): The message to log.
        """
        self.send_log_message(LOG_LEVEL_ERROR, message)


def list_datasets(clusters):
    datasets = []
    for cluster in clusters:
        cluster_path = f"/mnt/{cluster}"
        if not os.path.exists(cluster_path):
            log.error(f"Cluster path {cluster_path} does not exist.")
            continue
        for dataset_name in os.listdir(cluster_path):
            dataset_path = os.path.join(cluster_path, dataset_name)
            if os.path.isdir(dataset_path):
                datasets.append({"cluster": cluster, "dataset": dataset_name, "path": dataset_path})
    return datasets


def send_dataset_info(datasets, endpoint_url):
    """
    Sends dataset information to the specified endpoint and receives the enabled datasets and file extensions.
    Args:
        datasets (list): The list of datasets with their paths.
        endpoint_url (str): The endpoint URL to send dataset information.
    Returns:
        dict: The response from the server containing enabled datasets and file extensions.
    """
    protocol = "http://" if "http://" in endpoint_url else "https://"
    conn = http.client.HTTPConnection(endpoint_url.replace("http://", "").replace("https://", ""))
    payload = json.dumps({"datasets": datasets})
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", "/datasets", payload, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return json.loads(data)


def update_scan_status(dataset, status, endpoint_url):
    """
    Updates the scan status of a dataset on the remote endpoint.
    Args:
        dataset (dict): The dataset information.
        status (dict): The status information containing the last scan timestamp and success flag.
        endpoint_url (str): The endpoint URL to update the status information.
    Returns:
        dict: The response from the server.
    """
    protocol = "http://" if "http://" in endpoint_url else "https://"
    conn = http.client.HTTPConnection(endpoint_url.replace("http://", "").replace("https://", ""))
    payload = json.dumps({"dataset": dataset, "status": status})
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", "/update_status", payload, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return json.loads(data)


def generate_file_id(file_path):
    """
    Generate a unique identifier for a file based on its path and content.
    Args:
        file_path (str): The path to the file.
    Returns:
        str: The SHA-256 hash-based file identifier, or None if the file is missing.
    """
    sha256 = hashlib.sha256()
    sha256.update(file_path.encode('utf-8'))
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                sha256.update(chunk)
    except FileNotFoundError:
        log.error(f"File {file_path} not found for hashing.")
        return None
    return sha256.hexdigest()


def get_file_properties(file_path, dataset):
    """
    Gather metadata for a given file, including its size, MIME type, and extension.
    Args:
        file_path (str): The path to the file.
        dataset (str): The dataset name.
    Returns:
        dict: File metadata including `path`, `file_id`, `filename`, `extension`, `size_bytes`,
              `size_human`, and `mime_type`. Returns None if metadata cannot be collected.
    """
    try:
        file_id = generate_file_id(file_path)
        if file_id is None:
            return None
        file_info = {
            "path": file_path,
            "file_id": file_id,
            "filename": os.path.basename(file_path),
            "extension": os.path.splitext(file_path)[1],
            "size_bytes": os.path.getsize(file_path),
            "size_human": human_readable_size(os.path.getsize(file_path)),
            "mime_type": mimetypes.guess_type(file_path)[0] or "unknown",
            "dataset": str(dataset)
        }
        log.info(f"File properties collected: {file_info}")
        return file_info
    except (OSError, IOError) as e:
        log.error(f"Failed to get properties for file {file_path}: {e}")
        return None


def send_file_info_message(file_info, endpoint_url):
    """
    Sends file information to the specified endpoint.
    Args:
        file_info (dict): The file information to send.
        endpoint_url (str): The endpoint URL for sending data.
    Returns:
        str: The response from the server.
    """
    protocol = "http://" if "http://" in endpoint_url else "https://"
    conn = http.client.HTTPConnection(endpoint_url.replace("http://", "").replace("https://", ""))
    payload = json.dumps(file_info)
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", "/files", payload, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return data


def get_snapshots(dataset_path):
    """
    Retrieves ZFS snapshots for a given dataset.
    Args:
        dataset_path (str): The ZFS dataset path.
    Returns:
        list: A list of snapshot names.
    """
    command = f"zfs list -t snapshot -o name -s creation -r {dataset_path}"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise Exception(f"Error running command {command}: {result.stderr.decode()}")
    output = result.stdout.decode()
    snapshots = output.strip().split('\n')[1:]  # Skip the header line
    return snapshots


def parse_snapshot_timestamp(snapshot_name):
    """
    Parses the timestamp from a snapshot name.
    Args:
        snapshot_name (str): The snapshot name.
    Returns:
        datetime: The parsed timestamp.
    """
    snapshot_timestamp = snapshot_name.split('@')[1].replace("auto-", "")
    return datetime.strptime(snapshot_timestamp, "%Y-%m-%d_%H-%M")


def filter_snapshots_after(snapshots, timestamp):
    """
    Filters snapshots after a given timestamp.
    Args:
        snapshots (list): The list of snapshots.
        timestamp (datetime): The timestamp to filter against.
    Returns:
        list: A list of snapshots after the given timestamp.
    """
    return [s for s in snapshots if parse_snapshot_timestamp(s) > timestamp]


def process_file(file_path, dataset, endpoint_url):
    file_info = get_file_properties(file_path, dataset)
    if file_info:
        send_file_info_message(file_info, endpoint_url)
        return file_info
    return None


def scan_incremental(dataset_path, last_scan_timestamp, endpoint_url, file_extensions, max_workers, dataset_name):
    """
    Perform an incremental scan of the dataset from the nearest snapshot since the last scan.
    Args:
        dataset_path (str): The path to the dataset to be scanned.
        last_scan_timestamp (datetime): The timestamp of the last scan.
        endpoint_url (str): The endpoint URL to send file information messages.
        file_extensions (list): List of allowed file extensions.
        max_workers (int): Number of maximum worker threads for concurrent processing.
        dataset_name (str): The name of the dataset.
    Returns:
        list: A list of file information dictionaries for new or changed files.
    """
    snapshots = get_snapshots(dataset_path)
    if not snapshots:
        return []

    newer_snapshots = filter_snapshots_after(snapshots, last_scan_timestamp)
    if not newer_snapshots:
        nearest_snapshot = min(snapshots, key=lambda s: abs(parse_snapshot_timestamp(s) - last_scan_timestamp))
        newer_snapshots.append(nearest_snapshot)

    file_info_list = []
    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for snapshot in newer_snapshots:
            for root, _, files in os.walk(snapshot):
                for filename in files:
                    if any(filename.endswith(ext) for ext in file_extensions):
                        file_path = os.path.join(root, filename)
                        futures.append(executor.submit(process_file, file_path, dataset_name, endpoint_url))
        for future in as_completed(futures):
            result = future.result()
            if result:
                file_info_list.append(result)
    return file_info_list


def human_readable_size(size_in_bytes):
    """
    Convert a file size in bytes to a human-readable format (e.g., KB, MB).
    Args:
        size_in_bytes (int): File size in bytes.
    Returns:
        str: Human-readable file size (e.g., "4.00 KB").
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024


def scan_full(dataset_path, endpoint_url, file_extensions, max_workers, dataset_name):
    """
    Perform a full scan of the dataset.
    Args:
        dataset_path (str): The path to the dataset to be scanned.
        endpoint_url (str): The endpoint URL to send file information messages.
        file_extensions (list): List of allowed file extensions.
        max_workers (int): Number of maximum worker threads for concurrent processing.
        dataset_name (str): The name of the dataset.
    Returns:
        list: A list of file information dictionaries for all files in the dataset.
    """
    file_info_list = []
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for root, _, files in os.walk(dataset_path):
            for filename in files:
                if any(filename.endswith(ext) for ext in file_extensions):
                    file_path = os.path.join(root, filename)
                    futures.append(executor.submit(process_file, file_path, dataset_name, endpoint_url))
        for future in as_completed(futures):
            result = future.result()
            if result:
                file_info_list.append(result)
    return file_info_list


def scan_datasets(datasets, endpoint_url, file_extensions):
    """
    Scan datasets based on the provided configuration, scan type, and optional dataset name.
    Args:
        datasets (list): List of datasets to scan.
        endpoint_url (str): The endpoint URL to send file information messages.
        file_extensions (list): List of allowed file extensions.
    Returns:
        None
    """
    current_time = datetime.now()
    for dataset in datasets:
        if not dataset.get('enabled'):
            log.error(f"Dataset {dataset['dataset']} is not enabled.")
            continue
        dataset_path = dataset['path']
        log.info(f"Processing dataset: {dataset_path}")
        try:
            # Determine the type of scan
            if not dataset.get("last_scan") or dataset.get("status") != 'SUCCESS':
                log.info(f"Performing a full scan on dataset: {dataset['dataset']}")
                scan_full(dataset_path, endpoint_url, file_extensions, MAX_WORKERS, dataset['dataset'])
                dataset['scan_type'] = 'full'
            else:
                last_scan_timestamp = datetime.strptime(dataset['last_scan'], "%Y-%m-%d_%H-%M")
                log.info(f"Performing an incremental scan on dataset: {dataset['dataset']}")
                scan_incremental(dataset_path, last_scan_timestamp, endpoint_url, file_extensions, MAX_WORKERS,
                                 dataset['dataset'])
                dataset['scan_type'] = 'incremental'

            dataset['last_scan'] = current_time.strftime("%Y-%m-%d_%H-%M")
            dataset['status'] = 'SUCCESS'
        except Exception as e:
            log.error(f"Error processing dataset {dataset['dataset']}: {e}")
            dataset['status'] = 'FAILURE'

        # Update the status on the remote endpoint
        update_scan_status(dataset, {'last_scan': dataset['last_scan'], 'status': dataset['status'],
                                     'scan_type': dataset['scan_type']}, endpoint_url)


def main():
    global log  # Declare log as global to modify it within main
    endpoint_url = "http://housecat02.pvnkn3t.local:5001"
    log = Log(endpoint_url)
    log.info("Logging initialized successfully.")

    clusters = ["cluster-01", "cluster-02"]
    datasets = list_datasets(clusters)

    if not datasets:
        log.error("No datasets found.")
        return

    response = send_dataset_info(datasets, endpoint_url)
    if not response or "enabled_datasets" not in response or "file_extensions" not in response:
        log.error("Failed to retrieve enabled datasets or file extensions.")
        return

    enabled_datasets = response["enabled_datasets"]
    file_extensions = response["file_extensions"]

    log.info("Fetched enabled datasets and file extensions from the remote endpoint.")

    log.info("Starting the scan.")
    scan_datasets(enabled_datasets, endpoint_url, file_extensions)
    log.info("Scan completed.")


if __name__ == "__main__":
    main()
