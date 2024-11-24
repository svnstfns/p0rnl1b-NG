# Worker-Logik für asynchrone Aufgaben und Queue-Management.
# worker.py
"""
Worker Module

Processes tasks from the queue and interacts with the database to manage task statuses.
"""

import logging
import threading
from database.crud.task_queue import fetch_pending_task, mark_task_completed
from database.crud.inventory import fetch_file_info
from database.crud.duplicates import update_duplicates_table
from database.crud.worker_control import fetch_worker_status
from utils import compute_file_checksum

logger = logging.getLogger(__name__)

stop_threads = threading.Event()

def process_queue(volume_mapping):
    """
    Processes tasks in the queue until stopped.

    Args:
        volume_mapping (dict): Mapping of datasets to volume paths.
    """
    while not stop_threads.is_set():
        worker_status = fetch_worker_status()
        if worker_status != 'RUNNING':
            logger.info("Worker stopped as per control table.")
            stop_threads.wait(5)  # Check again after 5 seconds
            continue

        task = fetch_pending_task()
        if task:
            try:
                logger.info(
                    f"Processing task {task.queue_id} for file {task.file_id} with job {task.task_type}"
                )

                if task.task_type == 'CALC_FILEHASH':
                    file_info = fetch_file_info(task.file_id)
                    if file_info:
                        file_path = file_info.path
                        logger.info(f"Computing checksum for path {file_path}...")
                        file_checksum = compute_file_checksum(file_path)
                        if file_checksum:
                            logger.info(f"Checksum is {file_checksum}")
                            update_duplicates_table(file_checksum, task.file_id, file_info.size_bytes)
                            mark_task_completed(task.queue_id, 'OK')
                        else:
                            logger.error(f"Failed to compute checksum for file at {file_path}.")
                            mark_task_completed(task.queue_id, 'FAIL')
                    else:
                        logger.error(f"File ID {task.file_id} not found in inventory.")
                        mark_task_completed(task.queue_id, 'FAIL')

                elif task.task_type == 'CONV_VIDEO':
                    logger.info("Video conversion tasks not yet implemented.")
                    mark_task_completed(task.queue_id, 'OK')

            except Exception as e:
                logger.error(f"Task {task.queue_id} failed: {e}")
                mark_task_completed(task.queue_id, 'FAIL')
        else:
            stop_threads.wait(1)
