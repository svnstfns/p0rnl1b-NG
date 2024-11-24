from flask import Blueprint, request, jsonify
from common.logging_config import OriginFilter
from database.crud.inventory import insert_inventory
from database.crud.task_queue import add_task_to_queue

api_blueprint = Blueprint('api', __name__)

@api_blueprint.before_request
def before_request():
    """
    Add dynamic 'origin' for each API request to logs.
    """
    endpoint = request.endpoint if request.endpoint else "UNKNOWN"
    logger.addFilter(OriginFilter(f"API:{endpoint}"))


@api_blueprint.route('/log', methods=['POST'])
def log_message():
    """
    Endpoint to receive logs.
    """
    data = request.json
    level = data.get("level", "DEBUG")
    message = data.get("message", "")

    log_levels = {
        "INFO": logger.info,
        "ERROR": logger.error,
        "DEBUG": logger.debug
    }
    log_func = log_levels.get(level.upper(), logger.debug)
    log_func(message)

    return jsonify({"status": "success"}), 200


@api_blueprint.route('/datasets', methods=['POST'])
def process_datasets():
    """
    Process dataset information.
    """
    data = request.json
    datasets = data.get('datasets', [])

    if not datasets:
        logger.error("No datasets provided.")
        return jsonify({"status": "error", "message": "No datasets provided"}), 400

    try:
        response_data = {"datasets": [{"name": d['dataset']} for d in datasets]}
        return jsonify(response_data), 200
    except Exception as e:
        logger.exception(f"Error processing datasets: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


@api_blueprint.route('/files', methods=['POST'])
def receive_file_info():
    """
    Receive and process file information.
    """
    data = request.json
    required_fields = ["path", "file_id", "filename", "size_bytes", "mime_type", "dataset"]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        logger.error(f"Missing fields: {', '.join(missing_fields)}")
        return jsonify({"status": "error", "message": f"Missing fields: {', '.join(missing_fields)}"}), 400

    try:
        insert_inventory(
            file_id=data['file_id'],
            path=data['path'],
            filename=data['filename'],
            size_bytes=data['size_bytes'],
            mime_type=data['mime_type'],
            dataset=data['dataset']
        )
        logger.info(f"Inserted file {data['file_id']} into inventory.")

        add_task_to_queue(data['file_id'], 'CALC_FILEHASH')
        logger.info(f"Queued hash computation task for file {data['file_id']}.")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.exception(f"Error processing file {data['file_id']}: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


@api_blueprint.route('/update_status', methods=['POST'])
def update_scan_status():
    """
    Update the status of a dataset.
    """
    data = request.json
    dataset = data.get('dataset', {}).get('dataset')
    status = data.get('status', {}).get('status')

    if not dataset or not status:
        logger.error("Invalid input: Dataset or status missing.")
        return jsonify({"status": "error", "message": "Invalid input: Dataset or status missing."}), 400

    logger.info(f"Updated dataset {dataset} to status {status}.")
    return jsonify({"status": "success", "message": "Status updated successfully"}), 200
