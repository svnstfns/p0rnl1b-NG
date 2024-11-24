"""
Worker Routes

Provides a web interface and API endpoints for managing the worker.
"""

from flask import Blueprint, render_template, redirect, url_for
from worker_manager import start_worker, stop_worker
from database.crud.worker_control import fetch_worker_status
from database.session import get_session

worker_blueprint = Blueprint('worker', __name__, template_folder='../templates')


@worker_blueprint.route('/')
def worker_dashboard():
    """
    Display the worker dashboard with its current status and controls.
    """
    session = get_session()
    worker_status = fetch_worker_status(session)
    session.close()

    return render_template('worker.html', worker_status=worker_status)


@worker_blueprint.route('/start', methods=['POST'])
def start_worker_route():
    """
    API to manually start the worker.
    """
    try:
        start_worker()
        return redirect(url_for('worker.worker_dashboard'))
    except Exception as e:
        return render_template('worker.html', worker_status="ERROR", error_message=str(e))


@worker_blueprint.route('/stop', methods=['POST'])
def stop_worker_route():
    """
    API to manually stop the worker.
    """
    try:
        stop_worker()
        return redirect(url_for('worker.worker_dashboard'))
    except Exception as e:
        return render_template('worker.html', worker_status="ERROR", error_message=str(e))
