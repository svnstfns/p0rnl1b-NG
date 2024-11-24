from flask import Flask
from routes import api_blueprint, worker_blueprint
from database.session import init_db
from worker_manager import check_and_update_worker_state, stop_worker

# Initialize Flask app
app = Flask(__name__)

# Register Blueprints
app.register_blueprint(api_blueprint, url_prefix='/api')
app.register_blueprint(worker_blueprint, url_prefix='/worker')

if __name__ == '__main__':
    logger.info("Initializing database...")
    init_db()

    logger.info("Starting worker manager...")
    check_and_update_worker_state()

    try:
        logger.info("Starting Flask server...")
        app.run(host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        logger.info("Shutting down server...")

    logger.info("Stopping worker...")
    stop_worker()
    logger.info("Server stopped.")
