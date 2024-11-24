# crud/task_queue.py
from sqlalchemy.exc import SQLAlchemyError
from ..session import SessionLocal
from ..models import TaskQueue

def add_task_to_queue(task_type, file_id=None):
    """
    Add a task to the task queue.
    """
    with SessionLocal() as session:
        try:
            task = TaskQueue(task_type=task_type, file_id=file_id)
            session.add(task)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise RuntimeError(f"Error adding task to queue: {e}")

def fetch_pending_task():
    """
    Fetch the next pending task from the task queue.
    """
    with SessionLocal() as session:
        try:
            return session.query(TaskQueue).filter_by(is_completed=False).first()
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error fetching pending task: {e}")
