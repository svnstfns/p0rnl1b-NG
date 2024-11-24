# crud/inventory.py
from sqlalchemy.exc import SQLAlchemyError
from ..session import SessionLocal
from ..models import Inventory

def insert_inventory(file_id, path, filename, size_bytes, mime_type, dataset=None):
    """
    Insert a new record into the inventory table.
    """
    with SessionLocal() as session:
        try:
            new_record = Inventory(
                file_id=file_id,
                path=path,
                filename=filename,
                size_bytes=size_bytes,
                mime_type=mime_type,
                dataset=dataset,
            )
            session.add(new_record)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise RuntimeError(f"Error inserting inventory: {e}")

def fetch_file_info(file_id):
    """
    Fetch information about a file from the inventory table.
    """
    with SessionLocal() as session:
        try:
            return session.query(Inventory).filter_by(file_id=file_id).first()
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error fetching file info: {e}")
