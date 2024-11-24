from sqlalchemy import Column, Integer, String, Float, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Inventory(Base):
    """
    Represents the inventory table for storing file metadata.
    """
    __tablename__ = "inventory"
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String, nullable=False, unique=True)
    path = Column(Text, nullable=False)
    filename = Column(String, nullable=False)
    size_bytes = Column(Float, nullable=False)
    mime_type = Column(String, nullable=True)
    dataset = Column(String, nullable=True)

class TaskQueue(Base):
    """
    Represents the task queue for async processing.
    """
    __tablename__ = "task_queue"
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String, nullable=False)
    task_type = Column(String, nullable=False)
    is_completed = Column(Boolean, default=False)

# Weitere Tabellen kannst du hier hinzufügen.
