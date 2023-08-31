"""
Handles all database interactions for the file_watcher
"""

from sqlalchemy import (  # type: ignore[attr-defined]
    create_engine,
    Column,
    Integer,
    String, QueuePool
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base  # type: ignore[attr-defined]

from file_watcher.utils import logger

Base = declarative_base()


class Instrument(Base):  # type: ignore[valid-type, misc]
    """
    The base instrument class that reflects on the instrument table in the database
    """
    __tablename__ = "instruments"
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    instrument_name = Column(String)
    latest_run = Column(Integer)

    def __eq__(self, other):
        if isinstance(other, Instrument):
            return (
                self.instrument_name == other.instrument_name
                and self.latest_run == other.latest_run
            )
        return False


class DBUpdater:
    """
    The class responsible for the interacting with the database.
    """
    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/interactive-reduction"
        engine = create_engine(connection_string, poolclass=QueuePool, pool_size=20, pool_pre_ping=True)
        self.session_maker_func = sessionmaker(bind=engine)

    def update_latest_run(self, instrument: str, latest_run: Integer) -> None:
        """
        Update the DB with the new latest run for specified instrument
        :param instrument: The instrument to be updated
        :param latest_run: The run value to be put into the database
        """
        with self.session_maker_func() as session:
            row = session.query(Instrument).filter_by(instrument_name=instrument).first()
            if row is None:
                row = Instrument(instrument_name=instrument, latest_run=latest_run)
            else:
                row.latest_run = latest_run
            session.add(row)
            session.commit()
            logger.info("Latest run %s for %s added to the DB", row.latest_run, row.instrument_name)

    def get_latest_run(self, instrument: str):
        """
        Get the latest run from the DB for specified instrument
        :param instrument: The instrument to get the latest run from
        """
        with self.session_maker_func() as session:
            logger.info("Getting latest run for %s from DB...", instrument)
            row = session.query(Instrument).filter_by(instrument_name=instrument).first()
            if row is None:
                logger.info("No run in the DB for %s", instrument)
                return None
            else:
                latest_run = row.latest_run
                logger.info("Latest run for %s is %s in the DB", instrument, latest_run)
                return latest_run
