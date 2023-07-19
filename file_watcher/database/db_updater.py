from sqlalchemy import (  # type: ignore[attr-defined]
    create_engine,
    Column,
    Integer,
    String, QueuePool,
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base  # type: ignore[attr-defined]

from file_watcher.utils import logger

Base = declarative_base()


class Instrument(Base):  # type: ignore[valid-type, misc]
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
    def __init__(self, ip: str, username: str, password: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{ip}:5432/interactive-reduction"
        engine = create_engine(connection_string, poolclass=QueuePool, pool_size=20, pool_pre_ping=True)
        self.session_maker_func = sessionmaker(bind=engine)

    def update_latest_run(self, instrument: str, latest_run: Integer) -> None:
        with self.session_maker_func() as session:
            row = session.query(Instrument).filter_by(instrument_name=instrument).first()
            if row is None:
                row = Instrument(instrument_name=instrument, latest_run=latest_run)
            else:
                row.latest_run = latest_run
            session.add(row)
            session.commit()
            logger.info(f"Latest run {row.latest_run} for {row.instrument_name} added to the DB")

    def get_latest_run(self, instrument: str):
        with self.session_maker_func() as session:
            logger.info(f"Getting latest run for {instrument} from DB...")
            row = session.query(Instrument).filter_by(instrument_name=instrument).first()
            if row is None:
                logger.info(f"No run in the DB for {instrument}")
                return None
            else:
                latest_run = row.latest_run
                logger.info(f"Latest run for {instrument} is {latest_run} in the DB")
                return latest_run
