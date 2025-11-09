from sqlalchemy import insert, URL, create_engine
from sqlalchemy.orm import sessionmaker

from lesson_2 import User

"""
INSERT INTO users(telegram_id, full_name, username, language_code, created_at)
VALUES (1, 'Jhon Doe', 'johhny', 'en', '2020-01-01');
"""


class Repo:
    def __init__(self, session):
        self.session = session

    def add_user(self, telegram_id: int, full_name: str, username: str, language_code=None):
        stmt = insert(User).values(
            telegram_id=telegram_id,
            full_name=full_name,
            username=username,
            language_code=language_code
        )
        print(stmt)
        self.session.execute(stmt)
        self.session.commit()




from environs import Env

env = Env()
env.read_env('.env')

url = URL.create(
    drivername="postgresql+psycopg2",
    username=env.str("POSTGRES_USER"),
    password=env.str("POSTGRES_PASSWORD"),
    host=env.str("POSTGRES_HOST"),
    port=env.str("POSTGRES_PORT"),
    database=env.str("POSTGRES_DB"),
)
engine = create_engine(url)
session = sessionmaker(engine)
with session() as session:
    repo = Repo(session)
    repo.add_user(telegram_id=1,
                  full_name='Jhon Doe',
                  username='XXXXXX',
                  language_code="en"
                  )