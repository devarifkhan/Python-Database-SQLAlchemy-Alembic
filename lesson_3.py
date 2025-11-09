from sqlalchemy import insert, URL, create_engine, select
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
    def get_user_by_id(self,telegram_id:int)-> User:
        stmt = select(User).where(User.telegram_id == telegram_id)
        result = self.session.execute(stmt)
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()



if __name__ == "__main__":
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
        # repo.add_user(telegram_id=1,
        #               full_name='Jhon Doe',
        #               username='XXXXXX',
        #               language_code="en"
        #               )
        user = repo.get_user_by_id(1)
        print(user)
        print(
            f'User:{user.telegram_id} '
            f'Full name:{user.full_name} '
            f'Username:{user.username} '
            f'Language code:{user.language_code}'
        )

