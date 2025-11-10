from sqlalchemy import insert, URL, create_engine, select, or_
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
        stmt = select(User).from_statement(
            insert(User).values(
                telegram_id=telegram_id,
                full_name=full_name,
                username=username,
                language_code=language_code
            ).returning(User).on_conflict_do_update(
                index_elements=[User.telegram_id],
                set=dict(full_name=full_name, username=username)
            )
        )
        result = self.session.scalars(stmt).first()
        self.session.commit()

    def get_user_by_id(self, telegram_id: int) -> User:
        stmt = select(User).where(User.telegram_id == telegram_id)
        result = self.session.execute(stmt)
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User).where(or_(User.language_code == 'en', User.language_code == "uk"),
                                  User.username.ilike("%john%"),
                                  User.telegram_id > 0).order_by(User.created_at.desc()).limit(10)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def get_user_language(self, telegram_id: int) -> str:
        stmt = select(User.language_code).where(User.telegram_id == telegram_id).order_by(User.created_at.desc())
        result = self.session.execute(stmt)
        return result.scalars().first()


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
        users = repo.get_all_users()
        print(users)
