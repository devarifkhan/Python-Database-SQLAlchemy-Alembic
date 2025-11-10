import random

from faker.proxy import Faker
from sqlalchemy import insert, URL, create_engine, select, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lesson_2 import User, Order, Product, OrderProduct

"""
INSERT INTO users(telegram_id, full_name, username, language_code, created_at)
VALUES (1, 'Jhon Doe', 'johhny', 'en', '2020-01-01');
"""


class Repo:
    def __init__(self, session):
        self.session = session

    def add_user(self, telegram_id: int, full_name: str, username: str, language_code=None):
        stmt = select(User).from_statement(
            pg_insert(User).values(
                telegram_id=telegram_id,
                full_name=full_name,
                username=username,
                language_code=language_code
            ).on_conflict_do_update(
                index_elements=[User.telegram_id],
                set_=dict(full_name=full_name, username=username)
            ).returning(User)
        )
        result = self.session.scalars(stmt).first()
        self.session.commit()
        return result

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
    def add_order(self,user_id:int):
        stmt = select(Order).from_statement(
            insert(Order).values(
               user_id=user_id
            ).returning(Order)
        )
        result = self.session.scalars(stmt).first()
        self.session.commit()
        return result

    def add_product(self,title,description,price):
        stmt = select(Product).from_statement(
            insert(Product).values(
                title=title,description=description,price=price
            ).returning(Product)
        )
        result = self.session.scalars(stmt).first()
        self.session.commit()
        return result
    def add_product_to_order(self, order_id, product_id, quantity):
        stmt = select(OrderProduct).from_statement(
            pg_insert(OrderProduct).values(
                order_id=order_id,product_id=product_id,quantity=quantity
            ).on_conflict_do_update(
                index_elements=[OrderProduct.order_id, OrderProduct.product_id],
                set_=dict(quantity=quantity)
            ).returning(OrderProduct)
        )
        result = self.session.scalars(stmt).first()
        self.session.commit()
        return result

def seed_fake_data(repo):
    Faker.seed(0)
    fake = Faker()
    users = []
    orders = []
    products = []

    for _ in range(10):
        user = repo.add_user(
            telegram_id=fake.pyint(),
            full_name=fake.name(),
            username=fake.user_name(),
            language_code=fake.language_code()
        )
        users.append(user)
    for _ in range(10):
        order = repo.add_order(
            user_id=random.choice(users).telegram_id
        )
        orders.append(order)
    for _ in range(10):
        product = repo.add_product(
            title=fake.word(),
            description=fake.sentence(),
            price=fake.pyint()
        )
        products.append(product)
    for _ in range(10):
        repo.add_product_to_order(
            order_id=random.choice(orders).order_id,
            product_id=random.choice(products).product_id,
            quantity=fake.pyint()
        )


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
    session = sessionmaker(engine,expire_on_commit=False)
    with session() as session:
        repo = Repo(session)
        print("Seeding fake data...")
        seed_fake_data(repo)
        print("Data seeded successfully!")
