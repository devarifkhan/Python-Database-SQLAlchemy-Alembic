import random

from faker.proxy import Faker
from sqlalchemy import insert, URL, create_engine, select, or_, join
from sqlalchemy.orm import sessionmaker, aliased
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
    def select_all_invited_users(self):
        ParentUser = aliased(User)
        ReferralUser = aliased(User)
        stmt = select(ParentUser.full_name.label('parent_name'),
                      ReferralUser.full_name.label('referral_name')).select_from(
            join(ParentUser, ReferralUser, ReferralUser.telegram_id == ParentUser.referrer_id)
        ).where(ParentUser.referrer_id.isnot(None))
        result = self.session.execute(stmt)
        return result.all()


def seed_fake_data(repo):
    # Clear existing data
    from sqlalchemy import text
    repo.session.execute(text('TRUNCATE TABLE users CASCADE'))
    repo.session.commit()
    
    Faker.seed(0)
    fake = Faker()
    users = []
    orders = []
    products = []
    
    # Generate unique telegram_ids
    used_ids = set()
    
    # Create initial users without referrers
    for i in range(5):
        telegram_id = 1000 + i  # Use sequential IDs to avoid duplicates
        user = repo.add_user(
            telegram_id=telegram_id,
            full_name=fake.name(),
            username=fake.user_name(),
            language_code=fake.language_code()
        )
        users.append(user)
        used_ids.add(telegram_id)
    
    # Create users with referrers
    for i in range(5):
        telegram_id = 2000 + i  # Use different range for referred users
        referrer = random.choice(users)
        user_data = {
            'telegram_id': telegram_id,
            'full_name': fake.name(),
            'username': fake.user_name(),
            'language_code': fake.language_code(),
            'referrer_id': referrer.telegram_id
        }
        # Insert user with referrer using raw insert
        from sqlalchemy import insert
        stmt = insert(User).values(**user_data).returning(User)
        result = repo.session.execute(stmt)
        user = result.scalars().first()
        repo.session.commit()
        users.append(user)
        used_ids.add(telegram_id)
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
        seed_fake_data(repo)
        results = repo.select_all_invited_users()
        print(f"Found {len(results)} users with referrers")
        for row in results:
            print(f"Parent: {row.parent_name}, Referral: {row.referral_name}")
