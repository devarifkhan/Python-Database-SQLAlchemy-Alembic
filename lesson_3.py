import random

from faker.proxy import Faker
from sqlalchemy import insert, URL, create_engine, select, or_, join, func, desc
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

    # Advanced Join Queries
    def get_users_with_orders(self):
        """Get users with their order count using JOIN"""
        stmt = select(User.full_name, func.count(Order.order_id).label('order_count')).join(
            Order, User.telegram_id == Order.user_id
        ).group_by(User.telegram_id, User.full_name)
        result = self.session.execute(stmt)
        return result.all()

    def get_order_details_with_products(self):
        """Get order details with product information using multiple JOINs"""
        stmt = select(
            User.full_name.label('customer_name'),
            Order.order_id,
            Product.title.label('product_name'),
            OrderProduct.quantity,
            Product.price
        ).select_from(
            join(join(join(User, Order, User.telegram_id == Order.user_id), 
                      OrderProduct, Order.order_id == OrderProduct.order_id),
                 Product, OrderProduct.product_id == Product.product_id)
        )
        result = self.session.execute(stmt)
        return result.all()

    def get_users_with_referral_count(self):
        """Get users with count of people they referred using LEFT JOIN"""
        Referrer = aliased(User)
        Referred = aliased(User)
        stmt = select(
            Referrer.full_name.label('referrer_name'),
            func.count(Referred.telegram_id).label('referral_count')
        ).select_from(
            Referrer
        ).outerjoin(Referred, Referrer.telegram_id == Referred.referrer_id
        ).group_by(Referrer.telegram_id, Referrer.full_name)
        result = self.session.execute(stmt)
        return result.all()

    # Aggregated Queries
    def get_total_users_count(self):
        """Get total number of users"""
        stmt = select(func.count(User.telegram_id))
        result = self.session.execute(stmt)
        return result.scalar()

    def get_average_order_value(self):
        """Get average order value across all orders"""
        stmt = select(func.avg(Product.price * OrderProduct.quantity)).select_from(
            join(OrderProduct, Product, OrderProduct.product_id == Product.product_id)
        )
        result = self.session.execute(stmt)
        return result.scalar()

    def get_top_products_by_quantity(self, limit=5):
        """Get top products by total quantity ordered"""
        stmt = select(
            Product.title,
            func.sum(OrderProduct.quantity).label('total_quantity')
        ).join(OrderProduct).group_by(
            Product.product_id, Product.title
        ).order_by(desc('total_quantity')).limit(limit)
        result = self.session.execute(stmt)
        return result.all()

    def get_user_statistics(self):
        """Get user statistics by language"""
        stmt = select(
            User.language_code,
            func.count(User.telegram_id).label('user_count'),
            func.count(Order.order_id).label('total_orders')
        ).outerjoin(Order).group_by(User.language_code)
        result = self.session.execute(stmt)
        return result.all()

    def get_monthly_order_summary(self):
        """Get monthly order summary with aggregations"""
        stmt = select(
            func.date_trunc('month', Order.created_at).label('month'),
            func.count(Order.order_id).label('order_count'),
            func.sum(Product.price * OrderProduct.quantity).label('total_revenue')
        ).select_from(
            join(join(Order, OrderProduct, Order.order_id == OrderProduct.order_id),
                 Product, OrderProduct.product_id == Product.product_id)
        ).group_by(func.date_trunc('month', Order.created_at)).order_by('month')
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
            quantity=fake.pyint(min_value=1, max_value=5)
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
        
        # Test referral query
        results = repo.select_all_invited_users()
        print(f"Found {len(results)} users with referrers")
        for row in results:
            print(f"Parent: {row.parent_name}, Referral: {row.referral_name}")
        
        print("\n--- Advanced Join Queries ---")
        
        # Users with order count
        users_orders = repo.get_users_with_orders()
        print(f"\nUsers with orders ({len(users_orders)}):")
        for row in users_orders:
            print(f"{row.full_name}: {row.order_count} orders")
        
        # Order details with products
        order_details = repo.get_order_details_with_products()
        print(f"\nOrder details ({len(order_details)}):")
        for row in order_details[:5]:  # Show first 5
            print(f"{row.customer_name} - Order {row.order_id}: {row.quantity}x {row.product_name} (${row.price})")
        
        # Users with referral count
        referral_counts = repo.get_users_with_referral_count()
        print(f"\nReferral counts ({len(referral_counts)}):")
        for row in referral_counts:
            print(f"{row.referrer_name}: {row.referral_count} referrals")
        
        print("\n--- Aggregated Queries ---")
        
        # Total users
        total_users = repo.get_total_users_count()
        print(f"\nTotal users: {total_users}")
        
        # Average order value
        avg_order_value = repo.get_average_order_value()
        print(f"Average order value: ${avg_order_value:.2f}" if avg_order_value else "No orders")
        
        # Top products
        top_products = repo.get_top_products_by_quantity()
        print(f"\nTop products by quantity:")
        for row in top_products:
            print(f"{row.title}: {row.total_quantity} units")
        
        # User statistics by language
        user_stats = repo.get_user_statistics()
        print(f"\nUser statistics by language:")
        for row in user_stats:
            print(f"{row.language_code}: {row.user_count} users, {row.total_orders} orders")
        
        # Monthly order summary
        monthly_summary = repo.get_monthly_order_summary()
        print(f"\nMonthly order summary:")
        for row in monthly_summary:
            print(f"{row.month.strftime('%Y-%m')}: {row.order_count} orders, ${row.total_revenue:.2f} revenue")
