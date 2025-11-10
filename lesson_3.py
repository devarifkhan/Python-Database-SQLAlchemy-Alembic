import random

from faker.proxy import Faker
from sqlalchemy import insert, URL, create_engine, select, or_, join, func, desc, update, delete, and_, case, exists, text
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.exc import SQLAlchemyError
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

    # Update Queries with ORM
    def update_user_language(self, telegram_id: int, new_language: str):
        """Update user language by telegram_id"""
        stmt = update(User).where(User.telegram_id == telegram_id).values(language_code=new_language)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def update_product_price(self, product_id: int, new_price: float):
        """Update product price"""
        stmt = update(Product).where(Product.product_id == product_id).values(price=new_price)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def update_order_quantities(self, order_id: int, quantity_multiplier: float):
        """Update all product quantities in an order"""
        stmt = update(OrderProduct).where(
            OrderProduct.order_id == order_id
        ).values(quantity=OrderProduct.quantity * quantity_multiplier)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    # Delete Queries with ORM
    def delete_user_by_id(self, telegram_id: int):
        """Delete user by telegram_id"""
        stmt = delete(User).where(User.telegram_id == telegram_id)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def delete_products_by_price_range(self, min_price: float, max_price: float):
        """Delete products within price range (handles foreign key constraints)"""
        try:
            # First delete related OrderProduct records
            product_ids_subquery = select(Product.product_id).where(
                and_(Product.price >= min_price, Product.price <= max_price)
            )
            
            delete_orderproducts = delete(OrderProduct).where(
                OrderProduct.product_id.in_(product_ids_subquery)
            )
            self.session.execute(delete_orderproducts)
            
            # Then delete the products
            stmt = delete(Product).where(
                and_(Product.price >= min_price, Product.price <= max_price)
            )
            result = self.session.execute(stmt)
            self.session.commit()
            return result.rowcount
            
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def delete_empty_orders(self):
        """Delete orders with no products"""
        subquery = select(OrderProduct.order_id).distinct()
        stmt = delete(Order).where(~Order.order_id.in_(subquery))
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    # Bulk Insert Operations with ORM
    def bulk_insert_users(self, users_data: list):
        """Bulk insert multiple users"""
        stmt = insert(User).values(users_data)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def bulk_insert_products(self, products_data: list):
        """Bulk insert multiple products"""
        stmt = insert(Product).values(products_data)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def bulk_upsert_users(self, users_data: list):
        """Bulk upsert users (insert or update on conflict)"""
        stmt = pg_insert(User).values(users_data).on_conflict_do_update(
            index_elements=[User.telegram_id],
            set_=dict(
                full_name=pg_insert(User).excluded.full_name,
                username=pg_insert(User).excluded.username,
                language_code=pg_insert(User).excluded.language_code
            )
        )
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    # Advanced Query Operations
    def get_users_with_conditional_data(self):
        """Get users with conditional fields using CASE statements"""
        stmt = select(
            User.full_name,
            User.language_code,
            case(
                (func.count(Order.order_id) > 2, 'VIP'),
                (func.count(Order.order_id) > 0, 'Regular'),
                else_='New'
            ).label('customer_type'),
            func.coalesce(func.sum(Product.price * OrderProduct.quantity), 0).label('total_spent')
        ).outerjoin(Order).outerjoin(OrderProduct).outerjoin(Product
        ).group_by(User.telegram_id, User.full_name, User.language_code)
        result = self.session.execute(stmt)
        return result.all()

    def get_products_with_window_functions(self):
        """Get products with ranking and running totals using window functions"""
        stmt = select(
            Product.title,
            Product.price,
            func.rank().over(order_by=desc(Product.price)).label('price_rank'),
            func.sum(Product.price).over(order_by=Product.product_id).label('running_total'),
            func.avg(Product.price).over().label('avg_price')
        ).order_by(desc(Product.price))
        result = self.session.execute(stmt)
        return result.all()

    def get_users_with_subqueries(self):
        """Get users using EXISTS and subqueries"""
        has_orders = exists().where(Order.user_id == User.telegram_id)
        has_referrals = exists().where(User.referrer_id == User.telegram_id)
        
        stmt = select(
            User.full_name,
            User.language_code,
            has_orders.label('has_orders'),
            has_referrals.label('has_referrals')
        ).where(or_(has_orders, has_referrals))
        result = self.session.execute(stmt)
        return result.all()

    def get_complex_filtered_data(self):
        """Complex filtering with multiple conditions"""
        stmt = select(
            User.full_name,
            func.count(Order.order_id).label('order_count'),
            func.sum(Product.price * OrderProduct.quantity).label('total_value')
        ).join(Order).join(OrderProduct).join(Product).where(
            and_(
                User.language_code.in_(['en', 'fr', 'es']),
                Product.price > 1000,
                OrderProduct.quantity >= 2
            )
        ).group_by(User.telegram_id, User.full_name).having(
            func.count(Order.order_id) > 1
        )
        result = self.session.execute(stmt)
        return result.all()

    # Advanced Update Operations
    def conditional_update_users(self):
        """Update users based on complex conditions"""
        subquery = select(Order.user_id).join(OrderProduct).join(Product).group_by(
            Order.user_id
        ).having(func.sum(Product.price * OrderProduct.quantity) > 10000)
        
        stmt = update(User).where(
            User.telegram_id.in_(subquery)
        ).values(language_code='premium')
        
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    # Transaction Management
    def transfer_order_ownership(self, from_user_id: int, to_user_id: int):
        """Transfer all orders from one user to another with transaction"""
        try:
            from_user = self.session.execute(
                select(User).where(User.telegram_id == from_user_id)
            ).scalar_one_or_none()
            
            to_user = self.session.execute(
                select(User).where(User.telegram_id == to_user_id)
            ).scalar_one_or_none()
            
            if not from_user or not to_user:
                raise ValueError("One or both users not found")
            
            stmt = update(Order).where(
                Order.user_id == from_user_id
            ).values(user_id=to_user_id)
            
            result = self.session.execute(stmt)
            self.session.commit()
            return result.rowcount
            
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    # Raw SQL Operations
    def execute_raw_sql_query(self, sql_query: str):
        """Execute raw SQL for complex operations"""
        result = self.session.execute(text(sql_query))
        return result.fetchall()

    def get_database_statistics(self):
        """Get database statistics using raw SQL"""
        stats_query = """
        SELECT 
            'users' as table_name, COUNT(*) as record_count 
        FROM users
        UNION ALL
        SELECT 
            'orders' as table_name, COUNT(*) as record_count 
        FROM orders
        UNION ALL
        SELECT 
            'products' as table_name, COUNT(*) as record_count 
        FROM products
        UNION ALL
        SELECT 
            'orderproducts' as table_name, COUNT(*) as record_count 
        FROM orderproducts
        """
        return self.execute_raw_sql_query(stats_query)

    def get_top_customers_optimized(self, limit: int = 10):
        """Optimized query for top customers by order value"""
        stmt = select(
            User.full_name,
            User.telegram_id,
            func.sum(Product.price * OrderProduct.quantity).label('total_spent'),
            func.count(func.distinct(Order.order_id)).label('order_count')
        ).join(Order).join(OrderProduct).join(Product
        ).group_by(User.telegram_id, User.full_name
        ).order_by(desc('total_spent')).limit(limit)
        
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
    
    return users, orders, products


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
        users, orders, products = seed_fake_data(repo)
        
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
        
        print("\n--- Update Operations ---")
        
        # Update user language
        updated_count = repo.update_user_language(1000, 'en')
        print(f"Updated {updated_count} user language")
        
        # Update product price
        if products:
            updated_count = repo.update_product_price(products[0].product_id, 99.99)
            print(f"Updated {updated_count} product price")
        
        # Update order quantities
        if orders:
            updated_count = repo.update_order_quantities(orders[0].order_id, 2.0)
            print(f"Updated {updated_count} order quantities")
        
        print("\n--- Delete Operations ---")
        
        # Delete products by price range
        deleted_count = repo.delete_products_by_price_range(0, 1000)
        print(f"Deleted {deleted_count} products in price range")
        
        # Delete empty orders
        deleted_count = repo.delete_empty_orders()
        print(f"Deleted {deleted_count} empty orders")
        
        print("\n--- Bulk Operations ---")
        
        # Bulk insert users
        bulk_users = [
            {'telegram_id': 3001, 'full_name': 'Bulk User 1', 'username': 'bulk1', 'language_code': 'en'},
            {'telegram_id': 3002, 'full_name': 'Bulk User 2', 'username': 'bulk2', 'language_code': 'es'},
            {'telegram_id': 3003, 'full_name': 'Bulk User 3', 'username': 'bulk3', 'language_code': 'fr'}
        ]
        inserted_count = repo.bulk_insert_users(bulk_users)
        print(f"Bulk inserted {inserted_count} users")
        
        # Bulk insert products
        bulk_products = [
            {'title': 'Bulk Product 1', 'description': 'Description 1', 'price': 19.99},
            {'title': 'Bulk Product 2', 'description': 'Description 2', 'price': 29.99},
            {'title': 'Bulk Product 3', 'description': 'Description 3', 'price': 39.99}
        ]
        inserted_count = repo.bulk_insert_products(bulk_products)
        print(f"Bulk inserted {inserted_count} products")
        
        # Bulk upsert users (update existing)
        bulk_users[0]['full_name'] = 'Updated Bulk User 1'
        upserted_count = repo.bulk_upsert_users(bulk_users)
        print(f"Bulk upserted {upserted_count} users")
        
        # Final counts
        final_user_count = repo.get_total_users_count()
        print(f"\nFinal user count: {final_user_count}")
        
        # Clean up - delete a test user
        deleted_count = repo.delete_user_by_id(3001)
        print(f"Deleted {deleted_count} test user")
        
        print("\n--- Advanced Operations ---")
        
        # Conditional data with CASE statements
        conditional_data = repo.get_users_with_conditional_data()
        print(f"\nUsers with conditional data ({len(conditional_data)}):")
        for row in conditional_data[:3]:
            print(f"{row.full_name} ({row.language_code}): {row.customer_type} - ${row.total_spent:.2f}")
        
        # Window functions
        window_data = repo.get_products_with_window_functions()
        print(f"\nProducts with rankings ({len(window_data)}):")
        for row in window_data[:3]:
            print(f"{row.title}: Rank {row.price_rank}, Price ${row.price:.2f}")
        
        # Subqueries and EXISTS
        subquery_data = repo.get_users_with_subqueries()
        print(f"\nUsers with orders/referrals ({len(subquery_data)}):")
        for row in subquery_data[:3]:
            print(f"{row.full_name}: Orders={row.has_orders}, Referrals={row.has_referrals}")
        
        # Complex filtering
        complex_data = repo.get_complex_filtered_data()
        print(f"\nComplex filtered data ({len(complex_data)}):")
        for row in complex_data:
            print(f"{row.full_name}: {row.order_count} orders, ${row.total_value:.2f}")
        
        print("\n--- Advanced Updates ---")
        
        # Conditional updates
        updated_count = repo.conditional_update_users()
        print(f"Conditionally updated {updated_count} premium users")
        
        # Database statistics
        db_stats = repo.get_database_statistics()
        print(f"\nDatabase statistics:")
        for stat in db_stats:
            print(f"{stat[0]}: {stat[1]} records")
        
        # Top customers
        top_customers = repo.get_top_customers_optimized(5)
        print(f"\nTop customers by spending:")
        for customer in top_customers:
            print(f"{customer.full_name}: ${customer.total_spent:.2f} ({customer.order_count} orders)")
