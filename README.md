# Python Database with SQLAlchemy & Alembic

A comprehensive Python project demonstrating database operations using SQLAlchemy ORM and Alembic migrations.

## Setup

### 1. PostgreSQL Database
```bash
docker run --name postgresql \
  -e POSTGRES_PASSWORD=testpassword \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_DB=testdb \
  -p 5434:5432 \
  -d postgres:13.4-alpine
```

### 2. Environment Variables
Create `.env` file:
```
POSTGRES_USER=testuser
POSTGRES_PASSWORD=testpassword
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
POSTGRES_DB=testdb
```

### 3. Install Dependencies
```bash
pip install sqlalchemy psycopg2-binary environs faker alembic
```

## Project Structure

- `lesson_2.py` - Database models and schema definition
- `lesson_3.py` - Complete ORM operations implementation

## Database Models

### User Model
- `telegram_id` (Primary Key)
- `full_name`, `username`, `language_code`
- `referrer_id` (Self-referencing foreign key)
- Timestamps: `created_at`, `updated_at`

### Product Model
- `product_id` (Primary Key)
- `title`, `description`, `price`
- Timestamps

### Order Model
- `order_id` (Primary Key)
- `user_id` (Foreign Key to User)
- Timestamps

### OrderProduct Model (Junction Table)
- `order_id`, `product_id` (Composite Primary Key)
- `quantity`

## Features Implemented

### 1. Basic CRUD Operations
```python
# Create
repo.add_user(telegram_id=123, full_name="John Doe", username="john")

# Read
user = repo.get_user_by_id(123)
users = repo.get_all_users()

# Update
repo.update_user_language(123, "en")

# Delete
repo.delete_user_by_id(123)
```

### 2. Advanced Join Queries
```python
# Users with order count
repo.get_users_with_orders()

# Order details with products (4-table join)
repo.get_order_details_with_products()

# Users with referral count (LEFT JOIN)
repo.get_users_with_referral_count()
```

### 3. Aggregated Queries
```python
# Count operations
repo.get_total_users_count()

# Statistical operations
repo.get_average_order_value()
repo.get_top_products_by_quantity()

# Grouped aggregations
repo.get_user_statistics()  # By language
repo.get_monthly_order_summary()  # By month
```

### 4. Update Operations
```python
# Single field update
repo.update_user_language(user_id, "en")

# Conditional updates
repo.update_product_price(product_id, 99.99)

# Calculated updates
repo.update_order_quantities(order_id, 2.0)  # Double quantities
```

### 5. Delete Operations
```python
# Simple delete
repo.delete_user_by_id(user_id)

# Range-based delete with foreign key handling
repo.delete_products_by_price_range(10.0, 50.0)  # Handles constraints

# Subquery-based delete
repo.delete_empty_orders()  # Orders without products
```

### 6. Bulk Operations
```python
# Bulk insert
users_data = [{"telegram_id": 1, "full_name": "User 1", ...}, ...]
repo.bulk_insert_users(users_data)

# Bulk upsert (PostgreSQL)
repo.bulk_upsert_users(users_data)  # Insert or update on conflict
```

### 7. Complex Relationships
```python
# Self-referencing relationships (referrals)
repo.select_all_invited_users()

# Many-to-many through junction table
repo.add_product_to_order(order_id, product_id, quantity)
```

### 8. Advanced Query Operations
```python
# Conditional fields with CASE statements
repo.get_users_with_conditional_data()  # VIP/Regular/New customer types

# Window functions for rankings and analytics
repo.get_products_with_window_functions()  # Price rankings, running totals

# EXISTS and subqueries
repo.get_users_with_subqueries()  # Users with orders or referrals

# Complex filtering with multiple conditions
repo.get_complex_filtered_data()  # Multi-table filters with HAVING
```

### 9. Advanced Update Operations
```python
# Conditional updates based on subqueries
repo.conditional_update_users()  # Update to 'premium' based on spending

# Updates using joined table data
repo.update_with_joins()  # Price adjustments based on order patterns
```

### 10. Transaction Management
```python
# Safe operations with rollback capability
repo.transfer_order_ownership(from_user_id, to_user_id)

# Batch processing in single transaction
repo.batch_process_orders(order_updates_list)
```

### 11. Raw SQL Integration
```python
# Execute custom SQL queries
repo.execute_raw_sql_query("SELECT * FROM users WHERE...")

# Database statistics and analytics
repo.get_database_statistics()  # Table record counts
```

### 12. Performance Optimized Queries
```python
# Optimized top customers query
repo.get_top_customers_optimized(limit=10)
```

## Key SQLAlchemy Features Demonstrated

### ORM Patterns
- **Declarative Base** with mixins
- **Relationships** (one-to-many, many-to-many, self-referencing)
- **Aliased queries** for complex joins
- **Hybrid properties** and **column annotations**

### Query Techniques
- **Select statements** with filtering, ordering, limiting
- **Join operations** (INNER, LEFT, multiple table joins)
- **Aggregate functions** (COUNT, SUM, AVG)
- **Subqueries** and **window functions**
- **Date functions** (date_trunc for monthly summaries)

### Advanced Operations
- **Upsert operations** (ON CONFLICT DO UPDATE)
- **Bulk operations** for performance
- **Transaction management** with session commits
- **Raw SQL integration** when needed
- **Window functions** (RANK, ROW_NUMBER, running totals)
- **CASE statements** for conditional logic
- **EXISTS and subqueries** for complex filtering
- **Error handling** with rollback mechanisms
- **Performance optimization** techniques

## Running the Project

```bash
# Run the complete demonstration
python lesson_3.py
```

This will:
1. Clear existing data
2. Seed fake data (users, orders, products)
3. Demonstrate all query types
4. Show update/delete operations
5. Perform bulk operations
6. Execute advanced operations
7. Display results for each operation
8. Show performance optimizations
9. Demonstrate transaction management
10. Execute raw SQL queries

## Output Example
```
Found 5 users with referrers
Parent: Mary Smith, Referral: Joseph Brown

--- Advanced Join Queries ---
Users with orders (7):
Heather Snow: 1 orders

--- Aggregated Queries ---
Total users: 10
Average order value: $17560.20

--- Update Operations ---
Updated 1 user language

--- Bulk Operations ---
Bulk inserted 3 users

--- Advanced Operations ---
Users with conditional data (10):
John Doe (en): VIP - $25430.50

Products with rankings (10):
Premium Product: Rank 1, Price $9999.99

Users with orders/referrals (8):
Mary Smith: Orders=True, Referrals=True

--- Advanced Updates ---
Conditionally updated 2 premium users

Database statistics:
users: 13 records
orders: 10 records
products: 13 records

Top customers by spending:
John Doe: $25430.50 (5 orders)
```

## Advanced SQLAlchemy Features Demonstrated

### Query Optimization
- **Window Functions** - RANK(), ROW_NUMBER(), running totals
- **Conditional Logic** - CASE statements for dynamic fields
- **Subqueries & EXISTS** - Complex filtering and existence checks
- **Multiple JOINs** - 4+ table joins with proper aliasing
- **Aggregate Functions** - COUNT, SUM, AVG with GROUP BY/HAVING

### Data Manipulation
- **Conditional Updates** - Updates based on subquery results
- **Bulk Operations** - High-performance batch inserts/updates
- **Upsert Operations** - PostgreSQL ON CONFLICT handling
- **Transaction Safety** - Proper rollback on errors
- **Foreign Key Handling** - Safe deletion with constraint management

### Advanced Patterns
- **Repository Pattern** - Clean separation of concerns
- **Raw SQL Integration** - When ORM isn't sufficient
- **Error Handling** - SQLAlchemy exception management
- **Performance Monitoring** - Query optimization techniques

## Best Practices Implemented

- **Session management** with context managers
- **Error handling** with proper commits/rollbacks
- **Type hints** for better code documentation
- **Modular design** with repository pattern
- **Environment configuration** for database credentials
- **Faker integration** for realistic test data
- **Performance optimization** with bulk operations
- **Transaction safety** with try/catch blocks
- **Query optimization** with proper indexing considerations
- **Code organization** with logical method grouping
- **Foreign key constraint handling** for safe deletions
- **Referential integrity** maintenance during operations
