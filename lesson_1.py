from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

url = URL.create(
    drivername="postgresql+psycopg2",
    username="testuser",
    password="testpassword",
    host="localhost",
    port=5434,
    database="testdb",
)


engine = create_engine(url, echo=True)  # future=True not needed in SQLAlchemy 2.x
SessionLocal = sessionmaker(bind=engine)

# Example: run a simple query
with SessionLocal() as session:
    result = session.execute(text("SELECT 1"))
    print(result.scalar())  # prints 1
