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

engine = create_engine(url, echo=True)
SessionLocal = sessionmaker(bind=engine)

# Create table and insert dummy data
with SessionLocal() as session:
    # ✅ Create table if not exists
    #     session.execute(text("""
    #                          CREATE TABLE IF NOT EXISTS users
    #                          (
    #                              telegram_id
    #                              BIGINT
    #                              PRIMARY
    #                              KEY,
    #                              full_name
    #                              VARCHAR
    #                          (
    #                              255
    #                          ) NOT NULL,
    #                              username VARCHAR
    #                          (
    #                              255
    #                          ),
    #                              language_code VARCHAR
    #                          (
    #                              255
    #                          ) NOT NULL,
    #                              created_at TIMESTAMP DEFAULT NOW
    #                          (
    #                          ),
    #                              referrer_id BIGINT,
    #                              FOREIGN KEY
    #                          (
    #                              referrer_id
    #                          )
    #                              REFERENCES users
    #                          (
    #                              telegram_id
    #                          )
    #                              ON DELETE SET NULL
    #                              );
    #                          """))
    #
    #     # ✅ Insert dummy data
    #     session.execute(text("""
    #                          INSERT INTO users (telegram_id, full_name, username, language_code)
    #                          VALUES (1, 'John Doe', 'johndoe', 'en'),
    #                                 (2, 'Jane Smith', 'janesmith', 'en'),
    #                                 (3, 'Hiro Tanaka', 'hiro_t', 'jp'),
    #                                 (4, 'Carlos Ruiz', 'c_ruiz', 'es'),
    #                                 (5, 'Ayesha Khan', 'ayesha', 'ur') ON CONFLICT (telegram_id) DO NOTHING;
    #                          """))
    #
    #     session.commit()
    #
    # print("✅ Table created and dummy data inserted successfully.")
    result = session.execute(
        text("SELECT * FROM users WHERE telegram_id = :telegram_id"),
        {"telegram_id": 1},
    )
    for row in result.mappings():  # mappings() -> dict-like rows
        print(dict(row))
