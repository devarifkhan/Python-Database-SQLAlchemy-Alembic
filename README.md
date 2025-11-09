# Python-Database-SQLAlchemy-Alembic
docker run --name postgresql \
  -e POSTGRES_PASSWORD=testpassword \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_DB=testdb \
  -p 5434:5432 \
  -d postgres:13.4-alpine
