from typing import Optional, Annotated          # ✅ use typing.Annotated
from datetime import datetime

from sqlalchemy import BIGINT, Integer, Text, func, ForeignKey, create_engine
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr, relationship

# ---------- Base / Mixins ----------
class Base(DeclarativeBase):
    pass

class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls):
        return cls.__name__.lower() + "s"

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())

# ---------- Column aliases (now correct) ----------
int_pk  = Annotated[int, mapped_column(Integer, primary_key=True)]
str_255 = Annotated[str, mapped_column(VARCHAR(255))]

user_fk_setnull  = Annotated[int, mapped_column(BIGINT, ForeignKey("users.telegram_id", ondelete="SET NULL"))]
user_fk_cascade  = Annotated[int, mapped_column(BIGINT, ForeignKey("users.telegram_id", ondelete="CASCADE"))]

# ---------- Models ----------
class User(Base, TimestampMixin, TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    full_name:   Mapped[str_255]
    username:    Mapped[Optional[str_255]]
    language_code: Mapped[str_255]
    referrer_id: Mapped[Optional[int]] = mapped_column(BIGINT, ForeignKey("users.telegram_id", ondelete="SET NULL"))
    referrer:    Mapped[Optional["User"]] = relationship("User", remote_side=[telegram_id])

class Product(Base, TimestampMixin, TableNameMixin):
    product_id:   Mapped[int_pk]
    title:        Mapped[str_255]
    description:  Mapped[Optional[str]] = mapped_column(Text)   # real TEXT

class Order(Base, TimestampMixin, TableNameMixin):
    order_id: Mapped[int_pk]
    user_id:  Mapped[user_fk_cascade]
    user:     Mapped["User"] = relationship("User", passive_deletes=True)

class OrderProduct(Base, TableNameMixin):
    order_id:   Mapped[int] = mapped_column(Integer, ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(Integer, ForeignKey("products.product_id", ondelete="RESTRICT"), primary_key=True)
    quantity:   Mapped[int]

# ---------- Engine / DDL ----------
url = URL.create(
    drivername="postgresql+psycopg2",
    username="testuser",
    password="testpassword",
    host="localhost",
    port=5434,
    database="testdb",
)
engine = create_engine(url, echo=True)

