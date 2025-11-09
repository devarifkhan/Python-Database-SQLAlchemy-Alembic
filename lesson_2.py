from argparse import OPTIONAL
from typing import Optional

from sqlalchemy import BIGINT, func, ForeignKey, Integer
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr, relationship
from sqlalchemy.sql.annotation import Annotated


class Base(DeclarativeBase):
    pass


"""
CREATE TABLE IF NOT EXISTS users
(
    telegram_id
    BIGINT
    PRIMARY
    KEY,
    full_name
    VARCHAR
(
    255
) NOT NULL,
    username VARCHAR
(
    255
),
    language_code VARCHAR
(
    255
) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW
(
),
    referrer_id BIGINT,
    FOREIGN KEY
(
    referrer_id
)
    REFERENCES users
(
    telegram_id
)
    ON DELETE SET NULL
    );

"""


class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls):
        return cls.__name__.lower() + "s"


class TimestampMixin:
    created_at : Mapped[datetime] = mapped_column(
        TIMESTAMP,nullable=False, server_default=func.now()
    )
    updated_at : Mapped[datetime] = mapped_column(
        TIMESTAMP,nullable=False, server_default=func.now()
    )



int_pk = Annotated[
    int,
    mapped_column(
        Integer, primary_key=True
    )
]

user_fk = Annotated[
    int,
    mapped_column(
        BIGINT, ForeignKey("users.telegram_id",ondelete="SET NULL")
    )
]

str_255 = Annotated(str,mapped_column(VARCHAR(255)))

class User(Base,TimestampMixin,TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    full_name: Mapped[str_255]
    username: Optional[Mapped[str_255]]
    language_code: Mapped[str_255]
    referred_id: Optional[Mapped[user_fk]]

"""
create table products
(
    product_id  SERIAL PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    description TEXT,
    created_at  TIMESTAMP DEFAULT NOW()

)
"""

class Product(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Optional[Mapped[str_255]]

"""
create table orders(
order_id SERIAL PRIMARY KEY ,
    user_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (user_id)
    REFERENCES users(telegram_id)
    on DELETE CASCADE 
)
"""

class Order(Base,TimestampMixin, TableNameMixin):
    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]


"""
create table order_products(
order_id integer not null,
    product_id integer not null,
    quantity integer not null,
    FOREIGN KEY (order_id)
    REFERENCES orders(order_id)
    on delete CASCADE ,
    FOREIGN KEY (product_id)
    references products(product_id)
    on delete restrict 
)
"""

class OrderProduct(Base,TableNameMixin):
    order_id: Mapped[int_pk] = mapped_column(
        Integer, ForeignKey("orders.order_id",ondelete="CASCADE"),primary_key=True
    )
    product_id: Mapped[int_pk]= mapped_column(
        Integer, ForeignKey("products.product_id",ondelete="RESTRICT"),primary_key=True
    )
    quantity: Mapped[int]

