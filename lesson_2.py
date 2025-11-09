from argparse import OPTIONAL
from typing import Optional

from sqlalchemy import BIGINT, func, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr


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

class User(Base,TimestampMixin,TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(
        BIGINT, primary_key=True
    )
    full_name: Mapped[str] = mapped_column(
        VARCHAR(255)
    )
    username: Mapped[Optional[str]] = mapped_column(
        VARCHAR(255), nullable=True
    )
    language_code: Mapped[str] = mapped_column(
        VARCHAR(255), nullable=False
    )
    referrer_id: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        ForeignKey('users.telegram_id',ondelete='SET NULL'),nullable=True

    )
