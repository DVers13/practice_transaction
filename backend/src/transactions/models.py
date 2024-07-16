from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Client(Base):
    __tablename__ = "client"

    client: Mapped[str] = mapped_column(primary_key=True)
    date_of_birth: Mapped[datetime]
    passport: Mapped[str]
    passport_valid_to: Mapped[str]
    phone: Mapped[str]

class Card(Base):
    __tablename__ = "card"

    card_id: Mapped[int] = mapped_column(primary_key=True)
    client: Mapped[str] = mapped_column(ForeignKey('client.client'))
    card: Mapped[str]

class Terminal(Base):
    __tablename__ = "terminal"

    terminal_id: Mapped[int] = mapped_column(primary_key=True)
    terminal_type: Mapped[str]

class City(Base):
    __tablename__ = "city"

    city_id: Mapped[int] = mapped_column(primary_key=True)
    city: Mapped[str]

class Location(Base):
    __tablename__ = "location"

    location_id: Mapped[int] = mapped_column(primary_key=True)
    city_id: Mapped[int] = mapped_column(ForeignKey('city.city_id'))
    address: Mapped[str]

class Transaction(Base):
    __tablename__ = "transaction"

    id_transaction: Mapped[int] = mapped_column(primary_key=True)
    card_id: Mapped[int] = mapped_column(ForeignKey('card.card_id'))
    terminal_id: Mapped[int] = mapped_column(ForeignKey('terminal.terminal_id'))
    location_id: Mapped[int] = mapped_column(ForeignKey('location.location_id'))
    date: Mapped[datetime]
    operation_type: Mapped[str]
    operation_result: Mapped[str]
    amount: Mapped[str]
    #is_fraud: Mapped[str] # %

class TempTransaction(Base):
    __tablename__ = "temp_transaction"

    id_transaction: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[datetime]
    card: Mapped[str]
    client: Mapped[str]
    date_of_birth: Mapped[datetime]
    passport: Mapped[str]
    passport_valid_to: Mapped[str]
    phone: Mapped[str]
    operation_type: Mapped[str]
    amount: Mapped[str]
    operation_result: Mapped[str]
    terminal_type: Mapped[str]
    city: Mapped[str]
    address: Mapped[str]