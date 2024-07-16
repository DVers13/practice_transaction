from datetime import datetime, timedelta
from fastapi import UploadFile, File
import csv
from sqlalchemy import select, update
from database import new_session
from transactions.models import Client, Card, Terminal, City, Location, Transaction, TempTransaction
from transactions.schemas import TransactionFraud, Params
class TransactionRepository:

    @classmethod
    async def fill_client_table(cls):
        async with new_session() as session:
            stmt = (
                select(
                    TempTransaction.client,
                    TempTransaction.date_of_birth,
                    TempTransaction.passport,
                    TempTransaction.passport_valid_to,
                    TempTransaction.phone
                ).distinct()
            )
            result = await session.execute(stmt)
            unique_clients = result.fetchall()

            for client_data in unique_clients:
                client_record = Client(
                    client=client_data.client,
                    date_of_birth=client_data.date_of_birth,
                    passport=client_data.passport,
                    passport_valid_to=client_data.passport_valid_to,
                    phone=client_data.phone
                )

                existing_client = await session.execute(
                    select(Client).filter_by(client=client_data.client)
                )
                existing_client = existing_client.scalars().first()

                if not existing_client:
                    session.add(client_record)
                else:
                    update_dict = {
                        'date_of_birth': client_data.date_of_birth,
                        'passport': client_data.passport,
                        'passport_valid_to': client_data.passport_valid_to,
                        'phone': client_data.phone
                    }
                    stmt = (
                        update(Client)
                        .where(Client.client == client_data.client)
                        .values(**update_dict)
                    )
                    await session.execute(stmt)
            await session.commit()
    @classmethod
    async def fill_card_table(cls):
        async with new_session() as session:
            stmt = select(TempTransaction.card, TempTransaction.client).distinct()
            result = await session.execute(stmt)
            unique_cards = result.fetchall()

            for card_data in unique_cards:
                card_record = Card(
                    card=card_data.card,
                    client=card_data.client
                )

                existing_card = await session.execute(
                    select(Card).filter_by(card=card_data.card)
                )
                existing_card = existing_card.scalars().first()

                if not existing_card:
                    session.add(card_record)
            await session.commit()
    @classmethod
    async def fill_terminal_table(cls):
        async with new_session() as session:
            stmt = select(TempTransaction.terminal_type).distinct()
            result = await session.execute(stmt)
            unique_terminals = result.fetchall()

            for terminal_data in unique_terminals:
                terminal_record = Terminal(
                    terminal_type=terminal_data.terminal_type
                )

                existing_terminal = await session.execute(
                    select(Terminal).filter_by(terminal_type=terminal_data.terminal_type)
                )
                existing_terminal = existing_terminal.scalars().first()

                if not existing_terminal:
                    session.add(terminal_record)
            await session.commit()

    @classmethod
    async def fill_city_table(cls):
        async with new_session() as session:
            stmt = select(TempTransaction.city).distinct()
            result = await session.execute(stmt)
            unique_cities = result.fetchall()

            for city_data in unique_cities:
                city_record = City(
                    city=city_data.city
                )

                existing_city = await session.execute(
                    select(City).filter_by(city=city_data.city)
                )
                existing_city = existing_city.scalars().first()

                if not existing_city:
                    session.add(city_record)
            await session.commit()

    @classmethod
    async def fill_location_table(cls):
        async with new_session() as session:
            stmt = select(TempTransaction.city, TempTransaction.address).distinct()
            result = await session.execute(stmt)
            unique_locations = result.fetchall()

            for location_data in unique_locations:
                city_id = await session.execute(
                    select(City.city_id).filter_by(city=location_data.city)
                )
                city_id = city_id.scalars().first()

                location_record = Location(
                    city_id=city_id,
                    address=location_data.address
                )

                existing_location = await session.execute(
                    select(Location).filter_by(city_id=city_id, address=location_data.address)
                )
                existing_location = existing_location.scalars().first()

                if not existing_location:
                    session.add(location_record)
            await session.commit()
            
    @classmethod
    async def fill_transaction_table(cls):
        async with new_session() as session:
            stmt = select(TempTransaction).distinct()
            result = await session.execute(stmt)
            transactions = result.fetchall()

            for transaction_data in transactions:
                card_id = await session.execute(
                    select(Card.card_id).filter_by(card=transaction_data.card)
                )
                card_id = card_id.scalars().first()

                terminal_id = await session.execute(
                    select(Terminal.terminal_id).filter_by(terminal_type=transaction_data.terminal_type)
                )
                terminal_id = terminal_id.scalars().first()

                location_id = await session.execute(
                    select(Location.location_id)
                    .join(City, Location.city_id == City.city_id)
                    .filter(City.city == transaction_data.city, Location.address == transaction_data.address)
                )
                location_id = location_id.scalars().first()

                transaction_record = Transaction(
                    id_transaction=transaction_data.id_transaction,
                    card_id=card_id,
                    terminal_id=terminal_id,
                    location_id=location_id,
                    date=transaction_data.date,
                    operation_type=transaction_data.operation_type,
                    operation_result=transaction_data.operation_result,
                    amount=transaction_data.amount
                )

                existing_transaction = await session.execute(
                    select(Transaction).filter_by(id_transaction=transaction_data.id_transaction)
                )
                existing_transaction = existing_transaction.scalars().first()

                if not existing_transaction:
                    session.add(transaction_record)
            await session.commit()

    @classmethod
    async def upload_csv(cls, file: UploadFile = File(...)):
        contents = await file.read()
        csv_data = contents.decode('utf-8').splitlines()
        csv_reader = csv.reader(csv_data)
        next(csv_reader)
        async with new_session() as session:
            for row in csv_reader:
                id_transaction = int(row[0])
                date = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
                card = row[2]
                client = row[3]
                date_of_birth = datetime.strptime(row[4], "%Y-%m-%d")
                passport = row[5]
                passport_valid_to = row[6]
                phone = row[7]
                operation_type = row[8]
                amount = row[9]
                operation_result = row[10]
                terminal_type = row[11]
                city = row[12]
                address = row[13]

                transaction = TempTransaction(id_transaction = id_transaction,
                                                date = date,
                                                card = card,
                                                client = client,
                                                date_of_birth = date_of_birth,
                                                passport = passport,
                                                passport_valid_to = passport_valid_to,
                                                phone = phone,
                                                operation_type = operation_type,
                                                amount = amount,
                                                operation_result = operation_result,
                                                terminal_type = terminal_type,
                                                city = city,
                                                address = address)

                existing_transaction = await session.execute(select(TempTransaction)
                                                        .filter_by(id_transaction=id_transaction))
                existing_transaction = existing_transaction.scalars().first()
                
                if not existing_transaction:
                    session.add(transaction)
                    await session.flush()
                else:
                    update_dict = {'date': date,
                                   'card': card,
                                   'client': client,
                                   'date_of_birth': date_of_birth,
                                   'passport': passport,
                                   'passport_valid_to': passport_valid_to,
                                   'phone': phone,
                                   'operation_type': operation_type,
                                   'amount': amount,
                                   'operation_result': operation_result,
                                   'terminal_type': terminal_type,
                                   'city': city,
                                   'address': address}
                    stmt = (
                        update(TempTransaction)
                        .where(TempTransaction.id_transaction == id_transaction)
                        .values(**update_dict)
                    )
                    await session.execute(stmt)
            await session.commit()

            await cls.fill_client_table()
            await cls.fill_card_table()
            await cls.fill_terminal_table()
            await cls.fill_city_table()
            await cls.fill_location_table()
            await cls.fill_transaction_table()

        return {"status": "csv file upload and data stored in PostgreSQL"}

        
    @classmethod
    async def run_find_fraud(cls, params: Params):
        params = params.model_dump()
        list_id_transaction = params.pop("list_id_transaction")
        count_time_difference_max = params.pop("count_time_difference_max")
        time_difference_seconds = params.pop("time_difference_seconds")
        time_difference_minutes = params.pop("time_difference_minutes")
        threshold_amount = params.pop("threshold_amount")
        async with new_session() as session:
            if not list_id_transaction:
                query = select(Transaction.id_transaction)
                result = await session.execute(query)
                list_id_transaction = result.scalars().all()
            transaction_fraud = []
            for id_transaction in list_id_transaction:

                is_night = False

                query = (select(City.city, Transaction.date, Transaction.operation_result, Transaction.amount, Transaction.card_id, Transaction.id_transaction).
                        join(Location, Location.location_id == Transaction.location_id).
                        join(City, City.city_id == Location.city_id).
                        where(Transaction.id_transaction == id_transaction))
                result = await session.execute(query)
                transaction_row = result.mappings().first()

                query = (select(Card.client).
                        where(Card.card_id == transaction_row.card_id))
                result = await session.execute(query)
                client = result.scalar_one()
                
                if transaction_row.date.hour >= 0 and transaction_row.date.hour <= 6:
                    is_night = True

                query = (select(City.city, Transaction.date, Transaction.operation_result, Transaction.amount).
                        join(Card, Card.card_id == Transaction.card_id).
                        join(Location, Location.location_id == Transaction.location_id).
                        join(City, City.city_id == Location.city_id).
                        where(Card.client == client, Transaction.date <= transaction_row.date))
                result = await session.execute(query)
                client_transaction = result.mappings().all()

                previous_transaction = None
                time_difference = 0
                count_time_difference = 0
                amount_all = 0
                amount_count = 0
                first_pattern = False # 5 тр в минуту
                second_pattern = False # превышение среднего
                third_pattern = False # смена локации в течении менее 30мин
                
                for ctran in reversed(client_transaction):
                    amount_all += float(ctran.amount)

                    if (transaction_row.date - ctran.date) <= timedelta(minutes=time_difference_minutes) and transaction_row.city != ctran.city and not third_pattern:
                        third_pattern = True
                    
                for ctran in reversed(client_transaction):
                    if previous_transaction is not None:
                        time_difference += ctran.date - previous_transaction.date
                        count_time_difference += 1
                    else:
                        time_difference = ctran.date - ctran.date
                
                    if count_time_difference > count_time_difference_max and time_difference <= timedelta(seconds=time_difference_seconds):
                        first_pattern = True
                        break
                    previous_transaction = ctran
                
                amount_count = len(client_transaction) - 1 if len(client_transaction) > 1 else 0
                if amount_count > 0 and not second_pattern:
                    if float(transaction_row.amount) > (((amount_all - float(transaction_row.amount)) / (amount_count)) * threshold_amount):
                        second_pattern = True
                
                fraud = TransactionFraud(
                    id_transaction = transaction_row.id_transaction,
                    client=client,
                    is_night = is_night,
                    first_pattern=first_pattern,
                    second_pattern=second_pattern,
                    third_pattern=third_pattern
                )
                if fraud.first_pattern or fraud.second_pattern or fraud.third_pattern:
                    transaction_fraud.append(fraud)
            return transaction_fraud
        
    @classmethod
    async def get_client_by_id(cls, client_id: str):
        async with new_session() as session:
            query = (select(Client).
                    where(Client.client == client_id))
            result = await session.execute(query)
            client_row = result.mappings().first()
            return client_row