from datetime import datetime, timedelta
from fastapi import UploadFile, File
import csv
from sqlalchemy import select, update
from database import new_session
from transactions.models import Client, Card, Terminal, City, Location, Transaction
from transactions.schemas import TransactionFraud, Params
class TransactionRepository:
    
    @classmethod
    async def upload_csv(cls, file: UploadFile = File(...)):
        contents = await file.read()
        csv_data = contents.decode('utf-8').splitlines()
        csv_reader = csv.reader(csv_data)
        headers = next(csv_reader)
        async with new_session() as session:
            for row in csv_reader:
                existing_client = await session.execute(select(Client.client, Client.date_of_birth, Client.passport_valid_to, Client.phone).filter_by(client=row[3]))
                existing_client = existing_client.mappings().first()
                add_client = False if existing_client else True
                update_client = False
                date_of_birth = datetime.strptime(row[4], "%Y-%m-%d")
                passport_valid_to = row[6]
                phone=row[7]

                if not add_client:
                    if existing_client.date_of_birth != date_of_birth:
                        date_of_birth = existing_client.date_of_birth
                        update_client = True
                    if existing_client.phone != phone:
                        phone = existing_client.phone
                        update_client = True
                    if existing_client.passport_valid_to != 'бессрочно':
                        if passport_valid_to < existing_client.passport_valid_to:
                            passport_valid_to = existing_client.passport_valid_to
                            update_client = True

                client = Client(
                            client=row[3],
                            date_of_birth=date_of_birth,
                            passport=row[5],
                            passport_valid_to=passport_valid_to,
                            phone=phone
                        )
                
                if add_client:
                    session.add(client)
                    await session.flush()
                elif update_client:
                    update_dict = {'client': client.client,
                                   'date_of_birth':client.date_of_birth,
                                   'passport':client.passport,
                                   'passport_valid_to':client.passport_valid_to,
                                   'phone':client.phone}
                    stmt = (
                        update(Client)
                        .where(Client.client == row[3])
                        .values(**update_dict)
                    )
                    await session.execute(stmt)

                existing_card = await session.execute(select(Card.card_id).filter_by(card=row[2]))
                existing_card = existing_card.scalar_one_or_none()
                if not existing_card:
                    card = Card(client = row[3], card = row[2])
                    session.add(card)
                    await session.flush()
                    card_id = card.card_id
                else:
                    card_id = existing_card

                existing_terminal = await session.execute(select(Terminal.terminal_id).filter_by(terminal_type=row[11]))
                existing_terminal = existing_terminal.scalar_one_or_none()
                if not existing_terminal:
                    terminal = Terminal(terminal_type = row[11])
                    session.add(terminal)
                    await session.flush()
                    terminal_id = terminal.terminal_id
                else:
                    terminal_id = existing_terminal

                existing_city = await session.execute(select(City.city_id).filter_by(city=row[12]))
                existing_city = existing_city.scalar_one_or_none()
                if not existing_city:
                    city = City(city = row[12])
                    session.add(city)
                    await session.flush()
                    city_id = city.city_id
                else:
                    city_id = existing_city


                existing_location = await session.execute(select(Location.location_id).filter_by(city_id = city_id, address = row[13]))
                existing_location = existing_location.scalar_one_or_none()
                if not existing_location:
                    location = Location(city_id = city_id, address = row[13])
                    session.add(location)
                    await session.flush()
                    location_id = location.location_id
                else:
                    location_id = existing_location

                existing_transaction = await session.execute(select(Transaction.id_transaction).filter_by(id_transaction = int(row[0])))
                existing_transaction = existing_transaction.scalar_one_or_none()
                if not existing_transaction:
                    transaction = Transaction(
                        id_transaction = int(row[0]),
                        card_id = card_id,
                        terminal_id = terminal_id,
                        location_id = location_id,
                        date = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                        operation_type = row[8],
                        operation_result = row[10],
                        amount = row[9]
                    )
                    session.add(transaction)

            await session.commit()
            return {"status": "csv file upload and data stored in PostgreSQL"}
        
    @classmethod
    async def run_find_fraud(cls, params: Params):
        params = params.model_dump()
        list_id_transaction = params.pop("list_id_transaction")
        count_time_difference_max = params.pop("count_time_difference_max")
        time_difference_seconds = params.pop("time_difference_seconds")
        time_difference_minutes = params.pop("time_difference_minutes")
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
                threshold_amount = 4
                first_pattern = False # 5 тр в минуту
                second_pattern = False # превышение среднего
                third_pattern = False # смена локации в течении менее 30мин
                
                for ctran in client_transaction:
                    if previous_transaction is not None and not first_pattern:
                        time_difference += ctran.date - previous_transaction.date
                        count_time_difference += 1
                    else:
                        time_difference = ctran.date - ctran.date
                        count_time_difference += 1

                    if count_time_difference > count_time_difference_max and time_difference <= timedelta(seconds=time_difference_seconds):
                        first_pattern = True

                    amount_all += float(ctran.amount)

                    if (transaction_row.date - ctran.date) <= timedelta(minutes=time_difference_minutes) and transaction_row.city != ctran.city and not third_pattern:
                        third_pattern = True
                    
                    previous_transaction = ctran
                
                amount_count = len(client_transaction) - 1 if len(client_transaction) > 1 else 0
                if amount_count > 0 and not second_pattern:
                    if float(transaction_row.amount) > (((amount_all - float(transaction_row.amount)) / (amount_count)) * threshold_amount):
                        second_pattern = True
                
                fraud = TransactionFraud(
                    id_transaction = transaction_row.id_transaction,
                    client=client,
                    first_pattern=first_pattern,
                    second_pattern=second_pattern,
                    third_pattern=third_pattern
                )
                if fraud.first_pattern or fraud.second_pattern or fraud.third_pattern:
                    transaction_fraud.append(fraud)
            return transaction_fraud
                