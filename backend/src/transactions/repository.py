from datetime import datetime
from fastapi import Depends, UploadFile, File
import csv
from sqlalchemy import select
from database import new_session
from transactions.models import Client, Card, Terminal, City, Location, Transaction
from sqlalchemy.ext.asyncio import AsyncSession

class TransactionRepository:
    @classmethod
    async def upload_csv(cls, file: UploadFile = File(...)) -> int:
        contents = await file.read()

        csv_data = contents.decode('utf-8').splitlines()
        csv_reader = csv.reader(csv_data)
        headers = next(csv_reader)
        c = 0
        async with new_session() as session:
            for row in csv_reader:
                c+= 1
                print(c)
                existing_client = await session.execute(select(Client.client).filter_by(client=row[3]))
                existing_client = existing_client.scalar_one_or_none()
                if not existing_client:
                    client = Client(
                            client=row[3],
                            date_of_birth=datetime.strptime(row[4], "%Y-%m-%d"),
                            passport=row[5],
                            passport_valid_to=row[6],
                            phone=row[7]
                        )
                    session.add(client)
                    await session.flush()
                else:
                    client = existing_client

                existing_card = await session.execute(select(Card).filter_by(card=row[2]))
                existing_card = existing_card.scalar_one_or_none()
                if not existing_card:
                    card = Card(client = row[3], card = row[2])
                    session.add(card)
                    await session.flush()
                else:
                    card = existing_card
                card_id = card.card_id

                existing_terminal = await session.execute(select(Terminal).filter_by(terminal_type=row[11]))
                existing_terminal = existing_terminal.scalar_one_or_none()
                if not existing_terminal:
                    terminal = Terminal(terminal_type = row[11])
                    session.add(terminal)
                    await session.flush()
                else:
                    terminal = existing_terminal
                terminal_id = terminal.terminal_id

                existing_city = await session.execute(select(City).filter_by(city=row[12]))
                existing_city = existing_city.scalar_one_or_none()
                if not existing_city:
                    city = City(city = row[12])
                    session.add(city)
                    await session.flush()
                else:
                    city = existing_city
                city_id = city.city_id


                existing_location = await session.execute(select(Location).filter_by(city_id = city_id, address = row[13]))
                existing_location = existing_location.scalar_one_or_none()
                if not existing_location:
                    location = Location(city_id = city_id, address = row[13])
                    session.add(location)
                    await session.flush()
                else:
                    location = existing_location
                location_id = location.location_id


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