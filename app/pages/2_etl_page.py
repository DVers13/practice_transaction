import streamlit as st

st.title("ETL - процесс")

st.markdown(
    """ 
### Extract (Извлечение)
1. **Загрузка файла**:
    ```python
    contents = await file.read()
    ```
    Данные загружаются из CSV-файла асинхронным способом.

2. **Чтение содержимого файла**:
    ```python
    csv_data = contents.decode('utf-8').splitlines()
    csv_reader = csv.reader(csv_data)
    headers = next(csv_reader)
    ```
    Данные декодируются из байтов в строковый формат и читаются построчно с помощью `csv.reader`.

### Transform (Трансформация)
1. **Итерирование по строкам CSV-файла**:
    ```python
    for row in csv_reader:
    ```
    Каждая строка файла обрабатывается последовательно.

2. **Проверка и добавление клиентов**:
    ```python
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
    ```
    Проверка существования клиента и добавление нового клиента, если он не существует.

3. **Проверка и добавление карт**:
    ```python
    existing_card = await session.execute(select(Card).filter_by(card=row[2]))
    existing_card = existing_card.scalar_one_or_none()
    if not existing_card:
        card = Card(client=row[3], card=row[2])
        session.add(card)
        await session.flush()
    else:
        card = existing_card
    card_id = card.card_id
    ```
    Проверка существования карты и добавление новой карты, если она не существует.

4. **Проверка и добавление терминалов**:
    ```python
    existing_terminal = await session.execute(select(Terminal).filter_by(terminal_type=row[11]))
    existing_terminal = existing_terminal.scalar_one_or_none()
    if not existing_terminal:
        terminal = Terminal(terminal_type=row[11])
        session.add(terminal)
        await session.flush()
    else:
        terminal = existing_terminal
    terminal_id = terminal.terminal_id
    ```
    Проверка существования терминала и добавление нового терминала, если он не существует.

5. **Проверка и добавление городов**:
    ```python
    existing_city = await session.execute(select(City).filter_by(city=row[12]))
    existing_city = existing_city.scalar_one_or_none()
    if not existing_city:
        city = City(city=row[12])
        session.add(city)
        await session.flush()
    else:
        city = existing_city
    city_id = city.city_id
    ```
    Проверка существования города и добавление нового города, если он не существует.

6. **Проверка и добавление локаций**:
    ```python
    existing_location = await session.execute(select(Location).filter_by(city_id=city_id, address=row[13]))
    existing_location = existing_location.scalar_one_or_none()
    if not existing_location:
        location = Location(city_id=city_id, address=row[13])
        session.add(location)
        await session.flush()
    else:
        location = existing_location
    location_id = location.location_id
    ```
    Проверка существования локации и добавление новой локации, если она не существует.

### Load (Загрузка)
1. **Добавление транзакции**:
    ```python
    transaction = Transaction(
        id_transaction=int(row[0]),
        card_id=card_id,
        terminal_id=terminal_id,
        location_id=location_id,
        date=datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
        operation_type=row[8],
        operation_result=row[10],
        amount=row[9]
    )
    session.add(transaction)
    ```
    Создание нового объекта транзакции и его добавление в сессию.

2. **Коммит транзакции**:
    ```python
    await session.commit()
    ```
    Фиксация всех изменений в базе данных.

### Вывод
После выполнения всего процесса, возвращается сообщение об успешной загрузке и сохранении данных:
```python
return {"status": "csv file upload and data stored in PostgreSQL"}
```
    """)