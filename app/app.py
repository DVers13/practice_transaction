import streamlit as st

st.set_page_config(page_title = "This is a WebApp for summer practice") 
st.title("Подозрительный кейс: Разработайте алгоритм для определения мошеннических банковских операций")

st.markdown(
    """
    ## Контекст

    В банковской сфере Фрод (Fraud) — проведение мошеннических (неправомерных) операций с 
    использованием банковских карт без ведома их владельцев. В таких случаях убытки несёт и покупатель, и 
    продавец, так как теряет товары, купленные фродером и сумму за комиссию при переводе платежа. 
    Дополнительно, продавцу придется платить комиссию за чарджбек (возврат денег) и компенсировать 
    расходы на расследование. Согласно № 161-ФЗ «О национальной платежной системе» при соблюдении 
    ряда условий банк обязан компенсировать (возместить) клиенту понесенный ущерб.

    ## Как обнаружить фрод?

    Выявление мошеннических транзакций возможно путем анализа сохранённых записей по этим 
    транзакциям, а именно поиск отклонений от нормы.

    Пример признаков фрода:

    - очевидно шаблонное поведение (например, равные временные промежутки между кликами);
    - много кликов с одного IP/ID;
    - подозрительная активность в ночные часы;
    - счет получателя находится в «мошеннической» базе Центробанка.

    ## Постановка задачи

    **Цель**: Используя полученный csv - файл с синтетическими транзакциями – выявить фрод-паттерны (1) и 
    реализовать алгоритм обработки записей (2) для выявления и отображения мошеннических операций.

    **Задачи:**

    1. Выделить не менее 3 fraud-паттернов с их подробным описанием.
    2. Подготовить блок-схему ETL-процесса с описанием функциональных компонентов,
    этапов преобразования данных и использованных технических средств (т.е. включающую техстек).
    3. Реализовать автоматизированную систему, согласно заявленному в п.2 ETL-процессу.

    Важно: Для оценки риска и выявления фрода можно использовать как алгоритмические методы (выявлять 
    паттерны аналитически, с помощью скрипта), так и при помощи ML.

   **Ключевые требования к системе:**

    - Возможность обрабатывать csv-файлы, поступающие на вход ❎
    - К каждому полученному файлу применять правила определения мошеннических операций на основе выявленных паттернов ❎
    - Предварительная загрузка данных из csv-файла в БД ✅
    - Отображать список добавляемых транзакций в виде периодически обновляемого дашборда (на выбор -
    веб-приложение или графическая библиотека) с выделением подозрительных операций; преимуществом 
    может стать сбор доп.аналитики по входящим данным (агрегатных или накопительных показателей).❎

    **Входные данные** получены методом генерации и являются полностью синтетическими (не являются 
    реальными).

    **Состав данных:**

    transaction: номер транзакции, транзакции идут последовательно;

    date: дата и время транзакции в формате YYYY-MM-DD HH:MM:SS;

    card: хэшированный номер карты клиента;

    client: номер клиента;

    date_of_birth: дата рождения клиента в формате YYYY-MM-DD;

    passport: хэшированная серия и номер паспорта клиента;

    passport_valid_to: дата окончания срока действия паспорта в формате YYYY-MM-DD;

    phone: хэшированный номер телефона клиента;

    operation_type: тип операции - снятие, пополнение, перевод и оплата;

    amount: сумма транзакции в рублях;

    operation_result: результат операции - успешно или отказ;

    terminal_type: тип терминала, с помощью которого реализована операция: ATM – банкомат, POS – терминал 
    безналичной оплаты, WEB – интернет;

    city: город, в котором происходит операция;

    address: точный адрес – город, улица, дом.

    В качестве входных данных предоставляется архивная выгрузка **в формате csv** за период 17 мая 2024 г. 
    00:00:00 - 25 мая 2024 г. 12:00:00 для проведения аналитики (выявления паттернов) и пример 
    инкрементального файла для первичного тестирования приложения.

    **Решение должно быть представлено в виде:**
    - выступления с результатами проведенной аналитики, а также с ETL-схемой системы;
    - работающей программы, соответствующей ETL-схеме, которая выявляет и визуализирует подозрительные 
    транзакции во входящих данных.

    """
)