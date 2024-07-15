import csv
import numpy as np
import streamlit as st
import requests
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

if 'count_tran' not in st.session_state:
    st.session_state.count_tran = 5
if 'client_id' not in st.session_state:
    st.session_state.client_id = None
if 'time_seconds' not in st.session_state:
    st.session_state.time_seconds = 60
if 'time_minutes' not in st.session_state:
    st.session_state.time_minutes = 30
if 'threshold_amount' not in st.session_state:
    st.session_state.threshold_amount = 4.0
if 'settings' not in st.session_state:
    st.session_state.settings = False
if 'current_csv_transaction' not in st.session_state:
    st.session_state.current_csv_transaction = None
if 'page_size' not in st.session_state:
    st.session_state.page_size = 10
if 'page_index' not in st.session_state:
    st.session_state.page_index = 0
if 'process_transactions' not in st.session_state:
    st.session_state.process_transactions = None

st.title('Transaction')

file = st.file_uploader("Upload csv file", type="csv")
st.sidebar.title("About")
st.sidebar.info("info =)")

if st.button('Write data to PostgreSQL'):
    with st.spinner("Just a moment ..."): 
        url = "http://127.0.0.1:8000/transaction/upload_csv/"
        files = {'file': file.getvalue()}
        response = requests.post(url, files=files)

        csv_data = file.read().decode('utf-8').splitlines()
        csv_reader = csv.reader(csv_data)
        headers = next(csv_reader)
        st.session_state.current_csv_transaction = [row[0] for row in csv_reader]

    st.success('Done!')

def correct_time(time):
    time_part = time[2:]
    hours, minutes, seconds = 0, 0, 0
    if 'H' in time_part:
        hours, time_part = time_part.split('H')
        hours = int(hours)
    
    if 'M' in time_part:
        minutes, time_part = time_part.split('M')
        minutes = int(minutes)
    
    if 'S' in time_part:
        seconds = int(time_part.replace('S', ''))
    base_datetime = datetime(1, 1, 1) + timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return base_datetime.strftime('%H:%M:%S')

def fetch_data():
    url = "http://127.0.0.1:8000/transaction/run_find_fraud"
    data = {'count_time_difference_max': st.session_state.count_tran,
            'time_difference_seconds': st.session_state.time_seconds,
            'time_difference_minutes': st.session_state.time_minutes,
            'threshold_amount': st.session_state.threshold_amount}
    response = requests.post(url, json=data)
    json_response = response.json()
    return pd.DataFrame(
        [(rep['id_transaction'],rep['client'],rep['is_night'], rep['first_pattern'], rep['second_pattern'], rep['third_pattern']) for rep in json_response],
        columns=['id_transaction', 'client','is_night', 'first_pattern', 'second_pattern', 'third_pattern']
    )

def get_client(client_id):
    url = "http://127.0.0.1:8000/transaction/get_client_by_id?client_id=" + client_id.replace(" ", "")
    response = requests.post(url)
    json_response = response.json()
    return json_response

def run_find_fraud_current():
    url = "http://127.0.0.1:8000/transaction/run_find_fraud"
    data = {'list_id_transaction': [int(tr) for tr in st.session_state.current_csv_transaction],
            'count_time_difference_max': st.session_state.count_tran,
            'time_difference_seconds': st.session_state.time_seconds,
            'time_difference_minutes': st.session_state.time_minutes,
            'threshold_amount': st.session_state.threshold_amount}
            
    response = requests.post(url, json=data)
    json_response = response.json()
    return pd.DataFrame(
        [(rep['id_transaction'],rep['client'],rep['is_night'], rep['first_pattern'], rep['second_pattern'], rep['third_pattern']) for rep in json_response],
        columns=['id_transaction', 'client', 'is_night','first_pattern', 'second_pattern', 'third_pattern']
    )

def paginate_dataframe(df, page_size):
    total_pages = len(df) // page_size + (1 if len(df) % page_size > 0 else 0)

    page_num = st.session_state.get('page_num', 0)
    prev, page ,next = st.columns([1, 1, 1])
    if next.button('Next'):
        if page_num + 1 < total_pages:
            page_num += 1
        else:
            page_num = 0

    if prev.button('Previous'):
        if page_num > 0:
            page_num -= 1
        else:
            page_num = total_pages - 1
    page.text(f"Page {page_num} of {total_pages - 1}")

    st.session_state['page_num'] = page_num
    start_idx = page_num * page_size
    end_idx = start_idx + page_size
    return df.iloc[start_idx:end_idx]

def func(pct, allvals):
    absolute = int(np.round(pct/100.*np.sum(allvals)))
    return f"{pct:.1f}%\n({absolute:d} t)"

if st.button('Settings'):
    st.session_state.settings = not st.session_state.settings
if st.session_state.settings:
    st.markdown(""" Первый паттерн (частые транзакции за короткий промежуток времени)""")
    count_tran = st.slider("Количество транзакций", 5, 100, st.session_state.count_tran, 1, help="Количество транзакций порог которых должен быть превышен")
    time_seconds = st.slider("Время в секундах", 10, 600, st.session_state.time_seconds, 1, help="Максимальное время в секундах за которое может быть совершено n транзакций")
    st.session_state.count_tran = count_tran
    st.session_state.time_seconds = time_seconds
    st.markdown(""" Второй паттерн (транзакции на большую сумму, превышающую лимиты клиента) """)
    threshold_amount = st.slider("Множитель среднего значения", min_value=1.0, max_value=50.0, value=float(st.session_state.threshold_amount), step=0.1, help="Множитель среднего значения суммы транзакций клиента, превышение которого не допустимо")
    st.session_state.threshold_amount = threshold_amount
    st.markdown(""" Третий паттерн (географическая аномалия)""")
    time_minutes = st.slider("Время в минутах", 5, 96, st.session_state.time_minutes, 1, help="Время в минутах, за которое клиент может изменить свое местоположение")
    st.session_state.time_minutes = time_minutes


ffa, ffc = st.columns([1, 3])

if ffa.button('Find Fraud All'):
    with st.spinner("Just a moment ..."): 
        st.session_state.process_transactions = fetch_data()
        st.success('Done!')

if ffc.button('Find Fraud current csv'):
    with st.spinner("Just a moment ..."):
        st.session_state.process_transactions = run_find_fraud_current()
        st.success('Done!')

input, clear = st.columns([3, 1])

st.session_state.client_id = input.text_input("Client ID", None)

if clear.button('Clear'):
    st.session_state.client_id = None

if st.session_state.client_id is not None:
    data_client = get_client(st.session_state.client_id)
    data_client = data_client["Client"]
    st.write(data_client)

if st.session_state.process_transactions is not None:
    dataframe = st.session_state.process_transactions
    paged_df = paginate_dataframe(st.session_state.process_transactions, st.session_state.page_size)
    st.table(paged_df)
    data = [len(dataframe[(dataframe['first_pattern']==True)]), 
            len(dataframe[(dataframe['second_pattern']==True)]),
            len(dataframe[(dataframe['third_pattern']==True)])]
    for i in range(len(data)):
        if data[i] == 0:
            del data[i]
    fig, ax = plt.subplots()
    wedges, texts, autotexts = ax.pie(data, autopct=lambda pct: func(pct, data),explode=(0.01, 0.03, 0.05,),
                                  textprops=dict(color="w"))
    ax.legend(wedges, ['first_pattern', 'second_pattern', 'third_pattern'],
          title="Transaction",
          loc="center left",
          bbox_to_anchor=(1, 0, 0.5, 1))
    plt.setp(autotexts, size=8, weight="bold")

    st.pyplot(fig)