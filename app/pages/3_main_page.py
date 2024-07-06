import streamlit as st
import requests
from datetime import datetime, timedelta
import pandas as pd


st.title('Transaction')

file = st.file_uploader("Upload csv file", type="csv")
st.sidebar.title("About")
st.sidebar.info("info =)")
if st.button('Write data to PostgreSQL'):
    with st.spinner("Just a moment ..."): 
        url = "http://127.0.0.1:8000/transaction/upload_csv/"
        files = {'file': file.getvalue()}
        response = requests.post(url, files=files)
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
    url = "http://127.0.0.1:8000/transaction/run_process_transactions"
    response = requests.get(url)
    json_response = response.json()
    return pd.DataFrame(
        [(rep['client'], rep['failures'], correct_time(rep['time_diff']), rep['time_diff_count'], rep['amount_diff_count']) for rep in json_response],
        columns=['client', 'failures', 'time_diff', 'time_diff_count', 'amount_diff_count']
    )

if 'page_index' not in st.session_state:
    st.session_state.page_index = 0

if 'process_transactions' not in st.session_state:
    st.session_state.process_transactions = None

def paginate_dataframe(df, page_size):
    total_pages = len(df) // page_size + (1 if len(df) % page_size > 0 else 0)

    page_num = st.session_state.get('page_num', 0)
    prev, page ,next = st.columns([1, 1, 1])
    if next.button('Next'):
        page_num += 1

    if prev.button('Previous'):
        page_num -= 1

    page.text(f"Page {page_num} of {total_pages}")

    st.session_state['page_num'] = page_num
    start_idx = page_num * page_size
    end_idx = start_idx + page_size
    return df.iloc[start_idx:end_idx]

# def display_paginated_table(df, page_size=10):
#     total_pages = len(df) // page_size + (1 if len(df) % page_size > 0 else 0)

#     start_index = st.session_state.page_index * page_size
#     end_index = start_index + page_size
#     st.table(df.iloc[start_index:end_index])

#     prev, page ,next = st.columns([1, 1, 1])
#     if next.button("Next"):
#         if st.session_state.page_index < total_pages - 1:
#             st.session_state.page_index += 1

#     page.text(f"Page {st.session_state.page_index + 1} of {total_pages}")

#     if prev.button("Previous"):
#         if st.session_state.page_index > 0:
#             st.session_state.page_index -= 1

if st.button('Find Fraud'):
    with st.spinner("Just a moment ..."): 
        st.session_state.process_transactions = fetch_data()
        st.success('Done!')

page_size = 10
paged_df = paginate_dataframe(st.session_state.process_transactions, page_size)
st.table(paged_df)
# if st.session_state.process_transactions is not None:
#     display_paginated_table(st.session_state.process_transactions)


st.text("Тут выведем подозрительные активности и их процент(или цвет)")
st.text("Можно визуализировать какие нибудь графики")