import streamlit as st
import requests
import time

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

v = st.slider("вклад", min_value=10000, max_value=10000000, step=1000)
if st.button('Find Fraud'):
    with st.spinner("Just a moment ..."): 
        time.sleep(1)
    st.success('Done!')
    st.text("Тут выведем подозрительные активности и их процент(или цвет)")
    st.text("Можно визуализировать какие нибудь графики")