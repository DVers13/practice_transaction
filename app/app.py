import streamlit as st
import requests
import matplotlib.pyplot as plt
import time
import pandas as pd

def main():
    st.title('Transaction')
    file = st.file_uploader("Upload csv file", type="csv")

    if st.button('Write data to PostgreSQL'):
        url = "http://127.0.0.1:8000/transaction/upload_csv/"
        files = {'file': file.getvalue()}
        response = requests.post(url, files=files)
        st.text(response)
if __name__ == "__main__":
    main()
