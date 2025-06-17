
# 📊 Data Visualisation

### 1. Construire l'image Docker

```bash
docker build -t DataVisualisation-app .
```

### 2. Lancer le conteneur

```bash
docker run -p 8501:8501 DataVisualisation-app
```

### 3. Accéder à l'app

Ouvre ton navigateur à l'adresse :
[http://localhost:8501](http://localhost:8501)


st.success('This is a success message!', icon="✅")
st.info('This is a purely informational message', icon="ℹ️")
st.warning('This is a warning', icon="⚠️")
st.error('This is an error', icon="🚨")

st.toast('Hooray!', icon='🎉')


df = pd.DataFrame(np.random.randn(15, 3), columns=(["A", "B", "C"]))
my_data_element = st.line_chart(df)

for tick in range(10):
    time.sleep(.5)
    add_df = pd.DataFrame(np.random.randn(1, 3), columns=(["A", "B", "C"]))
    my_data_element.add_rows(add_df)


https://docs.streamlit.io/get-started/tutorials/create-a-multipage-app