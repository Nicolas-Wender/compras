import streamlit as st
import pandas as pd


# Título da página
st.title("Exemplo 2 de Página no Streamlit")

# Exemplo de dados
data = {
    "Nome": ["Alice", "Bob", "Carlos", "Diana", "Eva"],
    "Idade": [25, 30, 35, 40, 22],
    "Cidade": [
        "São Paulo",
        "Rio de Janeiro",
        "Belo Horizonte",
        "Curitiba",
        "Fortaleza",
    ],
}

# Criando o DataFrame
df_exemplo = pd.DataFrame(data)

# Título do app
st.title("Dataframe com Filtros")

# Filtro para a coluna 'Nome' (usando selectbox para selecionar um nome específico)
nome_selecionado = st.multiselect(
    "Selecione o Nome",
    options=df_exemplo["Nome"].unique(),
    default=df_exemplo["Nome"].unique(),
)

# Filtro para a coluna 'Idade' (usando um slider para definir um intervalo de idades)
idade_min, idade_max = st.slider(
    "Selecione o intervalo de idade",
    min_value=int(df_exemplo["Idade"].min()),
    max_value=int(df_exemplo["Idade"].max()),
    value=(int(df_exemplo["Idade"].min()), int(df_exemplo["Idade"].max())),
)

# Aplicando os filtros ao dataframe
df_exemplo_filtrado = df_exemplo[
    (df_exemplo["Nome"].isin(nome_selecionado))
    & (df_exemplo["Idade"] >= idade_min)
    & (df_exemplo["Idade"] <= idade_max)
]

# Mostrando o dataframe filtrado
st.dataframe(df_exemplo_filtrado)
