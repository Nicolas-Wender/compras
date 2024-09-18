import streamlit as st
import pandas as pd

# Exemplo de DataFrame
data = {
    'Nome': ['João', 'Maria', 'Pedro', 'Ana'],
    'Idade': [28, 34, 22, 45],
    'Selecionado': [False, False, False, False]  # Coluna de checkbox
}
df = pd.DataFrame(data)

# Inicializa o session state para manter o estado do DataFrame
if 'df' not in st.session_state:
    st.session_state['df'] = df

# Função que atualiza o DataFrame no session_state com base na interação do data_editor
def update_dataframe():
    # Atualiza o DataFrame no session state com as mudanças feitas no st.data_editor
    st.session_state['df'] = st.session_state['updated_df']

# Usa st.data_editor para permitir edição interativa, incluindo a coluna de checkbox
st.session_state['updated_df'] = st.data_editor(
    st.session_state['df'],
    num_rows="dynamic",
    use_container_width=True
)

# Botão para interagir com os dados (não recarrega o DataFrame)
if st.button("Confirmar Seleção"):
    selected_rows = st.session_state['updated_df'][st.session_state['updated_df']['Selecionado']]
    st.write("Linhas Selecionadas:")
    st.write(selected_rows)

# Atualiza o session_state se houver alguma alteração no st.data_editor
st.session_state['df'] = st.session_state['updated_df']
