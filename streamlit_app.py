import hmac
import unicodedata
import streamlit as st


# Função para normalizar strings com caracteres Unicode
def normalize_string(s):
    return unicodedata.normalize("NFKC", s)


# Senha de Login
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # Normaliza as strings antes de compará-las
        entered_password = normalize_string(st.session_state["password"])
        correct_password = normalize_string(st.secrets["password"])

        if hmac.compare_digest(entered_password, correct_password):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Senha incorreta")
    return False


if not check_password():
    st.stop()  # Do not continue if check_password is not True.

pg = st.navigation(
    [st.Page("dashboard.py"), st.Page("relatorio.py"), st.Page("calculadora.py") ]
)
pg.run()
