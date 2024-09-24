import time
import base64
from datetime import datetime, timedelta
import json
import logging
import requests
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from cryptography.fernet import Fernet
import json
from typing import List, Any, Dict
from tqdm import tqdm
import numpy as np

# setup
st.set_page_config(layout="wide")

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

# conexão com o banco de dados e api
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


# bigquery
def send_bigquery(df: pd.DataFrame, table_id: str, schema: list):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {} in {}".format(
            table.num_rows, len(table.schema), table_id, datetime.now()
        )
    )

    print("Send to biquery !")


def query_bigquery(sql_query: str):
    query_job = client.query(sql_query)

    return query_job.result()


# criptografia
def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password


def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password


# api bling
class api_bling:
    def __init__(self):
        self.cache = {}
        self._401_count = 0

    def get(self, url, loja: str):
        try:
            titulo_loja = "".join(loja.split()).upper()
            acess_token = self._access_token(titulo_loja)

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {acess_token}",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                if self._401_count >= 2:
                    response.raise_for_status()

                self._401_count += 1
                self.cache.clear()
                return self.get(url, loja)
            elif response.status_code == 429:
                time.sleep(1)
                return self.get(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

    def post(self, url, body, loja: str):
        try:
            titulo_loja = "".join(loja.split()).upper()
            acess_token = self._access_token(titulo_loja)

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {acess_token}",
            }
            payload = json.dumps(body)
            response = requests.post(url, headers=headers, data=payload)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                if self._401_count >= 2:
                    response.raise_for_status()

                self._401_count += 1
                self.cache.clear()
                return self.post(url, loja)
            elif response.status_code == 429:
                time.sleep(1)
                return self.post(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

    def _oauth_refresh(self, df_credenciais: pd.DataFrame, loja: str) -> str:
        chave_criptografia = str(st.secrets["chave_criptografia"]).encode()
        bling_client_id = st.secrets[f"BLING_CLIENT_ID_{loja}"]
        bling_client_secret = st.secrets[f"BLING_CLIENT_SECRET_{loja}"]

        refresh_token = decrypt_password(
            str(
                df_credenciais["valor"]
                .loc[
                    (df_credenciais["titulo"] == "refresh_token")
                    & (df_credenciais["loja"] == f"BLING_{loja}")
                ]
                .values[0]
            ),
            chave_criptografia,
        )

        credentialbs4 = f"{bling_client_id}:{bling_client_secret}"
        credentialbs4 = credentialbs4.encode("ascii")
        credentialbs4 = base64.b64encode(credentialbs4)
        credentialbs4 = bytes.decode(credentialbs4)

        header = {"Accept": "1.0", "Authorization": f"Basic {credentialbs4}"}
        dice = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        api = requests.post(
            "https://www.bling.com.br/Api/v3/oauth/token", headers=header, json=dice
        )

        situationStatusCode = api.status_code
        api = api.json()

        if situationStatusCode == 400:
            print(f"Request failed. code: {situationStatusCode}")

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "valor",
        ] = encrypt_password(api["access_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "validade",
        ] = str(datetime.now())

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "valor",
        ] = encrypt_password(api["refresh_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "validade",
        ] = str(datetime.now())

        schema = [
            bigquery.SchemaField("loja", "STRING"),
            bigquery.SchemaField("titulo", "STRING"),
            bigquery.SchemaField("validade", "STRING"),
            bigquery.SchemaField("valor", "STRING"),
        ]
        table_id = f"integracao-414415.data_ptl.credenciais"

        send_bigquery(df_credenciais, table_id, schema)

        return api["access_token"]

    def _validade_access_token(self, df_credenciais: pd.DataFrame, loja: str) -> str:

        data_atualizacao = datetime.strptime(
            df_credenciais["validade"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"BLING_{loja}")
            ]
            .values[0],
            "%Y-%m-%d %H:%M:%S.%f",
        )

        data_limite = data_atualizacao + timedelta(hours=6)

        if datetime.now() > data_limite or self._401_count >= 2:
            return self._oauth_refresh(df_credenciais, loja)

        return decrypt_password(
            df_credenciais["valor"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"BLING_{loja}")
            ]
            .values[0],
            str(st.secrets["chave_criptografia"]).encode(),
        )

    def _access_token(self, loja: str) -> str:
        if loja in self.cache:
            return self.cache[loja]

        results_query_credenciais = query_bigquery(
            "SELECT * FROM `integracao-414415.data_ptl.credenciais`"
        )

        df_credenciais = pd.DataFrame(
            data=[row.values() for row in results_query_credenciais],
            columns=[field.name for field in results_query_credenciais.schema],
        )

        self.cache[loja] = self._validade_access_token(df_credenciais, loja)

        return self.cache[loja]


#  requisitando pedidos de compra
def get_pedidos_compra_bling(loja: str) -> List[Dict[str, Any]]:
    # with st.spinner(f"Requisitando pedidos de compra {loja}..."):
    api = api_bling()
    id_pedidos = []
    dados_pedidos = []
    page = 1

    today = datetime.now().date()
    ontem = today - timedelta(days=90)

    data_anterior = ontem.strftime("%Y-%m-%d")
    data_posterior = today.strftime("%Y-%m-%d")

    while True:
        try:
            response = api.get(
                f"https://www.bling.com.br/Api/v3/pedidos/compras?pagina={page}&limite=100&idSituacao=28&dataInicial={data_anterior}&dataFinal={data_posterior}",
                loja,
            )

            if response == "error" or response["data"] == []:
                break

            for i in response["data"]:
                id_pedidos.append(i.get("id", ""))

            page += 1
        except Exception as e:
            print("Error get_pedidos_compra_bling /compras: ", e)

    if id_pedidos == []:
        return []

    for pedido in tqdm(id_pedidos):
        if pedido != "" and pedido != None:
            try:
                response = api.get(
                    f"https://www.bling.com.br/Api/v3/pedidos/compras/{pedido}",
                    loja,
                )

                if response == []:
                    continue

                pedido: list = response.get("data", [])

                for item in pedido.get("itens", {}):
                    id_pedido: str = str(pedido.get("id", ""))
                    numero_pedido: str = str(pedido.get("numero", ""))
                    data: str = str(pedido.get("data", ""))
                    data_prevista: str = str(pedido.get("dataPrevista", ""))
                    total: float = float(pedido.get("total", 0))
                    fornecedor: str = str(pedido.get("fornecedor", {}).get("id", ""))
                    situacao: str = str(pedido.get("situacao", {}).get("valor", ""))
                    nome_produto: str = str(item.get("descricao", ""))
                    valor_produto: float = float(item.get("valor", 0))
                    quantidade: float = float(item.get("quantidade", 0))
                    id_produto: str = str(item.get("produto", {}).get("id", ""))
                    sku: str = str(item.get("produto", {}).get("codigo", ""))

                    dados_pedidos.append(
                        {
                            "id_pedido": id_pedido,
                            "numero_pedido": numero_pedido,
                            "data": data,
                            "data_prevista": data_prevista,
                            "total": total,
                            "fornecedor": fornecedor,
                            "situacao": situacao,
                            "nome_produto": nome_produto,
                            "valor_produto": valor_produto,
                            "quantidade": quantidade,
                            "id_produto": id_produto,
                            "sku": sku,
                            "loja": loja,
                        }
                    )

            except Exception as e:
                print("Error get_pedidos_compra_bling /compras/pedido: ", e)

    return dados_pedidos


def requisitando_relatorio():
    produtos_bling = query_bigquery(
        "SELECT id, estoque_maximo FROM `integracao-414415.data_ptl.produtos_bling_proteloja`"
    )

    df_produtos_bling = pd.DataFrame(
        data=[row.values() for row in produtos_bling],
        columns=[field.name for field in produtos_bling.schema],
    )

    saldo_estoque_bling = query_bigquery(
        "SELECT id, saldo_fisico_total  FROM `integracao-414415.data_ptl.saldo_estoque_bling`"
    )

    df_saldo_estoque_bling = pd.DataFrame(
        data=[row.values() for row in saldo_estoque_bling],
        columns=[field.name for field in saldo_estoque_bling.schema],
    )

    dia_entrega_fornecedor = query_bigquery(
        "SELECT dias_entrega_fornecedor, dias_entrega_full, id FROM `integracao-414415.data_ptl.dia_entrega_fornecedor`"
    )

    df_dia_entrega_fornecedor = pd.DataFrame(
        data=[row.values() for row in dia_entrega_fornecedor],
        columns=[field.name for field in dia_entrega_fornecedor.schema],
    )

    fornecedores_bling = query_bigquery(
        "SELECT id, custo FROM `integracao-414415.data_ptl.fornecedores_bling_proteloja` WHERE padrao != false"
    )

    df_fornecedores_bling = pd.DataFrame(
        data=[row.values() for row in fornecedores_bling],
        columns=[field.name for field in fornecedores_bling.schema],
    )

    df_compras_proteloja = pd.DataFrame(get_pedidos_compra_bling("proteloja"))
    df_compras_vendolandia = pd.DataFrame(get_pedidos_compra_bling("vendolandia"))

    df_compras = pd.concat(
        [df_compras_proteloja, df_compras_vendolandia], ignore_index=True
    )

    df_compras["data_prevista"] = pd.to_datetime(
        df_compras["data_prevista"], errors="coerce"
    )
    df_min_dates = df_compras.groupby("id_produto")["data_prevista"].min().reset_index()
    df_min = pd.merge(df_min_dates, df_compras, on=["id_produto", "data_prevista"])
    df_resultado_compras = df_min[["id_produto", "quantidade", "data_prevista"]]

    df = pd.merge(df_produtos_bling, df_saldo_estoque_bling, on="id", how="left")
    df = pd.merge(df, df_dia_entrega_fornecedor, on="id", how="left")
    df = df.rename(
        columns={
            "id": "id_produto",
            "estoque_maximo": "50 dias",
            "saldo_fisico_total": "estoque total",
            "estoque_matriz": "matriz",
            "full_amazon": "full amz",
            "full_mgl_proteloja": "full magalu",
            "full_ml_proteloja": "full prot",
            "full_ml_vendolandia": "full vend",
            "full_ml_vendolandia2": "full vend2",
        }
    )
    df = pd.merge(df, df_fornecedores_bling, on="id", how="left")
    df = pd.merge(df, df_resultado_compras, on="id_produto", how="left")
    df = df.rename(
        columns={
            "dias_entrega_fornecedor": "dias entrega fornecedor",
            "dias_entrega_full": "dias entrega full",
            "nome_fornecedor": "nome fornecedor",
            "quantidade": "quantidade comprada",
            "data_prevista": "entrega prevista",
        }
    )

    df.drop(
        ["id_produto", "id_fornecedor", "variacao", "estrutura"],
        axis=1,
        inplace=True,
    )

    return df


# for para um dataframe

# row["50 dias"] X
# row["entrega prevista"] X
# row["dias entrega fornecedor"] X
# row["dias entrega full"] X
# row["estoque total"] X
# row["quantidade comprada"] X

# df["custo"]  X
# df["total"] X

for index, row in df.iterrows():
    dias_compras = (
        (pd.to_datetime(row["entrega prevista"]).date() - datetime.today().date()).days
        if pd.to_datetime(row["entrega prevista"]).date() > datetime.today().date()
        else 7
    )

    tempo_entrega = row["dias entrega fornecedor"] + row["dias entrega full"]

    media_vendas_diaria = row["50 dias"] / 50

    estoque_seguranca = media_vendas_diaria * 25
    estoque_maximo = estoque_seguranca + (media_vendas_diaria * tempo_entrega)

    estoque_futuro = lambda dias: (row["estoque total"] - (media_vendas_diaria * dias))

    if row["entrega prevista"] != 0:
        if (estoque_futuro(dias_compras) + row["quantidade comprada"]) > estoque_maximo:
            df.at[index, "status"].append("comprou muito")

        if estoque_futuro(dias_compras) < estoque_seguranca:
            df.at[index, "status"].append("ruptura")

    else:
        if estoque_futuro(tempo_entrega) < estoque_seguranca:
            df.at[index, "status"].append("ruptura")

    if row["estoque total"] > estoque_maximo:
        df.at[index, "status"].append("estoque sobrecarregado")

    if row["dias de estoque"] <= 7:
        df.at[index, "status"].append("verificar compras")


