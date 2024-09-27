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
from stqdm import stqdm
import numpy as np

st.set_page_config(layout="wide")

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

# conexão com o banco de dados e api
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


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


def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password


def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password


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


#  requisitando saldo de estoque
def get_saldo_estoque(loja: str, lista_produtos: List[str]) -> List[Dict[str, Any]]:
    with st.spinner(f"Atualizando saldo de estoque {loja}..."):
        api = api_bling()
        dados_estoque = []

        for i in stqdm(range(0, len(lista_produtos), 5)):
            time.sleep(0.1)
            selecao = "&idsProdutos[]=" + "&idsProdutos[]=".join(
                lista_produtos[i : i + 5]
            )

            try:
                response = api.get(
                    f"https://www.bling.com.br/Api/v3/estoques/saldos?{selecao}",
                    loja,
                )

                if response == "error" or response.get("data", []) == []:
                    next(iter(lista_produtos))

                for produto in response.get("data", []):
                    id_produto: str = str(produto.get("produto", {}).get("id", ""))
                    saldo_fisico_total: float = float(
                        produto.get("saldoFisicoTotal", 0)
                    )

                    saldo_estoques = {
                        2816599510: 0,
                        11910799658: 0,
                        14887642988: 0,
                        9738790725: 0,
                        14197230585: 0,
                        14886665514: 0,
                    }

                    for deposito in produto.get("depositos", []):
                        saldo_estoques[deposito.get("id", 0)] = float(
                            deposito.get("saldoFisico", 0)
                        )

                    dados_estoque.append(
                        {
                            "id": id_produto,
                            "saldo_fisico_total": saldo_fisico_total,
                            "estoque_matriz": saldo_estoques[2816599510],
                            "full_amazon": saldo_estoques[11910799658],
                            "full_mgl_proteloja": saldo_estoques[14887642988],
                            "full_ml_proteloja": saldo_estoques[9738790725],
                            "full_ml_vendolandia": saldo_estoques[14197230585],
                            "full_ml_vendolandia2": saldo_estoques[14886665514],
                            "loja": loja,
                        }
                    )

            except Exception as e:
                print("Error get_fornecedores_bling /fornecedores: ", e)

        return dados_estoque


#  atualizar saldo de estoque
def atualizar_saldo_estoque(lista_id_produtos: pd.DataFrame) -> List[Dict[str, Any]]:
    df_saldo_estoque_bling_proteloja = pd.DataFrame(
        get_saldo_estoque(
            "proteloja",
            lista_id_produtos[lista_id_produtos["loja"] == "proteloja"]["id"].array,
        )
    )

    df_saldo_estoque_bling_vendolandia = pd.DataFrame(
        get_saldo_estoque(
            "vendolandia",
            lista_id_produtos[lista_id_produtos["loja"] == "vendolandia"]["id"].array,
        )
    )

    df_concatenado = pd.concat(
        [df_saldo_estoque_bling_proteloja, df_saldo_estoque_bling_vendolandia],
        ignore_index=True,
    )

    if not df_concatenado.empty:
        df_concatenado.fillna(0, inplace=True)

        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("saldo_fisico_total", "FLOAT"),
            bigquery.SchemaField("estoque_matriz", "FLOAT"),
            bigquery.SchemaField("full_amazon", "FLOAT"),
            bigquery.SchemaField("full_mgl_proteloja", "FLOAT"),
            bigquery.SchemaField("full_ml_proteloja", "FLOAT"),
            bigquery.SchemaField("full_ml_vendolandia", "FLOAT"),
            bigquery.SchemaField("full_ml_vendolandia2", "FLOAT"),
            bigquery.SchemaField("loja", "STRING"),
        ]
        table_id = f"integracao-414415.data_ptl.saldo_estoque_bling"

        send_bigquery(df_concatenado, table_id, schema)


#  requisitando pedidos de compra
def get_pedidos_compra_bling(loja: str) -> List[Dict[str, Any]]:
    with st.spinner(f"Requisitando pedidos de compra {loja}..."):
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

        for pedido in stqdm(id_pedidos):
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
                        fornecedor: str = str(
                            pedido.get("fornecedor", {}).get("id", "")
                        )
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


# requisitando e formando relatorio
def requisitando_relatorio():
    with st.spinner(f"Requisitando banco de dados..."):
        #  requisitando banco de dados e dataframes
        produtos_bling = query_bigquery(
            "SELECT id, sku, nome, estoque_maximo, loja, variacao, estrutura FROM `integracao-414415.data_ptl.produtos_bling_proteloja`"
        )

        df_produtos_bling = pd.DataFrame(
            data=[row.values() for row in produtos_bling],
            columns=[field.name for field in produtos_bling.schema],
        )

        fornecedores_bling = query_bigquery(
            "SELECT id_produto, id_fornecedor, nome_fornecedor, custo FROM `integracao-414415.data_ptl.fornecedores_bling_proteloja` WHERE padrao != false"
        )

        df_fornecedores_bling = pd.DataFrame(
            data=[row.values() for row in fornecedores_bling],
            columns=[field.name for field in fornecedores_bling.schema],
        )

        dia_entrega_fornecedor = query_bigquery(
            "SELECT dias_entrega_fornecedor, dias_entrega_full, id FROM `integracao-414415.data_ptl.dia_entrega_fornecedor`"
        )

        df_dia_entrega_fornecedor = pd.DataFrame(
            data=[row.values() for row in dia_entrega_fornecedor],
            columns=[field.name for field in dia_entrega_fornecedor.schema],
        )

    # transformando dataframes requisitados por sql
    df_produtos_bling["id"] = df_produtos_bling["id"].astype(str)

    df_produtos_bling = df_produtos_bling[df_produtos_bling["variacao"] != "{}"]
    df_produtos_bling = df_produtos_bling[
        df_produtos_bling["estrutura"]
        == "{'tipoEstoque': '', 'lancamentoEstoque': '', 'componentes': []}"
    ]
    df_produtos_bling = df_produtos_bling[
        ~df_produtos_bling["nome"].str.contains("CONSUMIVEL", case=False, na=False)
    ]

    atualizar_saldo_estoque(df_produtos_bling)

    saldo_estoque_bling = query_bigquery(
        "SELECT id, saldo_fisico_total, estoque_matriz, full_amazon, full_mgl_proteloja, full_ml_proteloja, full_ml_vendolandia, full_ml_vendolandia2 FROM `integracao-414415.data_ptl.saldo_estoque_bling`"
    )

    df_saldo_estoque_bling = pd.DataFrame(
        data=[row.values() for row in saldo_estoque_bling],
        columns=[field.name for field in saldo_estoque_bling.schema],
    )

    df_saldo_estoque_bling["id"] = df_saldo_estoque_bling["id"].astype(str)

    # requisitando e transformando pedidos de compra
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

    # juntando e ajustando dataframes
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

    df = pd.merge(df, df_fornecedores_bling, on="id_produto", how="left")
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

    # calculando dias de estoque
    df["dias de estoque"] = np.where(
        df["50 dias"] != 0,
        round(df["estoque total"] / (df["50 dias"] / 50)),
        0,  # Devolver 0 quando '50 dias' for igual a zero
    )

    # adicionando status do estoque
    df.insert(0, "checkbox", [False] * len(df))

    # removendo valores nulos
    df.fillna(0, inplace=True)

    # adicionando status
    df["status"] = [[] for _ in range(len(df))]

    for index, row in df.iterrows():
        dias_compras = (
            (
                pd.to_datetime(row["entrega prevista"]).date() - datetime.today().date()
            ).days
            if pd.to_datetime(row["entrega prevista"]).date() > datetime.today().date()
            else 7
        )

        tempo_entrega = row["dias entrega fornecedor"] + row["dias entrega full"]

        media_vendas_diaria = row["50 dias"] / 50

        estoque_seguranca = media_vendas_diaria * 25
        estoque_maximo = estoque_seguranca + (media_vendas_diaria * tempo_entrega)

        estoque_futuro = lambda dias: (
            row["estoque total"] - (media_vendas_diaria * dias)
        )

        if row["entrega prevista"] != 0:
            if (
                estoque_futuro(dias_compras) + row["quantidade comprada"]
            ) > estoque_maximo:
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

    # convertendo coluna datas
    df["entrega prevista"] = df["entrega prevista"].apply(
        lambda x: "" if x == 0 else x.strftime("%d/%m/%Y")
    )

    return df


if "df" not in st.session_state:
    st.session_state["df"] = requisitando_relatorio()

df_view = st.session_state["df"]

# streamlit
st.title("Relatório de Estoque e Compras")

col1, col2, col3 = st.columns([1, 2, 2])

with col1:
    loja = st.selectbox(
        "Loja",
        ("proteloja", "vendolandia", "todas"),
    )

with col2:
    nome_selecionado = st.multiselect(
        "Filtre por Fornecedor",
        options=df_view["nome fornecedor"].unique(),
        default=[],
    )

with col3:
    tags_selecionado = st.multiselect(
        "Filtre por Tags",
        options=pd.Series(
            [item for sublist in df_view["status"] for item in sublist]
        ).unique(),
        default=[],
    )

df_view_filtrado_loja = df_view[df_view["loja"] == loja] if loja != "todas" else df_view

df_view_filtrado_fornecedor = (
    df_view_filtrado_loja[
        df_view_filtrado_loja["nome fornecedor"].isin(nome_selecionado)
    ]
    if len(nome_selecionado) > 0
    else df_view_filtrado_loja
)

df_view_filtrado = (
    df_view_filtrado_fornecedor[
        df_view_filtrado_fornecedor["status"].apply(
            lambda lista: any(item in lista for item in tags_selecionado)
        )
    ]
    if len(tags_selecionado) > 0
    else df_view_filtrado_fornecedor
)

st.data_editor(
    df_view_filtrado,
    height=570,
    column_config={
        "checkbox": st.column_config.CheckboxColumn(
            help="Selecione o Item que você já verificou",
            default=False,
            width="small",
        ),
        "sku": st.column_config.Column(
            help="",
            width="small",
        ),
        "nome": st.column_config.Column(help="", width="medium"),
        "50 dias": st.column_config.Column(
            help="",
            width="small",
        ),
        "estoque total": st.column_config.Column(
            help="",
            width="small",
        ),
        "matriz": st.column_config.Column(
            help="",
            width="small",
        ),
        "full amz": st.column_config.Column(
            help="",
            width="small",
        ),
        "full magalu": st.column_config.Column(
            help="",
            width="small",
        ),
        "full prot": st.column_config.Column(
            help="",
            width="small",
        ),
        "full vend": st.column_config.Column(
            help="",
            width="small",
        ),
        "full vend2": st.column_config.Column(
            help="",
            width="small",
        ),
        "dias entrega fornecedor": st.column_config.Column(
            help="",
            width="small",
        ),
        "dias entrega full": st.column_config.Column(
            help="",
            width="small",
        ),
        "nome fornecedor": st.column_config.Column(
            help="",
            width="medium",
        ),
        "custo": st.column_config.NumberColumn(
            help="",
            width="small",
            format="R$%.2f",
        ),
        "quantidade comprada": st.column_config.Column(
            help="",
            width="small",
        ),
        "entrega prevista": st.column_config.Column(
            help="",
            width="medium",
        ),
        "dias de estoque": st.column_config.Column(
            help="",
            width="small",
        ),
        "status": st.column_config.Column(
            help="",
            width="large",
        ),
        "loja": st.column_config.Column(
            help="",
            width="small",
        ),
    },
    disabled=[
        "sku",
        "nome",
        "50 dias",
        "estoque total",
        "matriz",
        "full amz",
        "full magalu",
        "full prot",
        "full vend",
        "full vend2",
        "dias entrega fornecedor",
        "dias entrega full",
        "nome fornecedor",
        "custo",
        "quantidade comprada",
        "entrega prevista",
        "dias de estoque",
        "status",
        "loja",
    ],
    hide_index=True,
)
