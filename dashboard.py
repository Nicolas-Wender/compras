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
import locale
from streamlit_echarts import st_echarts

locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")

# setup
st.set_page_config(layout="wide")

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

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


def query_bigquery(sql_query: str) -> pd.DataFrame:
    query_job = client.query(sql_query)

    result_query = query_job.result()

    df = pd.DataFrame(
        data=[row.values() for row in result_query],
        columns=[field.name for field in result_query.schema],
    )

    return df


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

        df_credenciais = query_bigquery(
            "SELECT * FROM `integracao-414415.data_ptl.credenciais`"
        )

        self.cache[loja] = self._validade_access_token(df_credenciais, loja)

        return self.cache[loja]


# requisitando saldo de estoque
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


# atualizando saldo de estoque
def atualizar_saldo_estoque() -> List[Dict[str, Any]]:
    lista_id_produtos = query_bigquery(
        """ 
            SELECT 
                CAST(id AS STRING) AS id,
                estoque_maximo,
                sku,
                loja,
                variacao,
                estrutura,
                nome
            FROM 
                `integracao-414415.data_ptl.produtos_bling_proteloja`
            WHERE 
                variacao != '{}'
                AND estrutura = "{'tipoEstoque': '', 'lancamentoEstoque': '', 'componentes': []}"
                AND UPPER(nome) NOT LIKE '%CONSUMIVEL%'
        """
    )

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


# requisitando pedidos de compra
def get_compra_bling(loja: str) -> List[Dict[str, Any]]:
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
                        data: datetime = (
                            datetime.strptime(str(pedido["data"]), "%Y-%m-%d")
                            if pedido["data"] and pedido["data"] != "0000-00-00"
                            else None
                        )
                        data_prevista: datetime = (
                            datetime.strptime(str(pedido["dataPrevista"]), "%Y-%m-%d")
                            if pedido["data"] and pedido["dataPrevista"] != "0000-00-00"
                            else None
                        )
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


# atualizar compras
def atualizar_compras_bling():
    df_compras_proteloja = pd.DataFrame(get_compra_bling("proteloja"))
    df_compras_vendolandia = pd.DataFrame(get_compra_bling("vendolandia"))

    df_compras = pd.concat(
        [df_compras_proteloja, df_compras_vendolandia], ignore_index=True
    )

    if df_compras.empty == False:
        schema = [
            bigquery.SchemaField("id_pedido", "STRING"),
            bigquery.SchemaField("numero_pedido", "STRING"),
            bigquery.SchemaField("data", "DATE"),
            bigquery.SchemaField("data_prevista", "DATE"),
            bigquery.SchemaField("total", "FLOAT"),
            bigquery.SchemaField("fornecedor", "STRING"),
            bigquery.SchemaField("situacao", "STRING"),
            bigquery.SchemaField("nome_produto", "STRING"),
            bigquery.SchemaField("valor_produto", "FLOAT"),
            bigquery.SchemaField("quantidade", "FLOAT"),
            bigquery.SchemaField("id_produto", "STRING"),
            bigquery.SchemaField("sku", "STRING"),
            bigquery.SchemaField("loja", "STRING"),
        ]
        table_id = f"integracao-414415.data_ptl.compras_bling"

        send_bigquery(df_compras, table_id, schema)

    df_compras["data_prevista"] = pd.to_datetime(
        df_compras["data_prevista"].fillna(""), errors="coerce"
    )

    # para cada id_produto, selecionar somente as linhas com datas minimas, e os valores da coluna quantidade e data prevista
    df_min_dates = df_compras.groupby("id_produto")["data_prevista"].min().reset_index()
    df_min = pd.merge(df_min_dates, df_compras, on=["id_produto", "data_prevista"])
    df_resultado_compras = df_min[["id_produto", "quantidade", "data_prevista"]]
    df_resultado_compras["quantidade"] = df_resultado_compras["quantidade"].fillna(0)


# requisitando e formando relatorio
def requisitando_relatorio() -> pd.DataFrame:
    # atualizar_saldo_estoque()
    # atualizar_compras_bling()

    df = query_bigquery(
        """
            WITH produtos_bling AS (
                SELECT 
                    CAST(id AS STRING) AS id,
                    estoque_maximo,
                    sku,
                    loja,
                    variacao,
                    estrutura,
                    nome
                FROM 
                    `integracao-414415.data_ptl.produtos_bling_proteloja`
                WHERE 
                    variacao != '{}'
                    AND estrutura = "{'tipoEstoque': '', 'lancamentoEstoque': '', 'componentes': []}"
                    AND UPPER(nome) NOT LIKE '%CONSUMIVEL%'
                ),

                saldo_estoque_bling AS (
                SELECT id, saldo_fisico_total FROM `integracao-414415.data_ptl.saldo_estoque_bling`
                ),

                dia_entrega_fornecedor AS (
                SELECT dias_entrega_fornecedor, dias_entrega_full, sku FROM `integracao-414415.data_ptl.dia_entrega_fornecedor`
                ),

                fornecedores_bling AS (
                SELECT id_produto AS id, custo FROM `integracao-414415.data_ptl.fornecedores_bling_proteloja` WHERE padrao != false
                ),

                filtro_compras_bling AS (
                SELECT
                    id_produto AS id,
                    quantidade,
                    data_prevista,
                    ROW_NUMBER() OVER (PARTITION BY id_produto ORDER BY data DESC) AS row_num
                FROM
                    `integracao-414415.data_ptl.compras_bling`
                ),

                compras_bling AS (
                SELECT id, quantidade, data_prevista FROM filtro_compras_bling WHERE row_num = 1
                )

                SELECT 
                    p.id,
                    p.sku,
                    p.estoque_maximo AS `50 dias`,
                    p.loja AS `loja`,
                    s.saldo_fisico_total AS `estoque total`,
                    d.dias_entrega_fornecedor AS `dias entrega fornecedor`,
                    d.dias_entrega_full AS `dias entrega full`,
                    f.custo AS `custo`,
                    c.quantidade AS `quantidade comprada`,
                    c.data_prevista AS `entrega prevista`
                FROM produtos_bling p
                LEFT JOIN saldo_estoque_bling s
                    ON p.id = s.id
                LEFT JOIN dia_entrega_fornecedor d
                    ON p.sku = d.sku
                LEFT JOIN fornecedores_bling f
                    ON p.id = f.id
                LEFT JOIN compras_bling c
                    ON p.id = c.id
        """
    )

    return df


def transformando_relatorio(df: pd.DataFrame) -> pd.DataFrame:
    df.fillna(0, inplace=True)

    df["dias de estoque"] = np.where(
        df["50 dias"] != 0,
        round(df["estoque total"] / (df["50 dias"] / 50)),
        0,
    )

    df["status"] = [[] for _ in range(len(df))]

    for index, row in df.iterrows():
        dias_compras = (
            (
                pd.to_datetime(row["entrega prevista"]).date() - datetime.today().date()
            ).days
            if pd.to_datetime(row["entrega prevista"]).date() > datetime.today().date()
            and row["entrega prevista"]
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

    return df


if "df_dash" not in st.session_state:
    pre_df = requisitando_relatorio()
    st.session_state["df_dash"] = transformando_relatorio(pre_df)

st.title("Dashboard")

col1, col2, col3, col4 = st.columns(4)

with col1:
    loja = st.selectbox(
        "Loja",
        ("proteloja", "vendolandia", "todas"),
    )

df = st.session_state["df_dash"]

df_filtrado = df[df["loja"] == loja] if loja != "todas" else df

formatar_moeda = lambda valor: locale.currency(valor, grouping=True)

n_estoque_ruptura = df_filtrado["status"].apply(lambda x: x.count("ruptura")).sum()
n_estoque_comprou_muito = (
    df_filtrado["status"].apply(lambda x: x.count("comprou muito")).sum()
)
n_estoque_sobrecarregado = (
    df_filtrado["status"].apply(lambda x: x.count("estoque sobrecarregado")).sum()
)
dinheiro_em_estoque = (df_filtrado["estoque total"] * df_filtrado["custo"]).sum()

condicao_vendas_custo_tempo = f"WHERE loja = '{loja}'" if loja != "todas" else ""

vendas_custo_tempo = query_bigquery(
    f"""
    WITH total_revenue AS (
        SELECT 
            data,
            titulo,
            custo,
            valor,
            CASE
                WHEN loja IN UNNEST(["203765097", "203830715", "203689269", "204184638", "203218801", "204184650", "203657733", "204184621", "203262103", "203773552", "203789591", "203773642"]) THEN 'proteloja'
                WHEN loja IN UNNEST(["204366947", "0"]) THEN 'vendolandia'
                ELSE 'nao contido'
            END AS loja
        FROM 
            `integracao-414415.data_ptl.vendas_geral_staging`
        WHERE 
            data >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND data <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        )
        SELECT 
            data,
            SUM(custo) AS custo,
            SUM(valor) AS valor
        FROM 
            total_revenue
        {condicao_vendas_custo_tempo}
        GROUP BY 
            data
        ORDER BY 
            data;
    """
)

tupla_proteloja = (
    "203765097",
    "203830715",
    "203689269",
    "204184638",
    "203218801",
    "204184650",
    "203657733",
    "204184621",
    "203262103",
    "203773552",
    "203789591",
    "203773642",
)
tupla_vendolandia = ("204366947", "0")

tabela_curva_abc = query_bigquery(
    f"""
    WITH total_revenue AS (
        SELECT 
            sku,
            titulo,
            SUM(quantidade) AS total_quantidade,
            SUM(valor) AS total_faturamento
        FROM 
            `integracao-414415.data_ptl.vendas_geral_staging`
        WHERE
            data > DATE_TRUNC(CURRENT_DATE(), MONTH)
            AND data < DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
            AND loja IN {tupla_proteloja if loja == 'proteloja' else tupla_vendolandia }
            AND situacao != "12"
        GROUP BY 
            sku, titulo
    ),

    vendas_ordenadas  AS (
        SELECT 
            sku,
            titulo, 
            total_quantidade,
            total_faturamento,
            total_faturamento / SUM(total_faturamento) OVER () AS perc_vendas
        FROM 
            total_revenue
        ORDER BY 
            total_faturamento DESC
    ),

    vendas_acumuladas AS (
        SELECT
            sku,
            titulo,
            total_quantidade,
            total_faturamento,
            perc_vendas,
            SUM(perc_vendas) OVER (ORDER BY total_faturamento DESC) AS perc_acumulado
        FROM
            vendas_ordenadas
    )

    SELECT
        CASE
            WHEN perc_acumulado <= 0.80 THEN 'A'
            WHEN perc_acumulado <= 0.95 THEN 'B'
            ELSE 'C'
        END AS curva,
        sku,
        titulo,
        total_quantidade AS `total quantidade`,
        total_faturamento AS `total faturamento`,
    FROM
        vendas_acumuladas
    ORDER BY
        perc_acumulado;
"""
)

st.divider()

with st.container():
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="n° estoque com ruptura", value=f"{n_estoque_ruptura}")
    with col2:
        st.metric(
            label="n° estoque com muita compra", value=f"{n_estoque_comprou_muito}"
        )
    with col3:
        st.metric(
            label="n° estoque sobrecarregado", value=f"{n_estoque_sobrecarregado}"
        )
    with col4:
        st.metric(
            label="dinheiro em estoque", value=f"{formatar_moeda(dinheiro_em_estoque)}"
        )

st.divider()

col1, col2 = st.columns(2)


with col1:
    st.markdown("### Curva ABC")
    st.dataframe(tabela_curva_abc)

with col2:
    st.markdown("### Vendas e Custo")

    x_data = [d.strftime("%Y-%m-%d") for d in vendas_custo_tempo["data"].tolist()]
    y_data_1 = [round(i, 2) for i in vendas_custo_tempo["valor"].tolist()]
    y_data_2 = [round(i, 2) for i in vendas_custo_tempo["custo"].tolist()]

    options = {
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross", "label": {"backgroundColor": "#6a7985"}},
        },
        "toolbox": {"feature": {"saveAsImage": {}}},
        "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
        "xAxis": [
            {
                "type": "category",
                "boundaryGap": False,
                "data": x_data,
            }
        ],
        "yAxis": [{"type": "value"}],
        "series": [
            {
                "name": "Valor",
                "type": "line",
                "stack": "Valor",
                "areaStyle": {},
                "emphasis": {"focus": "series"},
                "data": y_data_1,
            },
            {
                "name": "Custo",
                "type": "line",
                "stack": "Custo",
                "areaStyle": {},
                "emphasis": {"focus": "series"},
                "data": y_data_2,
            },
        ],
    }

    st_echarts(options=options, height="400px")
