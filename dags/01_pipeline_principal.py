import os
import io
import json
import requests
import pandas as pd
import boto3
import duckdb
from datetime import datetime
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

# ==========================================
# 1. CONFIGURAÇÕES GERAIS
# ==========================================
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password123'
BUCKET_BRONZE = 'bronze'
BUCKET_SILVER = 'silver'
BUCKET_GOLD = 'gold'

def get_s3_client():
    return boto3.client('s3',
                        endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY,
                        config=Config(signature_version='s3v4'))

# ==========================================
# 2. FUNÇÕES DA CAMADA BRONZE
# ==========================================
def ingest_olist_to_bronze():
    s3 = get_s3_client()
    data_dir = '/opt/airflow/data' 
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_dir, filename)
            s3.upload_file(file_path, BUCKET_BRONZE, f'olist/{filename}')

def ingest_holiday_data():
    target_years = [2016, 2017, 2018]
    all_holidays = []
    for year in target_years:
        api_url = f"https://brasilapi.com.br/api/feriados/v1/{year}"
        r = requests.get(api_url, timeout=10)
        if r.status_code == 200: all_holidays.extend(r.json())
    
    df = pd.DataFrame(all_holidays)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    get_s3_client().put_object(Bucket=BUCKET_BRONZE, Key='api_feriados/nacionais.parquet', Body=parquet_buffer.getvalue())

# ==========================================
# 3. FUNÇÕES DA CAMADA SILVER
# ==========================================
def process_bronze_to_silver():
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix='olist/')
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            csv_obj = s3.get_object(Bucket=BUCKET_BRONZE, Key=obj['Key'])
            df = pd.read_csv(csv_obj['Body'])
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            df = df.drop_duplicates()
            
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            s3.put_object(Bucket=BUCKET_SILVER, Key=obj['Key'].replace('.csv', '.parquet'), Body=parquet_buffer.getvalue())

def process_holidays_silver():
    s3 = get_s3_client()
    response = s3.get_object(Bucket=BUCKET_BRONZE, Key='api_feriados/nacionais.parquet')
    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
    df['data_referencia'] = pd.to_datetime(df['date']).dt.date
    df_final = df[['data_referencia', 'name', 'type']].rename(columns={'name':'feriado_nome', 'type':'feriado_tipo'}).drop_duplicates()
    
    buf = io.BytesIO()
    df_final.to_parquet(buf, index=False, engine='pyarrow')
    s3.put_object(Bucket=BUCKET_SILVER, Key='api_feriados/feriados.parquet', Body=buf.getvalue())

# ==========================================
# 4. FUNÇÃO DA CAMADA GOLD (Ambiente Isolado)
# ==========================================
def process_silver_to_gold():
    import os, duckdb, boto3, pandas as pd, pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from botocore.client import Config

    MINIO_CONF = {'endpoint_url': 'http://minio:9000', 'aws_access_key_id': 'admin', 'aws_secret_access_key': 'password123'}
    s3 = boto3.client('s3', config=Config(signature_version='s3v4'), **MINIO_CONF)

    for f in ['olist_orders_dataset.parquet', 'olist_customers_dataset.parquet', 'olist_products_dataset.parquet', 'olist_order_items_dataset.parquet']:
        s3.download_file('silver', f'olist/{f}', f'/tmp/{f}')
    s3.download_file('silver', 'api_feriados/feriados.parquet', '/tmp/feriados.parquet')
        
    conn = duckdb.connect()
    # Data Quality
    if conn.execute("SELECT COUNT(*) FROM '/tmp/olist_orders_dataset.parquet'").fetchone()[0] < 1000:
        raise ValueError("DQ Falhou: Poucos dados.")

    # Modelagem Dimensional
    conn.execute("COPY (SELECT DISTINCT customer_id, customer_unique_id, customer_city, customer_state FROM '/tmp/olist_customers_dataset.parquet') TO '/tmp/dim_cliente.parquet' (FORMAT PARQUET);")
    conn.execute("COPY (SELECT DISTINCT product_id, product_category_name FROM '/tmp/olist_products_dataset.parquet') TO '/tmp/dim_produto.parquet' (FORMAT PARQUET);")
    conn.execute("""
        COPY (
            SELECT t.*, CASE WHEN f.data_referencia IS NOT NULL THEN TRUE ELSE FALSE END AS is_feriado, COALESCE(f.feriado_nome, 'Dia Comum') AS tipo_dia
            FROM (SELECT DISTINCT CAST(strftime(CAST(order_purchase_timestamp AS TIMESTAMP), '%Y%m%d') AS INTEGER) AS date_key, CAST(order_purchase_timestamp AS DATE) AS data_completa, extract(year from CAST(order_purchase_timestamp AS TIMESTAMP)) AS ano, extract(month from CAST(order_purchase_timestamp AS TIMESTAMP)) AS mes FROM '/tmp/olist_orders_dataset.parquet' WHERE order_purchase_timestamp IS NOT NULL) t
            LEFT JOIN '/tmp/feriados.parquet' f ON t.data_completa = f.data_referencia
        ) TO '/tmp/dim_tempo.parquet' (FORMAT PARQUET);
    """)
    conn.execute("""
        COPY (
            SELECT o.order_id, i.order_item_id, o.customer_id, i.product_id, CAST(strftime(CAST(o.order_purchase_timestamp AS TIMESTAMP), '%Y%m%d') AS INTEGER) AS date_key, o.order_status, i.price AS valor_produto, i.freight_value AS valor_frete
            FROM '/tmp/olist_orders_dataset.parquet' o
            INNER JOIN '/tmp/olist_order_items_dataset.parquet' i ON o.order_id = i.order_id
        ) TO '/tmp/fato_pedidos.parquet' (FORMAT PARQUET);
    """)

    # Escrita Apache Iceberg
    catalog = SqlCatalog("default", **{"uri": "sqlite:////tmp/iceberg.db", "s3.endpoint": MINIO_CONF['endpoint_url'], "s3.access-key-id": MINIO_CONF['aws_access_key_id'], "s3.secret-access-key": MINIO_CONF['aws_secret_access_key']})
    catalog.create_namespace_if_not_exists("gold")
    
    for table in ['dim_cliente', 'dim_produto', 'dim_tempo', 'fato_pedidos']:
        df = pd.read_parquet(f'/tmp/{table}.parquet')
        
        # Remove do catálogo o metadado antigo (se existir)
        try: catalog.drop_table(f"gold.{table}")
        except: pass
        
        # --- LIMPEZA FÍSICA DOS PARQUETS ANTIGOS ---
        prefixo = f"iceberg/{table}/data/"
        response_s3 = s3.list_objects_v2(Bucket='gold', Prefix=prefixo)
        if 'Contents' in response_s3:
            objetos_para_deletar = [{'Key': obj['Key']} for obj in response_s3['Contents']]
            s3.delete_objects(Bucket='gold', Delete={'Objects': objetos_para_deletar})
        # --------------------------------------------------------

        # Cria a tabela e escreve o novo arquivo parquet (único e limpo)
        iceberg_tab = catalog.create_table(identifier=f"gold.{table}", schema=pa.Table.from_pandas(df).schema, location=f"s3://gold/iceberg/{table}")
        iceberg_tab.overwrite(pa.Table.from_pandas(df))

# ==========================================
# 5. ORQUESTRAÇÃO FINAL
# ==========================================
with DAG(
    'pipeline_olist_master',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=['olist', 'iceberg', 'medalhao']
) as dag:

    # Tasks Bronze
    task_bronze_olist = PythonOperator(task_id='ingestao_bronze_olist', python_callable=ingest_olist_to_bronze)
    task_bronze_api = PythonOperator(task_id='ingestao_bronze_api_feriados', python_callable=ingest_holiday_data)

    # Tasks Silver
    task_silver_olist = PythonOperator(task_id='transformacao_silver_olist', python_callable=process_bronze_to_silver)
    task_silver_api = PythonOperator(task_id='transformacao_silver_feriados', python_callable=process_holidays_silver)

    # Task Gold
    task_gold_iceberg = PythonVirtualenvOperator(
        task_id='gold_iceberg_data_quality',
        python_callable=process_silver_to_gold,
        requirements=['duckdb', 'boto3', 'pandas', 'pyarrow', 'pyiceberg[s3fs,pyarrow,sql]', 'sqlalchemy>=2.0.0'],
        system_site_packages=False
    )

    # Dependências
    [task_bronze_olist, task_bronze_api] >> task_silver_olist >> task_silver_api >> task_gold_iceberg