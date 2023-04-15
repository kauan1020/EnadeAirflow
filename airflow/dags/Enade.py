from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

# Definição constante
data_path = '/home/kauanu/PycharmProjects/AirFLowEnade/venv/data/'
arquivo = data_path + 'microdados_enade_2021.txt'
default_args = {
    'owner': 'Kauan Silva',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 8, 15),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "enade_airflow",
    description='Ingestão dados API Enade, tratamento e carga via pandas e airflow',
    default_args = default_args,
    schedule = '*/10 * * * *'
)

start_processing = BashOperator(
    task_id='start_proprocessing',
    bash_command='echo "Start Preprocessing!"',
    dag=dag
)

get_data = BashOperator(
    task_id = "get-data",
    bash_command = 'curl https://download.inep.gov.br/microdados/microdados_enade_2021.zip -o /home/kauanu/PycharmProjects/AirFLowEnade/venv/data/microdados_enade_2021.zip',
    dag = dag
)

def unzip_file():
    with zipfile.ZipFile('/home/kauanu/PycharmProjects/AirFLowEnade/venv/data/microdados_enade_2021.zip', 'r') as zipped:
        zipped.extractall( '/home/kauanu/PycharmProjects/AirFLowEnade/venv/data/')

unzip_data = PythonOperator(
    task_id = "unzip_data",
    python_callable = unzip_file,
    dag=dag
    )
def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_IO2', 'QE_I04',
                    'QEI05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal = ',', usecols = cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
        ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)

task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable = aplica_filtros,
    dag = dag
    )

 ##idade centralizada na média
def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + "idadecent.csv", index=False)

##idade centralizada ao quadrado
def constroi_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent.idadcent ** 2
    idadecent[['idade2']].to_csv(data_path + "idadequadrado.csv", index=False)

task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag = dag
    )

task_idade_quad = PythonOperator(
    task_id='constroi_idade_quadrado',
    python_callable=constroi_cent_quad,
    dag = dag
    )

def constroi_est_civil():
    filtro = pd.read.csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
    )

def constroi_cor():
    filtro = pd.read.csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['estcivil'] = filtro.QE_I02.replace({
    'A': 'Branca',
    'B': 'Preta',
    'C': 'Amarela',
    'D': 'Parda',
    'E': 'Indígena',
    'F': "",
    ' ': ""
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)
task_cor = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
    )

# Task de Join
def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
    filtro, idadecent, idadeaoquadrado, estcivil, cor
    ],
    axis=1
    )

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)

task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
    )

start_processing >> get_data >> unzip_data >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor]

task_idade_quad.set_upstream(task_idade_cent)

task_join.set_upstream([
task_est_civil, task_cor, task_idade_quad
])

