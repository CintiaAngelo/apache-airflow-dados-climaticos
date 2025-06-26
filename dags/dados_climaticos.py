from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.macros import ds_add 
from os.path import join
import pandas as pd
import os

with DAG(
        "dados_climaticos",
        start_date=pendulum.datetime(2022, 8, 22, tz="UTC"), 
        schedule='0 0 * * 1', # executar toda segunda feira
        catchup=False, 
        tags=['climaticos', 'dados', 'exemplo']
    ) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = f'mkdir -p "{os.path.expanduser("~/Documents/airflowalura/semana")}/{{{{ data_interval_end.strftime("%Y-%m-%d") }}}}"'

    )

    def extrai_dados(data_interval_end, **kwargs): 
        city = 'RiodeJaneiro'
        key = 'NHT5UZURSK6EA67JQGHZSLYD2'
        data_interval_end_str = data_interval_end
        
        
   
        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        folder_path = f'/Users/cintiacristinabragaangelo/Documents/airflowalura/semana={data_interval_end}/'
        os.makedirs(folder_path, exist_ok=True) 

        dados = pd.read_csv(URL)

        dados.to_csv(folder_path + 'dados_brutos.csv', index=False)
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(folder_path + 'temperaturas.csv', index=False)
        dados[['datetime', 'description', 'icon']].to_csv(folder_path + 'condicoes.csv', index=False)


    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
    )

    tarefa_1 >> tarefa_2