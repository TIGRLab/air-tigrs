from datetime import timedelta, datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

import datman.config
from datman.exceptions import UndefinedSetting


default_args = {
        'owner': 'bselby',
        'depends_on_past': False,
        }


def make_task_parser_dag(dag_id, study, config, parser, default_args):
    '''
    Creates a task parser dag for a given study.

    Returns a DAG composed of a BashOperator which simply calls the parser
    specified in the study config.
    '''

    dag = DAG(
            f'parse_tasks_{study}',
            default_args=default_args,
            description='Generates .tsvs from task data',
            schedule_interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            catchup=False,
            tags=['datman'])

    with dag:
        for task in parser.keys():
            parse_task = BashOperator(
                task_id=f'parse_{task}_task',
                bash_command='{{params.command}}',
                params={"command": parser[task]})

    return dag


config = datman.config.config()
projects = config.get_key("Projects")
logging.basicConfig(filename='task_parser_dags.log',
                    encoding='utf-8',
                    level=logging.DEBUG)

for proj in projects:
    try:
        config.set_study(proj)
        parser = config.get_key('TaskParser')
        dag_id = f"{config.study_name}_task_parser"
        logging.info(f"Task parser {parser} specified for {proj}")
        dag = make_task_parser_dag(dag_id, proj, config, parser, default_args)
        if dag is not None:
            globals()[dag_id] = dag
 
    except UndefinedSetting:
        continue

