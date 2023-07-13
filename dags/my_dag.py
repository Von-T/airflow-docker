from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

# this return value is automatically stored into the database (keyed to the respective task_id)
def _training_model():
    return randint(1, 10)

# this parameter (task instance) fetches from the database
def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids = [
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    best_accuracy = max(accuracies)

    if (best_accuracy > 8):
        return 'accurate'
    else:
        return 'inaccurate'

# DAG
with DAG("my_dag", start_date = datetime(2023, 7, 1), 
    schedule_interval = "@daily", catchup = False) as dag:
        
        # Task 1
        training_model_A = PythonOperator(
            task_id = "training_model_A",
            python_callable = _training_model
        )

        # Task 2
        training_model_B = PythonOperator(
            task_id = "training_model_B",
            python_callable = _training_model
        )

        # Task 3
        training_model_C = PythonOperator(
            task_id = "training_model_C",
            python_callable = _training_model
        )

        # Task 4
        choose_best_model = BranchPythonOperator(
              task_id = "choose_best_model",
              python_callable = _choose_best_model
        )

        # Task 5
        accurate = BashOperator(
              task_id = "accurate",
              bash_command = "echo 'accurate'"
        )

        # Task 6
        inaccurate = BashOperator(
              task_id = "inaccurate",
              bash_command = "echo 'inaccurate'"
        )

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]