# dags/report/run_streamlit_app.py

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration
from include.global_variables import global_variables as gv

@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=gv.default_args,
    dagrun_timeout=duration(hours=1),
    description="Runs a streamlit app.",
    tags=["reporting", "streamlit"]
)
def run_streamlit_app():

    # Task to install altair
    install_altair = BashOperator(
        task_id="install_altair",
        bash_command="pip install altair",
    )

    # Task to run the streamlit app contained in the include folder
    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        bash_command=gv.STREAMLIT_COMMAND,
        cwd="include/streamlit_app",
        env={
            "start_date": "{{ dag_run.conf['start_date'] }}",
            "end_date": "{{ dag_run.conf['end_date'] }}"
        }
    )

    install_altair >> run_streamlit_script

run_streamlit_app()