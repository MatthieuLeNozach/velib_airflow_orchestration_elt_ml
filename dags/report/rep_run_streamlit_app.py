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
def rep_run_streamlit_app():

    # Task to check if altair is installed
    check_altair_installed = BashOperator(
        task_id="check_altair_installed",
        bash_command="python -c 'import altair' || exit 1",
    )

    # Task to install altair if not installed
    # install_altair = BashOperator(
    #     task_id="install_altair",
    #     bash_command="pip install altair",
    # )

    # Task to run the streamlit app contained in the include folder
    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        bash_command=gv.STREAMLIT_COMMAND,
        cwd="include/streamlit_app"
    )

    check_altair_installed >> run_streamlit_script
    # check_altair_installed >> install_altair >> run_streamlit_script

rep_run_streamlit_app()