from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from processed_data.backup_utils import create_postgres_backup, get_backup_info
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

def create_manual_backup_task():
    """Создает backup базы данных PostgreSQL по требованию"""
    try:
        backup_path = create_postgres_backup()
        print(f"Ручной backup успешно создан: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"Ошибка при создании ручного backup: {e}")
        raise

def get_backup_info_task():
    """Получает информацию о существующих backup файлах"""
    try:
        info = get_backup_info()
        print(f"Информация о backup'ах:")
        print(f"Всего файлов: {info['total_count']}")
        print(f"Общий размер: {info['total_size_mb']} MB")
        
        if info['backups']:
            print("\nВсе backup'ы:")
            for backup in info['backups']:
                print(f"  - {backup['filename']} ({backup['size_mb']} MB, {backup['age_days']} дней)")
        
        return info
    except Exception as e:
        print(f"Ошибка при получении информации о backup'ах: {e}")
        raise

with DAG(
    dag_id="POSTGRES_MANUAL_BACKUP",
    description="Ручное создание backup базы данных PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    max_active_runs=1,
    tags=["backup", "postgres", "manual"],
) as dag:

    # Получение информации о существующих backup'ах
    get_backup_info_task_operator = PythonOperator(
        task_id="get_backup_info",
        python_callable=get_backup_info_task
    )

    # Создание backup'а
    create_backup = PythonOperator(
        task_id="create_manual_backup",
        python_callable=create_manual_backup_task
    )

    # Проверка созданного backup'а
    verify_backup = BashOperator(
        task_id="verify_backup_file",
        bash_command="""
        if [ -f "{{ ti.xcom_pull(task_ids='create_manual_backup') }}" ]; then
            echo "Backup файл существует"
            ls -lh "{{ ti.xcom_pull(task_ids='create_manual_backup') }}"
            echo "Backup успешно создан!"
        else
            echo "Backup файл не найден!"
            exit 1
        fi
        """,
        do_xcom_push=False
    )

    # Определяем порядок выполнения задач
    get_backup_info_task_operator >> create_backup >> verify_backup
