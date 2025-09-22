from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from processed_data.backup_utils import create_postgres_backup, cleanup_old_backups, get_backup_info
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

def create_backup_task():
    """Создает backup базы данных PostgreSQL"""
    try:
        backup_path = create_postgres_backup()
        print(f"Backup успешно создан: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"Ошибка при создании backup: {e}")
        raise

def cleanup_old_backups_task():
    """Очищает старые backup файлы (старше 7 дней)"""
    try:
        cleanup_old_backups(days_to_keep=7)
        print("Очистка старых backup'ов завершена")
    except Exception as e:
        print(f"Ошибка при очистке backup'ов: {e}")
        raise

def get_backup_info_task():
    """Получает информацию о существующих backup файлах"""
    try:
        info = get_backup_info()
        print(f"Информация о backup'ах:")
        print(f"Всего файлов: {info['total_count']}")
        print(f"Общий размер: {info['total_size_mb']} MB")
        
        if info['backups']:
            print("\nПоследние backup'ы:")
            for backup in info['backups'][:5]:  # Показываем только последние 5
                print(f"  - {backup['filename']} ({backup['size_mb']} MB, {backup['age_days']} дней)")
        
        return info
    except Exception as e:
        print(f"Ошибка при получении информации о backup'ах: {e}")
        raise

with DAG(
    dag_id="POSTGRES_BACKUP",
    description="Создание backup базы данных PostgreSQL и очистка старых файлов",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # Каждый день в 2:00
    catchup=False,
    max_active_runs=1,
    tags=["backup", "postgres", "maintenance"],
) as dag:

    # Проверка доступности pg_dump
    check_pg_dump = BashOperator(
        task_id="check_pg_dump_availability",
        bash_command="which pg_dump || echo 'pg_dump not found'",
        do_xcom_push=False
    )

    # Получение информации о существующих backup'ах
    get_backup_info = PythonOperator(
        task_id="get_backup_info",
        python_callable=get_backup_info_task
    )

    # Создание backup'а
    create_backup = PythonOperator(
        task_id="create_postgres_backup",
        python_callable=create_backup_task
    )

    # Очистка старых backup'ов
    cleanup_backups = PythonOperator(
        task_id="cleanup_old_backups",
        python_callable=cleanup_old_backups_task
    )

    # Проверка созданного backup'а
    verify_backup = BashOperator(
        task_id="verify_backup_file",
        bash_command="""
        if [ -f "{{ ti.xcom_pull(task_ids='create_postgres_backup') }}" ]; then
            echo "Backup файл существует"
            ls -lh "{{ ti.xcom_pull(task_ids='create_postgres_backup') }}"
        else
            echo "Backup файл не найден!"
            exit 1
        fi
        """,
        do_xcom_push=False
    )

    # Определяем порядок выполнения задач
    check_pg_dump >> get_backup_info >> create_backup >> verify_backup >> cleanup_backups
