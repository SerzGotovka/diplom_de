import os
import glob
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

def cleanup_old_logs_task():
    """Очищает логи старше недели"""
    try:
        # Путь к папке с логами (обычно это logs в корне проекта)
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
        
        if not os.path.exists(logs_dir):
            print(f"Папка logs не найдена: {logs_dir}")
            return {"deleted_count": 0, "total_size_mb": 0}
        
        # Вычисляем дату неделю назад
        week_ago = datetime.now() - timedelta(days=7)
        
        deleted_count = 0
        total_size_mb = 0
        
        # Ищем все файлы в папке logs
        log_files = glob.glob(os.path.join(logs_dir, "**", "*"), recursive=True)
        
        for file_path in log_files:
            if os.path.isfile(file_path):
                # Получаем время модификации файла
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                # Если файл старше недели, удаляем его
                if file_mtime < week_ago:
                    try:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        deleted_count += 1
                        total_size_mb += file_size / (1024 * 1024)  # Конвертируем в MB
                        print(f"Удален файл: {file_path} (размер: {file_size / (1024 * 1024):.2f} MB)")
                    except Exception as e:
                        print(f"Ошибка при удалении файла {file_path}: {e}")
        
        result = {
            "deleted_count": deleted_count,
            "total_size_mb": round(total_size_mb, 2),
            "logs_dir": logs_dir
        }
        
        print(f"Очистка завершена:")
        print(f"  - Удалено файлов: {deleted_count}")
        print(f"  - Освобождено места: {total_size_mb:.2f} MB")
        print(f"  - Папка: {logs_dir}")
        
        return result
        
    except Exception as e:
        print(f"Ошибка при очистке логов: {e}")
        raise

def get_logs_info_task():
    """Получает информацию о файлах в папке logs"""
    try:
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
        
        if not os.path.exists(logs_dir):
            print(f"Папка logs не найдена: {logs_dir}")
            return {"total_count": 0, "total_size_mb": 0, "old_files_count": 0}
        
        # Вычисляем дату неделю назад
        week_ago = datetime.now() - timedelta(days=7)
        
        total_count = 0
        total_size_mb = 0
        old_files_count = 0
        
        # Ищем все файлы в папке logs
        log_files = glob.glob(os.path.join(logs_dir, "**", "*"), recursive=True)
        
        for file_path in log_files:
            if os.path.isfile(file_path):
                total_count += 1
                file_size = os.path.getsize(file_path)
                total_size_mb += file_size / (1024 * 1024)
                
                # Проверяем возраст файла
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mtime < week_ago:
                    old_files_count += 1
        
        result = {
            "total_count": total_count,
            "total_size_mb": round(total_size_mb, 2),
            "old_files_count": old_files_count,
            "logs_dir": logs_dir
        }
        
        print(f"Информация о логах:")
        print(f"  - Всего файлов: {total_count}")
        print(f"  - Общий размер: {total_size_mb:.2f} MB")
        print(f"  - Файлов старше недели: {old_files_count}")
        print(f"  - Папка: {logs_dir}")
        
        return result
        
    except Exception as e:
        print(f"Ошибка при получении информации о логах: {e}")
        raise

with DAG(
    dag_id="LOGS_CLEANUP",
    description="Очистка логов старше недели",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * 0",  # Каждое воскресенье в 2:00
    catchup=False,
    max_active_runs=1,
    tags=["cleanup", "logs", "maintenance"],
) as dag:

    # Получение информации о логах
    get_logs_info = PythonOperator(
        task_id="get_logs_info",
        python_callable=get_logs_info_task
    )

    # Очистка старых логов
    cleanup_logs = PythonOperator(
        task_id="cleanup_old_logs",
        python_callable=cleanup_old_logs_task
    )


    # Определяем порядок выполнения задач
    get_logs_info >> cleanup_logs
