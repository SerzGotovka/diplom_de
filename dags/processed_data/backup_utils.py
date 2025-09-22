import os
import subprocess
import logging
from datetime import datetime
from typing import Dict, Any
from pathlib import Path
from processed_data.config import get_postgres_config

logger = logging.getLogger(__name__)

def create_postgres_backup() -> str:
    """
    Создает backup базы данных PostgreSQL используя pg_dump
    Возвращает путь к созданному backup файлу
    """
    try:
        # Получаем конфигурацию PostgreSQL
        config = get_postgres_config()
        
        # Создаем директорию для backup'ов если её нет
        backup_dir = Path("/opt/airflow/postgres_backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Генерируем имя файла с timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"postgres_backup_{timestamp}.sql"
        backup_path = backup_dir / backup_filename
        
        # Формируем команду pg_dump
        pg_dump_cmd = [
            "pg_dump",
            f"--host={config['host']}",
            f"--port={config['port']}",
            f"--username={config['user']}",
            f"--dbname={config['dbname']}",
            "--verbose",
            "--clean",
            "--no-owner",
            "--no-privileges",
            "--format=plain",
            f"--file={backup_path}"
        ]
        
        # Устанавливаем переменную окружения для пароля
        env = os.environ.copy()
        env['PGPASSWORD'] = config['password']
        
        logger.info(f"Создание backup базы данных {config['dbname']} в файл {backup_path}")
        
        # Выполняем команду pg_dump
        result = subprocess.run(
            pg_dump_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        
        if result.returncode == 0:
            logger.info(f"Backup успешно создан: {backup_path}")
            logger.info(f"Размер файла: {backup_path.stat().st_size / 1024 / 1024:.2f} MB")
            return str(backup_path)
        else:
            logger.error(f"Ошибка при создании backup: {result.stderr}")
            raise RuntimeError(f"pg_dump failed: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка выполнения pg_dump: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании backup: {e}")
        raise

def cleanup_old_backups(days_to_keep: int = 7) -> None:
    """
    Удаляет старые backup файлы старше указанного количества дней
    
    Args:
        days_to_keep: Количество дней для хранения backup'ов (по умолчанию 7)
    """
    try:
        backup_dir = Path("/opt/airflow/postgres_backups")
        
        if not backup_dir.exists():
            logger.info("Директория backup'ов не существует, пропускаем очистку")
            return
        
        # Вычисляем дату, до которой нужно удалить файлы
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        
        deleted_count = 0
        total_size_freed = 0
        
        # Проходим по всем файлам в директории backup'ов
        for backup_file in backup_dir.glob("postgres_backup_*.sql"):
            if backup_file.stat().st_mtime < cutoff_date:
                file_size = backup_file.stat().st_size
                backup_file.unlink()
                deleted_count += 1
                total_size_freed += file_size
                logger.info(f"Удален старый backup: {backup_file.name}")
        
        if deleted_count > 0:
            logger.info(f"Очистка завершена. Удалено файлов: {deleted_count}")
            logger.info(f"Освобождено места: {total_size_freed / 1024 / 1024:.2f} MB")
        else:
            logger.info("Старые backup файлы не найдены")
            
    except Exception as e:
        logger.error(f"Ошибка при очистке старых backup'ов: {e}")
        raise

def get_backup_info() -> Dict[str, Any]:
    """
    Возвращает информацию о существующих backup файлах
    """
    try:
        backup_dir = Path("/opt/airflow/postgres_backups")
        
        if not backup_dir.exists():
            return {"backups": [], "total_count": 0, "total_size_mb": 0}
        
        backups = []
        total_size = 0
        
        for backup_file in backup_dir.glob("postgres_backup_*.sql"):
            stat = backup_file.stat()
            backup_info = {
                "filename": backup_file.name,
                "path": str(backup_file),
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "created_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "age_days": round((datetime.now().timestamp() - stat.st_mtime) / (24 * 60 * 60), 1)
            }
            backups.append(backup_info)
            total_size += stat.st_size
        
        # Сортируем по дате создания (новые сначала)
        backups.sort(key=lambda x: x["created_at"], reverse=True)
        
        return {
            "backups": backups,
            "total_count": len(backups),
            "total_size_mb": round(total_size / 1024 / 1024, 2)
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о backup'ах: {e}")
        return {"backups": [], "total_count": 0, "total_size_mb": 0, "error": str(e)}
