import asyncio
import json
import os
import re
import csv
import sys
import logging
from pathlib import Path
from getpass import getpass
from typing import List, Dict, Any, Optional, Tuple
import random
import yaml
import datetime

# --- Библиотеки из оригинальных скриптов ---
from dotenv import load_dotenv
from telethon.sync import TelegramClient, errors, functions
from telethon.tl import types

# --- Настройка логирования ---
from rich.logging import RichHandler
from rich.console import Console

console = Console()
logger = logging.getLogger("rich")
logger.setLevel(logging.INFO)
handler = RichHandler(console=console, show_time=True, show_level=True, show_path=False)
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
load_dotenv()

# --- Логирование сырых ответов Telegram API ---
RAW_TELEGRAM_LOG = "telegram_raw_responses.log"

def log_telegram_raw_response(response, context=""):
    """Логирует сырые ответы Telegram API в отдельный файл."""
    try:
        with open(RAW_TELEGRAM_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n--- {context} ---\n")
            f.write(str(response))
            f.write("\n")
    except Exception as e:
        logger.error(f"Ошибка при логировании сырых ответов Telegram: {e}")

# --- Файл для хранения лимитов аккаунтов ---
ACCOUNT_LIMITS_FILE = "account_limits.json"
ACCOUNT_DAILY_LIMIT = 50

def load_account_limits() -> Dict[str, Dict[str, int]]:
    """Загружает лимиты аккаунтов из файла."""
    if os.path.exists(ACCOUNT_LIMITS_FILE):
        try:
            with open(ACCOUNT_LIMITS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Очищаем устаревшие даты
                today = datetime.date.today().isoformat()
                for phone in list(data.keys()):
                    if today not in data[phone]:
                        data[phone] = {today: 0}
                return data
        except Exception as e:
            logger.error(f"Ошибка при загрузке лимитов аккаунтов: {e}")
    return {}

def save_account_limits(limits: Dict[str, Dict[str, int]]):
    """Сохраняет лимиты аккаунтов в файл."""
    try:
        with open(ACCOUNT_LIMITS_FILE, "w", encoding="utf-8") as f:
            json.dump(limits, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка при сохранении лимитов аккаунтов: {e}")

def get_today_str():
    return datetime.date.today().isoformat()

# ==============================================================================
# Класс для управления конфигурацией и аккаунтами
# ==============================================================================

class AccountManager:
    """Управляет множественными аккаунтами Telegram и их ротацией с учетом лимитов."""
    
    def __init__(self, config_file: str = "config.yaml"):
        self.config_file = config_file
        self.accounts = []
        self.current_account_index = 0
        self.clients = {}  # Словарь для хранения активных клиентов
        self.batch_pause = 120  # Значение по умолчанию в секундах
        self.account_limits = load_account_limits()
        self.load_config()
    
    def load_config(self):
        """Загружает конфигурацию из YAML файла или из .env."""
        if os.path.exists(self.config_file):
            logger.info(f"Загрузка конфигурации из {self.config_file}")
            try:
                with open(self.config_file, 'r', encoding='utf-8') as file:
                    config = yaml.safe_load(file)
                    
                    # Загружаем аккаунты
                    if 'accounts' in config and config['accounts']:
                        self.accounts = config['accounts']
                        logger.info(f"Загружено {len(self.accounts)} аккаунтов из конфигурации")
                    
                    # Загружаем настройки пауз
                    if 'settings' in config:
                        self.batch_pause = config['settings'].get('batch_pause_seconds', 120)
                        logger.info(f"Пауза между батчами установлена: {self.batch_pause} секунд")
                        
            except Exception as e:
                logger.error(f"Ошибка при загрузке YAML конфигурации: {e}")
                self.load_from_env()
        else:
            logger.info("YAML конфигурация не найдена, загрузка из .env")
            self.load_from_env()
    
    def load_from_env(self):
        """Загружает конфигурацию из переменных окружения или запрашивает у пользователя."""
        api_id = os.getenv("API_ID")
        api_hash = os.getenv("API_HASH")
        phone_number = os.getenv("PHONE_NUMBER")
        
        if not all([api_id, api_hash, phone_number]):
            logger.info("Введите данные для входа в Telegram:")
            api_id = api_id or input("Введите ваш API ID: ")
            api_hash = api_hash or input("Введите ваш API HASH: ")
            phone_number = phone_number or input("Введите ваш номер телефона: ")
        
        self.accounts = [{
            'phone_number': phone_number,
            'api_id': int(api_id),
            'api_hash': api_hash,
            'session_name': f"{phone_number}"
        }]
    
    def get_account_limit(self, phone_number: str) -> int:
        today = get_today_str()
        return self.account_limits.get(phone_number, {}).get(today, 0)
    
    def increment_account_limit(self, phone_number: str, count: int):
        today = get_today_str()
        if phone_number not in self.account_limits:
            self.account_limits[phone_number] = {}
        if today not in self.account_limits[phone_number]:
            self.account_limits[phone_number][today] = 0
        self.account_limits[phone_number][today] += count
        save_account_limits(self.account_limits)
    
    def get_next_account(self, batch_size: int = 1) -> Optional[Dict[str, Any]]:
        """
        Возвращает следующий аккаунт для использования, который не превысит лимит с учетом batch_size.
        Если таких нет — возвращает None.
        """
        today = get_today_str()
        n = len(self.accounts)
        for offset in range(n):
            idx = (self.current_account_index + offset) % n
            account = self.accounts[idx]
            phone = account['phone_number']
            used = self.account_limits.get(phone, {}).get(today, 0)
            if used + batch_size <= ACCOUNT_DAILY_LIMIT:
                self.current_account_index = (idx + 1) % n
                logger.info(f"Переключение на аккаунт: {phone} (сегодня использовано: {used}, лимит: {ACCOUNT_DAILY_LIMIT})")
                return account
        logger.warning("Нет аккаунтов с доступным лимитом для текущего батча.")
        return None
    
    async def get_client(self, account: Dict[str, Any]) -> TelegramClient:
        """Получает или создает клиента для указанного аккаунта."""
        phone = account['phone_number']
        
        # Если клиент уже существует и подключен, возвращаем его
        if phone in self.clients and self.clients[phone].is_connected():
            return self.clients[phone]
        
        # Создаем нового клиента
        client = await self.login_account(account)
        self.clients[phone] = client
        return client
    
    async def login_account(self, account: Dict[str, Any]) -> TelegramClient:
        """Создает сессию Telethon для конкретного аккаунта."""
        logger.info(f"Вход в Telegram для аккаунта {account['phone_number']}...")
        
        session_name = account.get('session_name', account['phone_number'])
        client = TelegramClient(
            session_name,
            account['api_id'],
            account['api_hash'],
            device_model="Ubuntu",
            system_version="23.04",
            app_version="10.0.0"
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.send_code_request(account['phone_number'])
            try:
                code = input(f"Введите код для {account['phone_number']}: ")
                await client.sign_in(account['phone_number'], code)
            except errors.SessionPasswordNeededError:
                pw = getpass(f"Двухфакторная аутентификация для {account['phone_number']}: ")
                await client.sign_in(password=pw)
        
        logger.info(f"Вход выполнен успешно для {account['phone_number']}")
        await asyncio.sleep(5)  # Задержка 5 секунд после входа
        # Дополнительная пауза перед началом проверки номеров
        logger.info("Пауза 10 секунд перед началом проверки номеров этим аккаунтом...")
        await asyncio.sleep(10)
        return client
    
    async def disconnect_all(self):
        """Отключает все активные клиенты."""
        for phone, client in self.clients.items():
            if client.is_connected():
                logger.info(f"Отключение аккаунта {phone}...")
                await client.disconnect()
    
    def save_example_config(self):
        """Создает пример конфигурационного файла."""
        example_config = {
            'accounts': [
                {
                    'phone_number': '+1234567890',
                    'api_id': 12345678,
                    'api_hash': 'your_api_hash_here'
                },
                {
                    'phone_number': '+0987654321',
                    'api_id': 87654321,
                    'api_hash': 'another_api_hash_here'
                }
            ],
            'settings': {
                'batch_pause_seconds': 120,
                'request_pause_min': 120,
                'request_pause_max': 180
            }
        }
        
        with open('config.example.yaml', 'w', encoding='utf-8') as file:
            yaml.dump(example_config, file, default_flow_style=False, allow_unicode=True)
        
        logger.info("Создан пример конфигурационного файла: config.example.yaml")


# ==============================================================================
# Функции из telegram-phone-number-checker (логика работы с Telegram)
# ==============================================================================

def get_human_readable_user_status(status: types.TypeUserStatus) -> str:
    """Преобразует статус пользователя Telegram в читаемый формат."""
    if isinstance(status, types.UserStatusOnline):
        return "Currently online"
    elif isinstance(status, types.UserStatusOffline):
        return status.was_online.strftime("%Y-%m-%d %H:%M:%S %Z")
    elif isinstance(status, types.UserStatusRecently):
        return "Last seen recently"
    elif isinstance(status, types.UserStatusLastWeek):
        return "Last seen last week"
    elif isinstance(status, types.UserStatusLastMonth):
        return "Last seen last month"
    else:
        return "Unknown"

def get_random_russian_first_name() -> str:
    """Возвращает случайное русское имя."""
    names = [
        "Александр", "Максим", "Иван", "Дмитрий", "Артём", "Никита", "Михаил", "Даниил", "Егор", "Андрей",
        "Алексей", "Кирилл", "Илья", "Матвей", "Роман", "Сергей", "Владимир", "Павел", "Глеб", "Виктор",
        "Виталий", "Валерий", "Антон", "Василий", "Григорий", "Евгений", "Константин", "Леонид", "Олег", "Руслан"
    ]
    return random.choice(names)

async def get_names(
    client: TelegramClient, phone_number: str, download_profile_photos: bool = False
) -> dict:
    """
    Проверяет номер телефона, возвращает информацию о пользователе.
    Добавляет номер в контакты, получает информацию, а затем удаляет контакт.
    """
    phone_number = '+' + phone_number if not phone_number.startswith('+') else phone_number
    result = {}
    logger.info(f"Проверка номера: {phone_number}...")
    try:
        # Используем рандомное русское имя, фамилию не указываем
        contact = types.InputPhoneContact(
            client_id=0,
            phone=phone_number,
            first_name=get_random_russian_first_name(),
            last_name=""
        )
        contacts = await client(functions.contacts.ImportContactsRequest([contact]))
        # Логируем сырой ответ ImportContactsRequest
        log_telegram_raw_response(contacts, context=f"ImportContactsRequest for {phone_number}")
        users = contacts.to_dict().get("users", [])
        logger.info(users)

        if not users:
            result.update({"error": "Номер не найден в Telegram или пользователь заблокировал добавление в контакты."})
        elif len(users) == 1:
            # Добавляем задержку перед удалением контакта
            logger.info("Пауза 3 секунды перед удалением контакта...")
            await asyncio.sleep(3)
            
            updates_response: types.Updates = await client(functions.contacts.DeleteContactsRequest(id=[users[0].get("id")]))
            # Логируем сырой ответ DeleteContactsRequest
            log_telegram_raw_response(updates_response, context=f"DeleteContactsRequest for {phone_number}")
            user = updates_response.users[0]
            result.update({
                "id": user.id, "username": user.username, "usernames": user.usernames,
                "first_name": user.first_name, "last_name": user.last_name, "fake": user.fake,
                "verified": user.verified, "premium": user.premium, "mutual_contact": user.mutual_contact,
                "bot": user.bot, "bot_chat_history": user.bot_chat_history, "restricted": user.restricted,
                "restriction_reason": user.restriction_reason,
                "user_was_online": get_human_readable_user_status(user.status),
                "phone": user.phone,
            })
            if download_profile_photos:
                # Логика загрузки фото (опционально)
                pass  # Не реализовано
        else:
            result.update({"error": "На этот номер зарегистрировано несколько аккаунтов, что является непредвиденной ситуацией."})
    except TypeError as e:
        result.update({"error": f"TypeError: {e}. Возможно, не удалось удалить контакт {phone_number}."})
    except Exception as e:
        result.update({"error": f"Непредвиденная ошибка: {e}."})
        logger.error(f"Критическая ошибка при обработке {phone_number}: {e}")
    
    return result


async def validate_users(
    client: TelegramClient, 
    phone_numbers: List[str], 
    download_profile_photos: bool,
    pause_min: int = 120,
    pause_max: int = 180
) -> Dict[str, Any]:
    """
    Принимает СПИСОК номеров и возвращает словарь с информацией по каждому.
    Включает случайную задержку между запросами для предотвращения бана.
    """
    result = {}
    for i, phone in enumerate(phone_numbers, 1):
        clean_phone = re.sub(r"\s+", "", phone, flags=re.UNICODE)
        if clean_phone and clean_phone not in result:
            # Выполняем основную логику проверки номера
            result[clean_phone] = await get_names(client, clean_phone, download_profile_photos)
            
            # Если это не последний номер в пачке, делаем паузу
            if i < len(phone_numbers):
                sleep_duration = random.uniform(pause_min, pause_max)
                logger.info(f"Пауза на {sleep_duration:.2f} секунд перед следующим номером...")
                await asyncio.sleep(sleep_duration)
                
    return result


# ==============================================================================
# Функции из второго скрипта (обработка CSV и пакетов)
# ==============================================================================

def read_phone_numbers(csv_file_path: str, batch_size: int = 10) -> List[List[str]]:
    """Читает номера телефонов из CSV файла пачками (батчами)."""
    batches = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8', newline='') as file:
            reader = csv.reader(file)
            # Пропускаем заголовок, если он есть
            first_row = next(reader, None)
            if not first_row: 
                return []  # Файл пуст
            
            # Проверяем, является ли первая строка номером
            if first_row[0].strip().replace("+", "").isdigit():
                current_batch = [first_row[0].strip()]
            else:
                current_batch = []  # Это был заголовок

            for row in reader:
                if row and row[0].strip():
                    phone_number = row[0].strip()
                    current_batch.append(phone_number)
                    if len(current_batch) >= batch_size:
                        batches.append(current_batch)
                        current_batch = []
            if current_batch:
                batches.append(current_batch)
    except FileNotFoundError:
        logger.critical(f"Ошибка: Файл {csv_file_path} не найден")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Ошибка при чтении файла: {e}")
        sys.exit(1)
    return batches


def write_header_if_needed(output_file: str):
    """Записывает заголовок в CSV, если файл не существует или пуст."""
    try:
        file_exists = os.path.exists(output_file)
        is_empty = os.path.getsize(output_file) == 0 if file_exists else True
        if not file_exists or is_empty:
            with open(output_file, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    'phone_number', 'found', 'id', 'username', 'usernames', 
                    'first_name', 'last_name', 'fake', 'verified', 'premium',
                    'mutual_contact', 'bot', 'bot_chat_history', 'restricted',
                    'restriction_reason', 'user_was_online', 'phone', 'error',
                    'checked_by_account'  # Добавляем поле для отслеживания аккаунта
                ])
    except (IOError, OSError) as e:
        logger.error(f"Не удалось записать заголовок в файл {output_file}: {e}")


def parse_and_save_results(results_data: Dict[str, Any], output_file: str, checked_by: str):
    """Парсит результаты и дозаписывает их в CSV файл."""
    write_header_if_needed(output_file)
    try:
        with open(output_file, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            for phone, user_data in results_data.items():
                if user_data and 'error' not in user_data:
                    usernames_list = user_data.get('usernames', [])
                    usernames_str = ""
                    if usernames_list:
                        usernames_str = json.dumps([u.get('username', '') for u in usernames_list if isinstance(u, dict)])
                    
                    row = [
                        phone, "Yes", user_data.get('id', ''), user_data.get('username', ''),
                        usernames_str, user_data.get('first_name', ''), user_data.get('last_name', ''),
                        user_data.get('fake', ''), user_data.get('verified', ''), user_data.get('premium', ''),
                        user_data.get('mutual_contact', ''), user_data.get('bot', ''),
                        user_data.get('bot_chat_history', ''), user_data.get('restricted', ''),
                        str(user_data.get('restriction_reason', '')), user_data.get('user_was_online', ''),
                        user_data.get('phone', ''), '', checked_by
                    ]
                else:
                    error_msg = user_data.get('error') if user_data else "No data returned"
                    row = [phone, "No", '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 
                          error_msg, checked_by]
                writer.writerow(row)
    except Exception as e:
        logger.error(f"Ошибка при сохранении результатов: {e}")


# ==============================================================================
# Основная логика программы
# ==============================================================================

async def main():
    """Основная асинхронная функция программы."""
    # --- Настройки из аргументов командной строки ---
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "phone_numbers.csv"
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "telegram_check_results.csv"
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    batch_start = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    batch_end = int(sys.argv[5]) if len(sys.argv) > 5 else None
    config_file = sys.argv[6] if len(sys.argv) > 6 else "config.yaml"
    
    # Инициализация менеджера аккаунтов
    account_manager = AccountManager(config_file)
    
    # Создаем пример конфигурации если нужно
    if not os.path.exists(config_file) and not os.path.exists('config.example.yaml'):
        account_manager.save_example_config()
    
    logger.info(f"Входной файл: {input_csv}")
    logger.info(f"Выходной файл: {output_csv}")
    logger.info(f"Размер батча: {batch_size}")
    logger.info(f"Конфигурационный файл: {config_file}")
    logger.info(f"Количество доступных аккаунтов: {len(account_manager.accounts)}")
    
    batches = read_phone_numbers(input_csv, batch_size)
    batches = batches[batch_start:batch_end]
    if not batches:
        logger.info("Не найдено номеров для обработки.")
        return

    total_numbers = sum(len(b) for b in batches)
    logger.info(f"Найдено {total_numbers} номеров в {len(batches)} батчах.")
    
    try:
        processed_count = 0
        
        for i, batch in enumerate(batches, 1):
            logger.info(f"\n--- Обработка батча {i}/{len(batches)} ---")
            
            # Проверяем, есть ли аккаунт с доступным лимитом для этого батча
            account = account_manager.get_next_account(batch_size=len(batch))
            if not account:
                logger.warning("Нет аккаунтов с доступным лимитом для текущего батча. Завершаем выполнение.")
                break

            client = await account_manager.get_client(account)
            
            # Получаем настройки пауз из конфигурации
            pause_min = 120
            pause_max = 180
            if hasattr(account_manager, 'accounts') and account_manager.accounts:
                # Если есть настройки в конфигурации
                for acc in account_manager.accounts:
                    if acc['phone_number'] == account['phone_number']:
                        pause_min = acc.get('request_pause_min', 120)
                        pause_max = acc.get('request_pause_max', 180)
                        break
            
            # Проверяем номера текущего батча
            result_data = await validate_users(
                client, 
                batch, 
                download_profile_photos=False,
                pause_min=pause_min,
                pause_max=pause_max
            )
            logger.info(result_data)
            
            if result_data:
                parse_and_save_results(result_data, output_csv, account['phone_number'])
                processed_count += len(batch)
                # Увеличиваем счетчик лимита для аккаунта
                account_manager.increment_account_limit(account['phone_number'], len(batch))
                logger.info(f"Успешно обработано {len(batch)} номеров аккаунтом {account['phone_number']}.")
                logger.info(f"Сегодня этим аккаунтом проверено: {account_manager.get_account_limit(account['phone_number'])} из {ACCOUNT_DAILY_LIMIT}")
            else:
                logger.error(f"Ошибка при обработке батча {i}. Результаты не получены.")
            
            # Пауза между батчами (если это не последний батч)
            if i < len(batches):
                pause_duration = account_manager.batch_pause
                logger.info(f"Пауза между батчами: {pause_duration} секунд...")
                await asyncio.sleep(pause_duration)

        logger.info("\n======================================")
        logger.info("Обработка завершена.")
        logger.info(f"Всего обработано номеров: {processed_count} из {total_numbers}")
        logger.info(f"Результаты сохранены в файле: {output_csv}")
        logger.info("======================================")

    except Exception as e:
        logger.critical(f"Произошла критическая ошибка в основной программе: {e}")
    finally:
        logger.info("Отключение всех активных сессий...")
        await account_manager.disconnect_all()


if __name__ == "__main__":
    # Для запуска асинхронной функции main
    asyncio.run(main())