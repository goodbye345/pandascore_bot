import requests
import time
import pandas as pd
from typing import List, Dict

def fetch_upcoming_matches(token: str) -> List[Dict]:
    """
    Запрашивает все upcoming матчи с PandaScore API
    """
    base_url = "https://api.pandascore.co/matches/upcoming"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    all_matches = []
    page_number = 1
    page_size = 100
    max_retries = 5
    base_timeout = 30

    while True:
        url = f"{base_url}?page[size]={page_size}&page[number]={page_number}"

        retries = 0
        retry_delay = 1

        while retries <= max_retries:
            try:
                print(f"Запрашиваю страницу {page_number}...")
                response = requests.get(url, headers=headers, timeout=base_timeout)

                # Обработка 429 Too Many Requests
                if response.status_code == 429:
                    retry_after = response.headers.get('Retry-After')
                    wait_time = int(retry_after) if retry_after else retry_delay
                    print(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    retry_delay *= 1.5
                    retries += 1
                    continue

                # Проверяем успешность запроса
                response.raise_for_status()

                # Парсим JSON
                try:
                    data = response.json()
                except ValueError as e:
                    print(f"Ошибка парсинга JSON: {e}")
                    print(f"Ответ сервера: {response.text[:500]}...")
                    raise

                # Проверяем формат данных
                if isinstance(data, list):
                    # Если данные пришли сразу списком
                    if not data:
                        print(f"Получен пустой список на странице {page_number}. Завершаем.")
                        return all_matches
                    all_matches.extend(data)
                    print(f"Страница {page_number} загружена (список). Всего матчей: {len(all_matches)}")
                elif isinstance(data, dict):
                    # Если данные в формате {'data': [...], ...}
                    if not data.get('data'):
                        print(f"Запрос завершен. Всего страниц: {page_number - 1}")
                        return all_matches
                    all_matches.extend(data['data'])
                    print(f"Страница {page_number} загружена. Всего матчей: {len(all_matches)}")
                else:
                    print(f"Неожиданный формат данных: {type(data)}")
                    print(f"Данные: {str(data)[:200]}...")
                    return all_matches

                break  # Выходим из цикла ретраев

            except requests.exceptions.Timeout:
                print(f"Timeout на странице {page_number}, попытка {retries + 1}")
                if retries >= max_retries:
                    raise Exception(f"Превышено максимальное количество попыток для страницы {page_number}")
                time.sleep(retry_delay)
                retry_delay *= 1.5
                retries += 1

            except requests.exceptions.RequestException as e:
                print(f"Ошибка запроса на странице {page_number}, попытка {retries + 1}: {e}")
                if retries >= max_retries:
                    raise Exception(f"Превышено максимальное количество попыток для страницы {page_number}")
                time.sleep(retry_delay)
                retry_delay *= 1.5
                retries += 1

        page_number += 1

def main():
    # 🔥 ЗАМЕНИТЕ НА ВАШ ТОКЕН
    TOKEN = "juP7s96dZ35FXnPYRXp262pYP5CRQWQFckD4H3O5Glur5E7p3aE"

    try:
        print("Начинаю загрузку upcoming матчей...")
        matches = fetch_upcoming_matches(TOKEN)
        print(f"\nВсего загружено матчей: {len(matches)}")

        if matches:
            # Создаем DataFrame
            df = pd.DataFrame(matches)

            # Показываем основную информацию
            print("\nПервые несколько записей:")
            columns_to_show = []
            if 'id' in df.columns:
                columns_to_show.append('id')
            if 'name' in df.columns:
                columns_to_show.append('name')
            if 'begin_at' in df.columns:
                columns_to_show.append('begin_at')

            if columns_to_show:
                print(df[columns_to_show].head())
            else:
                print(df.head())

            # Сохраняем в CSV файл
            df.to_csv('upcoming_matches.csv', index=False)
            print("\nДанные сохранены в файл: upcoming_matches.csv")
            print(f"Файл содержит {len(df)} записей")

            # Показываем структуру данных
            print("\nСтруктура данных:")
            print(f"Количество колонок: {len(df.columns)}")
            print(f"Основные колонки: {list(df.columns[:10])}")

        else:
            print("Не удалось загрузить данные")

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()