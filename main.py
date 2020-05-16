import time
from statistics import median
import asyncio
import sys

from pymongo import MongoClient

from downloader import download_vacancies, get_details


def get_salary_averages(vacancies):
    salaries = []

    for item in vacancies:
        if isinstance(item, dict):
            salary = get_salary(item)
            if salary:
                salaries.append(salary)

    print('Медианная зарплата: ', median(salaries))
    print('Средняя: ', sum(salaries)//len(salaries))


def get_salary(vacancy):
    """
    Returns middle between salary from and salary to.
    If it's only salary from, returns. If it's salary in foreign currency , exchange it.
    """
    salary = None
    salary_block = vacancy.get('salary')
    currency = salary_block.get('currency', 'RUR')

    if salary_block:
        salary_from = salary_block.get('from', None)
        salary_to = salary_block.get('to', None)
        if salary_from and salary_to:
            salary = (salary_from + salary_to) // 2

        elif salary_from or salary_to:
            salary = salary_from if salary_from else salary_to
        if salary and currency != 'RUR':
            salary = salary*get_exchange_rates().get('EUR') if currency == 'EUR' \
                else salary*get_exchange_rates().get('USD') if currency == 'USD' \
                else salary*get_exchange_rates().get('KZT') if currency == 'KZT' \
                else None

    return salary


def get_exchange_rates():
    """Dummy. Will be improved."""
    return {
        'EUR': 80,
        'USD': 70,
        "KZT": 0.17
    }


def calculate_all(db, base=None):
    if base:
        vacancies = db.get_collection(base).find()
        get_salary_averages(vacancies)
    else:
        for collection in db.list_collection_names():
            print(collection.replace('vacancies_', ''))
            vacancies = db.get_collection(collection).find()
            get_salary_averages(vacancies)


if __name__ == '__main__':
    start_time = time.time()

    client = MongoClient('172.19.33.120')
    db = client.jobs

    if len(sys.argv) == 2:
        search_base = f'vacancies_{sys.argv[1].replace(" ", "_")}'
        vacancies = getattr(db, search_base)
        vacancies.delete_many({})
        asyncio.get_event_loop().run_until_complete(download_vacancies(vacancies, keyword=sys.argv[1]))
        print('Downloading vacancies details...')
        asyncio.run(get_details(vacancies))
        calculate_all(db, base=search_base)
    else:
        calculate_all(db)

    print('Total time:', time.time() - start_time)

