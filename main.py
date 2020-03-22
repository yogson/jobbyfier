from statistics import median
from time import sleep
import sys

from pymongo import MongoClient
import requests


def download_vacancies(collection, page=0, **kwargs):

    only_with_salary = kwargs.get('only_with_salary', True)
    keyword = kwargs.get('keyword')

    payload = {
        'User-Agent': 'api-agent',
        'only_with_salary': 'true' if only_with_salary else 'false',
        'per_page': 100,
        'page': page
    }

    if keyword:
        payload.update({
            'text': keyword
        })

    resp = requests.get(
        f'https://api.hh.ru/vacancies',
        params=payload
    ).json()

    print(
        'Page: ',
        resp.get('page')
    )

    for vacancy in resp.get('items', []):
        vacancies.insert_one(vacancy)

    if page < resp.get('pages', 1)-1:
        sleep(1)
        download_vacancies(db, page=page+1, **kwargs)


def get_salary_averages(vacancies):
    salaries = []

    for item in vacancies:
        if isinstance(item, dict):
            salaries.append(get_salary(item))

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


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Wrong arguments")
        print("Usage: main.py <search_base>")
        sys.exit(-1)
    search_base = sys.argv[1]
    client = MongoClient()
    db = client.jobs
    vacancies = getattr(db, f'vacancies_{search_base}')
    vacancies.delete_many({})
    download_vacancies(vacancies, keyword=search_base)


