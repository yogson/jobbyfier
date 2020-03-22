from statistics import median, mode

from pymongo import MongoClient
import requests


def download_vacancies(db, page=0, **kwargs):
    vacancies = db.vacancies

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

    for vacancy in resp.get('items', []):
        vacancies.insert_one(vacancy)

    while page < resp.get('pages', 0):
        download_vacancies(page=page+1, **kwargs)


def get_salary_averages(vacancies):
    salaries = []

    for item in vacancies:
        if isinstance(item, dict):
            salaries.append(get_salary(item))

    print('\n')
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
                else salary*get_exchange_rates().get('USD')

    return salary


def get_exchange_rates():
    """Dummy. Will be improved."""
    return {
        'EUR': 80,
        'USD': 70
    }


client = MongoClient()
db = client.jobs
download_vacancies(db)
