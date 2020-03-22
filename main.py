from statistics import median, mode

from pymongo import MongoClient
import requests


client = MongoClient()
db = client.jobs
vacancies = db.vacancies


resp = requests.get('https://api.hh.ru/vacancies?only_with_salary=true&per_page=100&page=0', params={'User-Agent': 'api-test-agent'}).json()

salaries = []

for item in resp.get('items', []):

    if isinstance(item, dict):

        salary = None
        salary_block = item.get('salary')
        currency = salary_block.get('currency', 'RUR')
        if salary_block:
            salary_from = salary_block.get('from', None)
            salary_to = salary_block.get('to', None)
            if salary_from and salary_to:
                salary = (salary_from + salary_to) // 2
                salaries.append(salary)
            elif salary_from or salary_to:
                salary = salary_from if salary_from else salary_to
            if salary and currency != 'RUR':
                salary = salary*80 if currency == 'EUR' else salary*70

        #print(item['id'], item['name'], salary if salary else None, item.get('alternate_url'))

        vacancy = {
            'id': item['id'],
            'name': item['name'],
            'salary': salary,
            'url': item.get('alternate_url')

        }
        vacancies.insert_one(vacancy)

print('\n')
print('Медианная зарплата: ', median(salaries))
print('Средняя: ', sum(salaries)//len(salaries))
print('Мода: ', mode(salaries))
resp.pop('items')
print(resp)

