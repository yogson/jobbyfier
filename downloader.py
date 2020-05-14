import requests
import aiohttp


async def fetch(session, url, payload):
    async with session.get(url, params=payload) as response:
        return await response.json()

async def download_vacancies(vacancies, **kwargs):

    reps = []

    url = 'https://api.hh.ru/vacancies'

    only_with_salary = kwargs.get('only_with_salary', True)
    keyword = kwargs.get('keyword')

    payload = {
        'User-Agent': 'api-agent',
        'only_with_salary': 'true' if only_with_salary else 'false',
        'per_page': 100,
        'page': 0
    }

    if keyword:
        payload.update({
            'text': keyword
        })

    resp = requests.get(url, params=payload).json()

    print(
        'Page: ',
        resp.get('page')
    )

    for vacancy in resp.get('items', []):
        vacancies.insert_one(vacancy)

    pages_count = resp.get('pages', 1)

    for page in range(1, pages_count):
        payload.update({
            'page': page
        })
        print('Gone for page #', page)
        async with aiohttp.ClientSession() as session:
            reps.append(await fetch(session, url, payload))

    for one in reps:
        print(one.status)
            # for vacancy in resp.get('items', []):
            #     vacancies.insert_one(vacancy)


async def download_single(id):
    payload = {
        'User-Agent': 'api-agent'
    }
    async with aiohttp.ClientSession() as session:

        return await fetch(session, f'https://api.hh.ru/vacancies/{id}', payload)


async def get_details(collection):
    reps = []
    ids = [i['id'] for i in collection.find({}, {'_id': 0, 'id': 1})]
    i = 0
    for id_ in ids:
        i += 1
        print(i)
        reps.append(await download_single(id_))

    for one in reps:
        detailed = one.json()
        description = detailed.get('description')
        if description:
            collection.find_one_and_update(
                {'id': id_},
                {'$set': {'description': description}}
            )