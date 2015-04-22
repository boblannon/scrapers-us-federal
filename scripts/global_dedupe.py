import os
import subprocess
import time
import shutil
import json
from copy import deepcopy
import logging

import requests

os.environ['DJANGO_SETTINGS_MODULE'] = 'pupa.settings'

from django.conf import settings


DEDUPE_BIN = os.path.join(settings.BIN_DIR,
                          'echelon-0.1.0-SNAPSHOT-standalone.jar')

API_URL = settings.API_URL

logging.basicConfig(filename='/projects/scrape.influenceexplorer.com/logs/dedupe.log',
                    level=logging.INFO, format='%(asctime)s %(message)s')

logging.getLogger("requests").setLevel(logging.WARNING)


def get_whole_list(endpoint):
    params = {'apikey': settings.API_KEY, 'page': 1}
    max_page = 999999
    while params['page'] <= max_page:
        _url = '/'.join([API_URL, endpoint])
        resp = requests.get(_url, params=params)
        jd = resp.json()
        max_page = jd['meta']['max_page']
        for result in jd['results']:
            yield result
        logging.info('[{o}] retrieved page {n} of {m}'.format(n=params['page'],
                                                              m=max_page,
                                                              o=endpoint))
        params['page'] += 1


def get_entity(entity_id):
    params = {'apikey': settings.API_KEY}
    target_url = "{a}/{e}/".format(a=API_URL, e=entity_id)
    resp = requests.get(target_url, params=params)
    jd = resp.json()
    return jd


def add_related(list_entry):
    eg = deepcopy(list_entry)
    eg.update(get_entity(eg['id']))
    for re in eg.get('related_entities', ''):
        entity_id = re['entity_id']
        re_full = get_entity(entity_id)
        if 'participants' in re_full:
            for p in re_full['participants']:
                p.update(get_entity(p['entity_id']))
        if 'memberships' in re_full:
            for m in re_full['memberships']:
                if 'person' in m:
                    m['person'].update(get_entity(m['person']['id']))
                if 'organization' in m:
                    m['organization'].update(get_entity(m['organization']['id']))
        re.update(re_full)
    return eg


def export_data(file_loc, objects):
    logging.info('exporting data to {fl}'.format(fl=file_loc))
    total_num = len(objects)
    with open(file_loc, 'w') as out:
        count = 0
        for obj in objects:
            if not count % 100:
                logging.info('exported {n} of {t} entities to {fl}'.format(
                    fl=file_loc, n=count, t=total_num))
            js = json.dumps(add_related(obj))
            out.write(js)
            out.write('\n')
            count += 1


def main():
    IN_DIR = os.path.join(settings.DEDUPE_DIR, 'IN')
    OUT_DIR = os.path.join(settings.DEDUPE_DIR, 'OUT')
    DONE_DIR = os.path.join(settings.DEDUPE_DIR, 'DONE')
    ERR_DIR = os.path.join(settings.DEDUPE_DIR, 'ERROR')
    for d in [settings.DEDUPE_DIR, IN_DIR, OUT_DIR, DONE_DIR,
              ERR_DIR]:
        if not os.path.exists(d):
            os.mkdir(d)

    timestr = time.strftime("%Y%m%d-%H%M%S")

    logging.info('dedupe started')

    org_file = os.path.join(IN_DIR, 'organizations')
    person_file = os.path.join(IN_DIR, 'people')
    output_file = os.path.join(OUT_DIR,
                               'output_{ts}'.format(ts=timestr))

    if not os.path.exists(org_file):
        organizations = [r for r in get_whole_list('organizations')]

        export_data(org_file, organizations)

    if not os.path.exists(person_file):
        people = [r for r in get_whole_list('people')]

        export_data(person_file, people)

    logging.info('deduping...')
    exit_status = subprocess.call(['java',
                                   '-jar', DEDUPE_BIN,
                                   '-i', org_file,
                                   '-p', person_file,
                                   '-o', output_file])

    if exit_status == 0:
        org_done_loc = os.path.join(DONE_DIR,
                                    'organizations_{ts}'.format(ts=timestr))
        people_done_loc = os.path.join(DONE_DIR,
                                       'people_{ts}'.format(ts=timestr))
        shutil.move(org_file, org_done_loc)
        shutil.move(person_file, people_done_loc)
        logging.info('dedupe done')
    else:
        org_err_loc = os.path.join(ERR_DIR,
                                   'organizations_{ts}'.format(ts=timestr))
        people_err_loc = os.path.join(ERR_DIR,
                                      'people_{ts}'.format(ts=timestr))
        shutil.move(org_file, org_err_loc)
        shutil.move(person_file, people_err_loc)
        logging.info('something went wrong')

    if __name__ != '__main__':
        return output_file


if __name__ == '__main__':
    main()
