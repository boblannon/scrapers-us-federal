import os
import sys
import subprocess
import time
import shutil
import json
from copy import deepcopy

import requests

PROJ_ROOT = os.path.join(__file__, os.pardir, os.pardir)

sys.path.append(PROJ_ROOT)

import pupa_settings

DEDUPE_BIN = os.path.join(pupa_settings.BIN_DIR,
                          'echelon-0.1.0-SNAPSHOT-standalone.jar')

API_URL = pupa_settings.API_URL


def get_whole_list(endpoint):
    params = {'apikey': pupa_settings.API_KEY, 'page': 1}
    max_page = 999999
    while params['page'] <= max_page:
        _url = '/'.join([API_URL, endpoint])
        resp = requests.get(_url, params=params)
        jd = resp.json()
        max_page = jd['meta']['max_page']
        for result in jd['results']:
            yield result
        params['page'] += 1


def get_entity(entity_id):
    params = {'apikey': pupa_settings.API_KEY}
    resp = requests.get('/'.join([API_URL, entity_id]), params=params)
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
    with open(file_loc, 'w') as out:
        for obj in objects:
            js = json.dumps(add_related(obj))
            out.write(js)
            out.write('\n')

def main():
    IN_DIR = os.path.join(pupa_settings.DEDUPE_DIR, 'IN')
    OUT_DIR = os.path.join(pupa_settings.DEDUPE_DIR, 'OUT')
    DONE_DIR = os.path.join(pupa_settings.DEDUPE_DIR, 'DONE')
    ERR_DIR = os.path.join(pupa_settings.DEDUPE_DIR, 'ERROR')
    for d in [pupa_settings.DEDUPE_DIR, IN_DIR, OUT_DIR, DONE_DIR,
              ERR_DIR]:
        if not os.path.exists(d):
            os.mkdir(d)

    timestr = time.strftime("%Y%m%d-%H%M%S")

    org_file = os.path.join(IN_DIR, 'organizations')
    person_file = os.path.join(IN_DIR, 'people')
    output_file = os.path.join(OUT_DIR,
                              'output_{ts}'.format(ts=timestr))

    organizations = [r for r in get_whole_list('organizations')]

    export_data(org_file, organizations)

    people = [r for r in get_whole_list('people')]

    export_data(person_file, people)

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
        print('dedupe done')
    else:
        org_err_loc = os.path.join(ERR_DIR,
                                   'organizations_{ts}'.format(ts=timestr))
        people_err_loc = os.path.join(ERR_DIR,
                                      'people_{ts}'.format(ts=timestr))
        shutil.move(org_file, org_err_loc)
        shutil.move(person_file, people_err_loc)
        print('something went wrong')


if __name__ == '__main__':
    main()
