import os
import shutil
import json
import logging
from copy import deepcopy
from collections import defaultdict
from glob import glob

os.environ['DJANGO_SETTINGS_MODULE'] = 'pupa.settings'

from django import setup

setup()

from django.conf import settings

from opencivicdata.models import Organization, OrganizationName, Person, PersonName
from pupa.utils.model_ops import merge_model_objects
from pupa.utils import combine_dicts


DEDUPE_BIN = os.path.join(settings.BIN_DIR,
                          'echelon-0.1.0-SNAPSHOT-standalone.jar')

API_URL = settings.API_URL

logging.basicConfig(filename='/projects/scrape.influenceexplorer.com/logs/dedupe.log',
                    level=logging.INFO, format='%(asctime)s %(message)s')


def consolidate_other_names(all_other_names, name_model):
    # TODO consolidate other names using start and end dates
    consolidated = {}
    for name, objects in all_other_names:
        earliest_start = sorted(objects, key=lambda x: x.start_date)[0]
        latest_end = sorted(objects, key=lambda x: x.end_date)[-1]
        consolidated[name] = name_model(name=name,
                                        note='',
                                        start_date=earliest_start,
                                        end_date=latest_end)
        for o in objects:
            if o.pk != '':
                o.delete()
    return consolidated


def collect_alias_other_names(alias_objects):
    all_other_names = defaultdict(list)
    for alias_object in alias_objects:
        for other_name in alias_object.other_names.all():
            all_other_names[other_name].append(other_name)
    return all_other_names


def collect_alias_names(alias_objects, name_model):
    # collect names
    all_names = defaultdict(list)
    for alias_object in alias_objects:
        # find all events listing this entity using this name
        participation_under_this_name = alias_object.eventparticipant_set.filter(
            name=alias_object.name)

        # find the earliest event
        s_events = [ep.event for ep in participation_under_this_name
                    if ep.event.start_time is not None]

        if len(s_events) != 0:
            earliest_event = sorted(s_events, key=lambda x: x.start_time)[0]
            earliest_start_time = earliest_event.start_time.strftime('%Y-%m-%d')
        else:
            earliest_start_time = 'unknown'

        # find the latest end time for an event
        e_events = [ep.event for ep in participation_under_this_name
                    if ep.event.end_time is not None]

        if len(e_events) != 0:
            latest_event = sorted(e_events, key=lambda x: x.end_time)[-1]
            latest_end_time = latest_event.end_time.strftime('%Y-%m-%d')
        else:
            latest_end_time = 'unknown'

        # add name
        all_names[alias_object.name].append(name_model(name=alias_object.name,
                                                       note='',
                                                       start_date=earliest_start_time,
                                                       end_date=latest_end_time))
    return dict(all_names)


def merge_objects(merge_map):
    primary_id = merge_map['main-id']
    alias_ids = merge_map['cluster-ids']

    if primary_id.startswith('ocd-organization'):
        object_model = Organization
        name_model = OrganizationName
    elif primary_id.startswith('ocd-person'):
        object_model = Person
        name_model = PersonName
    else:
        raise Exception('Only able to merge people and orgs')

    # pull down primary object
    primary_object = object_model.objects.get(id=primary_id)

    # pull down alias objects
    alias_objects = object_model.objects.filter(id__in=alias_ids)
    assert len(alias_ids) == len(alias_objects)

    # round up existing other names
    existing_other_names = collect_alias_other_names(alias_objects)

    # for each alias object, add `name` to primary_object.other_names
    names_to_other_names = collect_alias_names(alias_objects, name_model)

    for name, name_objects in names_to_other_names:
        existing_other_names[name].extend(name_objects)

    new_other_names = consolidate_other_names(existing_other_names, name_model)

    for other_name in new_other_names:
        primary_object.other_names.add(other_name)
    primary_object.save()

    # for each alias object, combine_dict the extras to primary object
    orig_extras = deepcopy(primary_object.extras)
    for alias_object in alias_objects:
        new_extras = combine_dicts(orig_extras, alias_object.extras)

    # apply merge_model_objects
    primary_object = merge_model_objects(primary_object, alias_objects)

    primary_object.extras = new_extras

    primary_object.save()


def read_echelon_output(output_loc):
    with open(output_loc) as output:
        merge_maps = json.load(output)
        for merge_map in merge_maps:
            yield merge_map


def main():
    IN_DIR = os.path.join(settings.DEDUPE_DIR, 'IN')
    OUT_DIR = os.path.join(settings.DEDUPE_DIR, 'OUT')
    DONE_DIR = os.path.join(settings.DEDUPE_DIR, 'DONE')
    ERR_DIR = os.path.join(settings.DEDUPE_DIR, 'ERROR')

    output_locs = glob(os.path.join(OUT_DIR, '*'))

    if len(output_locs) > 1:
        logging.warn('More than one echelon output to apply!!')
        # I'm afraid of this so we're just going to bail
        raise Exception('More than one echelon output in OUT')

    output_loc = output_locs[0]

    try:
        for merge_map in read_echelon_output(output_loc):
            merge_objects(merge_map)
    except Exception as e:
        output_fname = os.path.basename(output_loc)
        output_err_loc = os.path.join(ERR_DIR, output_fname)
        shutil.move(output_loc, output_err_loc)
        logging.info('something went wrong')
        raise e
    else:
        output_fname = os.path.basename(output_loc)
        output_done_loc = os.path.join(DONE_DIR, output_fname)
        shutil.move(output_loc, output_done_loc)
        logging.info('finished merge')

if __name__ == '__main__':
    main()
