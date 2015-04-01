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

from django.db import transaction
from django.conf import settings

from opencivicdata.models import Organization, OrganizationName, Person, PersonName
from pupa.utils.model_ops import merge_model_objects
from pupa.utils import combine_dicts

DEDUPE_BIN = os.path.join(settings.BIN_DIR,
                          'echelon-0.1.0-SNAPSHOT-standalone.jar')

API_URL = settings.API_URL

logger = logging.getLogger("")


def consolidate_other_names(all_other_names, name_model):
    # TODO consolidate other names using start and end dates
    consolidated = {}
    for name, objects in all_other_names.items():
        start_dates = [o.start_date for o in objects if o.start_date is not None]
        end_dates = [o.end_date for o in objects if o.end_date is not None]

        if len(start_dates) != 0:
            earliest_start = sorted(start_dates)[0]
        else:
            earliest_start = None
        if len(end_dates) != 0:
            latest_end = sorted(end_dates)[-1]
        else:
            latest_end = None

        consolidated[name] = name_model(name=name,
                                        note='',
                                        start_date=earliest_start,
                                        end_date=latest_end)

    return list(set(consolidated.values()))


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
            earliest_start_time = None

        # find the latest end time for an event
        e_events = [ep.event for ep in participation_under_this_name
                    if ep.event.end_time is not None]

        if len(e_events) != 0:
            latest_event = sorted(e_events, key=lambda x: x.end_time)[-1]
            latest_end_time = latest_event.end_time.strftime('%Y-%m-%d')
        else:
            latest_end_time = None

        # add name
        all_names[alias_object.name].append(name_model(name=alias_object.name,
                                                       note='',
                                                       start_date=earliest_start_time,
                                                       end_date=latest_end_time))
    return dict(all_names)


@transaction.commit_on_success
def merge_objects(merge_map):
    primary_id = merge_map['main-id']
    alias_ids = [i for i in merge_map['cluster-ids'] if i != primary_id]

    if primary_id.startswith('ocd-organization'):
        object_model = Organization
        name_model = OrganizationName
        name_attr = 'organization'
    elif primary_id.startswith('ocd-person'):
        object_model = Person
        name_model = PersonName
        name_attr = 'person'
    else:
        raise Exception('Only able to merge people and orgs')

    # pull down primary object
    primary_object = object_model.objects.get(id=primary_id)

    # pull down alias objects
    alias_objects = object_model.objects.filter(id__in=alias_ids)
    assert len(alias_ids) == len(alias_objects)

    # round up existing other names
    existing_other_names = collect_alias_other_names(alias_objects)

    names_to_other_names = collect_alias_names(alias_objects, name_model)

    for name, name_objects in names_to_other_names.items():
        existing_other_names[name].extend(name_objects)

    new_other_names = consolidate_other_names(existing_other_names, name_model)

    # for each alias object, create a new OtherName whose FK is primary_object
    for new_other_name in new_other_names:
        if new_other_name.name != primary_object.name:
            logger.debug('{pn}, adding {an}'.format(pn=primary_object.name,
                                                    an=new_other_name.name))
            setattr(new_other_name, name_attr, primary_object)
            logger.debug('after setattr, id is {}'.format(new_other_name.id))
            new_other_name.id = None
            new_other_name.save()
            logger.debug('saved object: {}'.format(new_other_name.__dict__))

    # for each alias object, combine_dict the extras to primary object
    orig_extras = deepcopy(primary_object.extras)
    for alias_object in alias_objects:
        new_extras = combine_dicts(json.loads(orig_extras),
                                   json.loads(alias_object.extras))

    # apply merge_model_objects
    primary_object = merge_model_objects(primary_object, alias_objects, keep_old=True)

    primary_object.extras = new_extras

    primary_object.save()

    return alias_objects


def read_echelon_output(output_loc):
    with open(output_loc) as output:
        merge_maps = json.load(output)
        for merge_map in merge_maps:
            yield merge_map


@transaction.commit_on_success
def clean_up_organizations(to_be_deleted):
    for alias_id in to_be_deleted:
        if alias_id.startswith('ocd-organization'):
            org = Organization.objects.get(id=alias_id)
            org.delete()


@transaction.commit_on_success
def clean_up_people(to_be_deleted):
    for alias_id in to_be_deleted:
        if alias_id.startswith('ocd-person'):
            person = Person.objects.get(id=alias_id)
            person.delete()


def main():
    logger.info('beginning merge')
    IN_DIR = os.path.join(settings.DEDUPE_DIR, 'IN')
    OUT_DIR = os.path.join(settings.DEDUPE_DIR, 'OUT')
    DONE_DIR = os.path.join(settings.DEDUPE_DIR, 'DONE')
    ERR_DIR = os.path.join(settings.DEDUPE_DIR, 'ERROR')
    DELETE_DIR = os.path.join(settings.DEDUPE_DIR, 'DELETE')

    output_locs = glob(os.path.join(OUT_DIR, '*'))

    if len(output_locs) > 1:
        logger.warn('More than one echelon output to apply!!')
        # I'm afraid of this so we're just going to bail
        raise Exception('More than one echelon output in OUT')

    output_loc = output_locs[0]
    output_fname = os.path.basename(output_loc)
    to_be_deleted_loc = os.path.join(DELETE_DIR, 'deleted_from_{}'.format(output_fname))

    to_be_deleted = []

    try:
        for merge_map in read_echelon_output(output_loc):
            alias_objects = merge_objects(merge_map)
            for ao in alias_objects:
                to_be_deleted.append(ao.id)
    except Exception as e:
        output_err_loc = os.path.join(ERR_DIR, output_fname)
        shutil.move(output_loc, output_err_loc)
        logger.info('something went wrong with merge')
        raise e
    else:
        output_done_loc = os.path.join(DONE_DIR, output_fname)
        shutil.move(output_loc, output_done_loc)
        with open(to_be_deleted_loc, 'w') as out:
            for alias_id in to_be_deleted:
                out.write(alias_id + '\n')
        logger.info('finished merge')

    try:
        logger.info('cleaning up organizations')
        clean_up_organizations(to_be_deleted)
        logger.info('cleaning up people')
        clean_up_people(to_be_deleted)
    except Exception as e:
        to_be_deleted_fname = os.path.basename(to_be_deleted_loc)
        to_be_deleted_err_loc = os.path.join(ERR_DIR, to_be_deleted_fname)
        shutil.move(to_be_deleted_loc, to_be_deleted_err_loc)
        logger.info('something went wrong when cleaning up')
        raise e
    else:
        to_be_deleted_fname = os.path.basename(to_be_deleted_loc)
        to_be_deleted_err_loc = os.path.join(ERR_DIR, to_be_deleted_fname)
        shutil.move(to_be_deleted_loc, to_be_deleted_err_loc)
        logger.info('finished cleaning up')


if __name__ == '__main__':
    main()
