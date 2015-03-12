import os
import logging
import json
import datetime

from copy import deepcopy
from collections import OrderedDict
from collections import defaultdict

from lxml import etree

import pupa.utils

from validictory import ValidationError

from .parse_schema import sopr_html, sopr_xml, house_xml


class Form(object):

    _type = 'disclosure_form'
    _form_jurisdiction = None
    _form_type = None
    schema = {'title': 'Basic Form Model', 'description': ''}

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._record = {'_meta': {}}
        self._name = self.schema['title']
        self._description = self.schema['description']

    def as_dict(self):
        return self._record

    def pre_save(self):
        pass

    def validate(self):
        validator = pupa.utils.DatetimeValidator(required_by_default=False)

        try:
            validator.validate(self.as_dict(), self.schema)
        except ValidationError as ve:
            raise ValidationError('validation of {} {} failed: {}'.format(
                self.__class__.__name__, self._form_jurisdiction, ve)
            )

    def __getitem__(self, key):
        return self.as_dict()[key]

    @property
    def _id(self):
        return self._record['_meta']['document_id']

    @_id.setter
    def _id(self, document_id):
        if not self._record['_meta']:
            self._record['_meta'] = {}
        self._record['_meta']['document_id'] = document_id

    def __str__(self):
        return self._id


class Parser(object):

    def __init__(self, jurisdiction, datadir, strict_validation=True):
        self.jurisdiction = jurisdiction
        self.datadir = datadir
        self.strict_validation = True

        # logging convenience methods
        self.logger = logging.getLogger("parser")
        self.info = self.logger.info
        self.debug = self.logger.debug
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical

    def save_object(self, obj):
        """
            Save object to disk as JSON.

            Generally shouldn't be called directly.
        """

        obj.pre_save()

        filename = '{id}.json'.format(id=obj._id).replace('/', '-')

        self.info('save %s %s as %s', obj._type, obj, filename)
        self.debug(json.dumps(OrderedDict(sorted(obj.as_dict().items())),
                              cls=pupa.utils.JSONEncoderPlus, indent=4,
                              separators=(',', ': ')))

        self.output_names[obj._type].add(filename)

        with open(os.path.join(self.datadir, filename), 'w') as f:
            json.dump(obj.as_dict(), f, cls=pupa.utils.JSONEncoderPlus)

        # validate after writing, allows for inspection on failure
        try:
            obj.validate()
        except ValueError as ve:
            self.warning(ve)
            if self.strict_validation:
                raise ve

    def do_parse(self, **kwargs):
        if not kwargs.get('root', False):
            raise Exception('No document root included')

        record = {'objects': defaultdict(int)}
        self.output_names = defaultdict(set)
        record['start'] = datetime.datetime.utcnow()
        for obj in self.parse(**kwargs) or []:
            self.debug('{o}'.format(o=obj))
            if hasattr(obj, '__iter__'):
                for iterobj in obj:
                    self.save_object(iterobj)
            else:
                self.save_object(obj)
            yield obj
        record['end'] = datetime.datetime.utcnow()
        if not self.output_names:
            self.error('no objects returned from parse')
        for _type, nameset in self.output_names.items():
            record['objects'][_type] += len(nameset)

        # return record

    def parse(self, **kwargs):
        raise NotImplementedError(self.__class__.__name__ +
                                  ' must provide a parse() method')


class SchemaParser(Parser):

    def __init__(self, jurisdiction, data_dir, strict_validation=True):
        super().__init__(jurisdiction, data_dir, strict_validation)
        self.schema = self.form_model.schema

    def extract_location(self, container, path, prop, expect_array=False,
                         missing_okay=False):
        raise NotImplementedError(self.__class__.__name__ +
                                  ' must provide a method for extracting' +
                                  ' from path location')

    def parse_schema_node(self, schema_node, container, prop_name):
        # initial container is just the root node of the lxml etree
        if schema_node['type'] == 'array':
            return self.parse_array(
                deepcopy(schema_node),
                container,
                prop_name
            )

        elif schema_node['type'] == 'object':
            result = {}
            for subprop, subnode in schema_node['properties'].items():
                result[subprop] = self.parse_schema_node(
                    deepcopy(subnode),
                    container,
                    subprop
                )
            return result
        else:
            _parse_fct = schema_node['parser']
            e = self.extract_location(
                container,
                schema_node['path'],
                prop_name,
                missing_okay=schema_node.get('missing', False)
            )

            if e is not None:
                if e in ([], ''):
                    return _parse_fct(e)
                return _parse_fct(e)
            else:
                # TODO: should this return null if blank=True?
                return None

    def parse_array(self, schema_node, container, prop):
        result_array = []

        array_container = self.extract_location(
            container,
            schema_node['path'],
            prop,
            missing_okay=schema_node.get('missing', False)
        )

        items_schema = schema_node['items']
        even_odd = schema_node.get('even_odd', False)

        items = self.extract_location(
            array_container,
            items_schema['path'],
            prop,
            expect_array=True,
            missing_okay=items_schema.get('missing', False)
        )

        if even_odd:
            evens = items[::2]
            odds = items[1::2]
            all_props = items_schema['properties']
            even_props = [(p, s) for p, s in all_props.items()
                          if s['even_odd'] == 'even']
            odd_props = [(p, s) for p, s in all_props.items()
                         if s['even_odd'] == 'odd']
            for even, odd in zip(evens, odds):
                result = {}
                for prop_name, prop_node in even_props:
                    result.update({prop_name: self.parse_schema_node(
                        prop_node, even, prop_name)})
                for prop_name, prop_node in odd_props:
                    result.update({prop_name: self.parse_schema_node(
                        prop_node, odd, prop_name)})
                result_array.append(result)
        else:
            for item in items:
                result = self.parse_schema_node(items_schema, item, prop)
                if result:
                    result_array.append(result)
        return result_array

    def parse(self, root=None, **kwargs):
        if self.schema['type'] == 'object':
            form = self.form_model(**kwargs)
            for prop, schema_node in self.schema['properties'].items():
                if prop == "_meta":
                    continue
                else:
                    form._record[prop] = self.parse_schema_node(schema_node,
                                                                root,
                                                                prop)
            yield form
        else:
            raise NotImplementedError('Sorry, only implemented for schemas'
                                      'where top level is object')


class LXMLSchemaParser(SchemaParser):

    def extract_location(self, container, path, prop, expect_array=False,
                         missing_okay=False):
        found = container.xpath(path)
        if not found:
            if missing_okay:
                if expect_array:
                    return []
                else:
                    return None
            else:
                container_loc = container.getroottree().getpath(container)
                self.error("\n    ".join(
                           ["no match for property {n}",
                            "container: {c}",
                            "path: {p}\n"]
                           ).format(n=prop,
                                    c=container_loc,
                                    p=path)
                           )
        else:
            self.debug("\n    ".join(
                       ["match found for property {n}",
                        "container: {c}",
                        "path: {p}\n",
                        "found: {f}"]
                       ).format(n=prop,
                                c=container.getroottree().getpath(container),
                                p=path,
                                f=found)
                       )

            if expect_array:
                return found
            else:
                if len(found) > 1:
                    self.warning("\n    ".join(
                                 ["more than one result for {n}",
                                  "container: {c}",
                                  "path: {p}\n"]
                                 ).format(n=prop,
                                          c=container.getroottree().getpath(container),
                                          p=path)
                                 )
                    return found[0]
                else:
                    return found[0]


class HTMLSchemaParser(LXMLSchemaParser):

    def parse(self, **kwargs):
        from lxml.html import HTMLParser
        html_parser = HTMLParser()

        etree_root = etree.fromstring(kwargs['root'], parser=html_parser)

        return super().parse(root=etree_root,
                             document_id=kwargs['document_id'])


class XMLSchemaParser(LXMLSchemaParser):

    def parse(self, **kwargs):
        etree_root = etree.parse(kwargs['root'])

        object_path = self.form_model.schema['object_path']

        for object_root in etree_root.xpath(object_path):
            yield from super().parse(root=object_root)


class LobbyingRegistrationForm(Form):

    _form_jurisdiction = 'unitedstates'
    _form_type = 'LD1'
    schema = sopr_html.ld1_schema

    def __init__(self, **kwargs):
        super().__init__()
        self._id = kwargs['document_id']


class SenatePostEmploymentForm(Form):
    _form_jurisdiction = 'unitedstates'
    _form_type = 'post_employment'
    schema = sopr_xml.post_employment_schema


class HousePostEmploymentForm(Form):
    _form_jurisdiction = 'unitedstates'
    _form_type = 'post_employment'
    schema = house_xml.post_employment_schema


class UnitedStatesLobbyingRegistrationParser(HTMLSchemaParser):
    form_model = LobbyingRegistrationForm


class UnitedStatesSenatePostEmploymentParser(XMLSchemaParser):
    form_model = SenatePostEmploymentForm

    def parse(self, **kwargs):
        for form in super().parse(**kwargs):
            name_str = '-'.join([s for s in [form._record['name']['name_first'],
                                             form._record['name']['name_middle'],
                                             form._record['name']['name_last']]
                                 if s is not None])
            office_str = form._record['office_name']
            date_str = form._record['restriction_period']['restriction_period_begin_date']
            form._id = '_'.join([s.replace(' ', '-') for s in ['senate-post-employment',
                                                               name_str,
                                                               office_str,
                                                               date_str]])
            yield form


class UnitedStatesHousePostEmploymentParser(XMLSchemaParser):
    form_model = HousePostEmploymentForm

    def parse(self, **kwargs):
        for form in super().parse(**kwargs):
            name_str = form._record['employee_name']
            office_str = form._record['office_name']
            date_str = form._record['termination_date']
            form._id = '_'.join([s.replace(' ', '-') for s in ['house-post-employment',
                                                               name_str,
                                                               office_str,
                                                               date_str]])
            yield form
