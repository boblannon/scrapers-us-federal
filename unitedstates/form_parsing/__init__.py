import logging

from lxml import etree

from pupa import utils

class Parser(object):

    def __init__(self, jurisdiction, datadir, *, strict_validation=True):
        super(self, Parser).__init__(self)
        self.jurisdiction = jurisdiction
        self.datadir = datadir

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

        obj.pre_save(self.jurisdiction.jurisdiction_id)

        filename = '{0}_{1}.json'.format(obj._type, obj._id).replace('/', '-')

        self.info('save %s %s as %s', obj._type, obj, filename)
        self.debug(json.dumps(OrderedDict(sorted(obj.as_dict().items())),
                              cls=utils.JSONEncoderPlus, indent=4, separators=(',', ': ')))

        self.output_names[obj._type].add(filename)

        with open(os.path.join(self.datadir, filename), 'w') as f:
            json.dump(obj.as_dict(), f, cls=utils.JSONEncoderPlus)

        # validate after writing, allows for inspection on failure
        try:
            obj.validate()
        except ValueError as ve:
            self.warning(ve)
            if self.strict_validation:
                raise ve

        # after saving and validating, save subordinate objects
        for obj in obj._related:
            self.save_object(obj)
    
    def do_parse(self, **kwargs):
        record = {'objects': defaultdict(int)}
        self.output_names = defaultdict(set)
        record['start'] = datetime.datetime.utcnow()
        for obj in self.scrape(**kwargs) or []:
            if hasattr(obj, '__iter__'):
                for iterobj in obj:
                    self.save_object(iterobj)
            else:
                self.save_object(obj)
        record['end'] = datetime.datetime.utcnow()
        record['skipped'] = getattr(self, 'skipped', 0)
        if not self.output_names:
            raise ScrapeError('no objects returned from scrape')
        for _type, nameset in self.output_names.items():
            record['objects'][_type] += len(nameset)

        return record

    def parse(self, **kwargs):
        raise NotImplementedError(self.__class__.__name__ + ' must provide a scrape() method')

class SchemaParser(Parser):

    def __init__(self, parser_schema):
        super(self, SchemaParser).__init__(self)
        self._schema = parser_schema

    def parse_schema_node(self, schema_node, container):
        #initial container is just the root node of the lxml etree  
        if schema_node['type'] == 'array':
            return self.parse_array(schema_node, container)
        if schema_node['type'] == 'object':
            result = {}
            for prop, node in schema_node['properties']:
                result['prop'] = self.parse_schema_node(node, container)
            return result
        else:
            _parse_fct = schema_node['parser']
            _elements = container.xpath(schema_node['path'])
            for e in _elements:
                return _parse_fct(e)

    def parse_array(self, schema_node, container):
        result_array = []

        array_container = container.xpath(schema_node['path'])
        items_schema = schema_node['items']
        even_odd = items_schema.get('even_odd', False)

        items = array_container.xpath(items_schema['path'])
        if even_odd:
            evens = items[::2]
            odds = items[1::2]
            even_props = filter(lambda x: x['even_odd'] == 'even', items['properties'])
            odd_props = filter(lambda x: x['even_odd'] == 'odd', items['properties'])
            for even, odd in zip(evens,odds):
                result = {}
                for prop_name, prop_node in even_props:
                    result.update({prop_name: parse_schema_node(prop_node, even)})
                for prop_name, prop_node in odd_props:
                    result.update({prop_name: parse_schema_node(prop_node, odd)})
                result_array.append(result)
        else:
            for item in array_container.xpath(schema_node['items']['path']):
                result = parse_schema_node(schema_node['items']['properties'], item)
                result_array.append(result)
        return result_array

    def parse(self, root):
        if self._schema['type'] == 'object':
            record = {}
            for prop, schema_node in self._schema.iteritems():
                if prop == "_meta":
                    continue
                else:
                    record[prop] = self.parse_schema_node(schema_node, root)


class HTMLSchemaParser(SchemaParser):

    def parse(self, html_string):
        from lxml.html import HTMLParser
        html_parser = HTMLParser()

        etree_root = etree.fromstring(html_string, parser=html_parser)

        super(self, HTMLSchemaParser).parse(etree_root)



