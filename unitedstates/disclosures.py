import requests

from pupa.scrape import Scraper
from pupa.scrape import Disclosure, DisclosedEvent

from .form_parsing import ref.sopr


class UnitedStatesLobbyingRegistrationDisclosureScraper(Scraper):
    
    def __init__(self, start_date=None, end_date=None)
        super(self, UnitedStatesLobbyingRegistrationDisclosureScraper).__init__(self)
        self.base_url = 'http://soprweb.senate.gov/index.cfm'
        if start_date:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            self.start_date = datetime.today()
        if end_date:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            self.end_date = datetime.today()


    def search_filings(self, form):
        _search_url_rgx = re.compile(r"(window\.open\(')(.*?)('\))",
                                     re.IGNORECASE)
        
        search_params = {'event': 'processSearchCriteria'}
        resp = requests.post(self.base_url, params=search_params,
                             data=form)
        d = pq(resp.text, parser='html')
        results = d('tbody tr')
        
        if len(results) >= 3000:
            error_msg = "More than 3000 results for params:\n{}".format(
                        json.dumps(search_params, indent=2))
            raise Exception(error_msg)
        
        for result in results:
            filing_type = result.xpath('td[3]')[0].text
            filing_year = result.xpath('td[6]')[0].text
            try:
                m = re.match(_search_url_rgx, result.attrib['onclick'])
            except KeyError:
                log.error('element {} has no onclick attribute'.format(
                          etree.tostring(result)))
            _doc_path = m.groups()[1]
            _params = dict(urlparse.parse_qsl(
                                    urlparse.urlparse(_doc_path).query))
            _params['Type'] = filing_type
            _params['Year'] = filing_year
            if params:
                yield params
            else:
                log.error('unable to parse {}'.format(
                          etree.tostring(result)))

    def apply_element_node(parsed, node):
        _parse_fct = node['parser']
        _path = node['path']
        try:
            # log.debug(_path)
            element = parsed.xpath(_path)[0]
            return _parse_fct(element)
        except IndexError:
            # log.debug(parsed.xpath(_path))
            return None


    def apply_container_node(parsed, node):
        _parse_fct = node['parser']
        _children = node['children']
        _path = node['path']
        element_array = parsed.xpath(_path)
        if element_array:
            return [r for r in _parse_fct(element_array, _children)
                    if any(r.values())]
        else:
            return []


    def parse_filing(self, response, parse_schema):

        # extracts information from the filing. goal is faithful representation
        # of original data as-is, but represented as a python dict
        schema_containers = filter(lambda x: 'children' in x,
                                   self.parse_schema)
        schema_elements = filter(lambda x: 'children' not in x,
                                 self.parse_schema)
        record = defaultdict(dict)
        record['document_id'] = filename
        _parsed = etree.parse(response.content, html_parser)
        # print etree.tostring(_parsed)
        for node in schema_elements:
            _section = node['section']
            _field = node['field']
            record[_section][_field] = self.apply_element_node(_parsed, node)
        for node in schema_containers:
            _section = node['section']
            _field = node['field']
            record[_section][_field] = self.apply_container_node(_parsed, node)
        else:
            raise 

        # TODO: for now, just opening the pre-parsed files

        if self.validate_parse(record):
            self.save_parse(record)
        pass

    def validate_parse(self, parse_output):
        # checks parsed filing data against parse_schema. also tests for
        # inter-field logical clashes or unexpected values
        return True

    def save_parse(self, parse_output):
        # persists the validated parse
        parse_loc = os.path.join(PARSE_DIR,
                                 '{}.json'.format(record['document_id']))
        if os.path.exists(parse_loc) :
            raise OSError('path {} already exists'.format(parse_loc))
        else:
            with open(parse_loc, 'w') as fout:
                json.dump(record, fout, indent=2)

    def transform_parse(self):
        # mines parsed data to build Disclosure objects
        pass

    def scrape(self, start_date=None, end_date=None):
        
        all_params = []
                                  
        search_form = {'datePostedStart': datetime.strftime(self.start_date,
                                                            '%m/%d/%Y'),
                       'datePostedEnd': datetime.strftime(self.end_date,
                                                            '%m/%d/%Y')}
 
        registration_types = [t for t in ref.sopr.FILING_TYPES
                              if t['action'].startswith('registration')]

        for filing_type in registration_types:
            _form = search_form.copy()
            _form['reportType'] = filing_type['code']
            for params in self.search_filings(_form):
                filename, response = self.urlretrieve(
                    self.base_url,
                    filename=params['filingID'],
                    method='POST',
                    body=params,
                    dir=self.cache_dir
                )
                filing_json = self.parse_filing(response)
                disclosure = self.transform_parse(filing_json)
                yield disclosure
