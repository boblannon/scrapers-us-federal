import os
import requests
import json
import re
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qsl

from lxml.html import HTMLParser
from lxml import etree

from pupa.scrape import Scraper
from pupa.scrape import Disclosure, Person, Organization, Event

from .ref.sopr_html import FILING_TYPES

from .form_parsing.utils import mkdir_p

from .form_parsing import UnitedStatesLobbyingRegistrationParser

PARSE_DIR = os.path.join(os.environ['HOME'], 'tmp', 'parsed')
if not os.path.exists(PARSE_DIR):
    mkdir_p(PARSE_DIR)


class SOPRDisclosure(Disclosure):

    def __init__(self):
        pass


class UnitedStatesLobbyingRegistrationDisclosureScraper(Scraper):
    base_url = 'http://soprweb.senate.gov/index.cfm'
    start_date = datetime.today()
    end_date = datetime.today()
    registration_types = [t for t in FILING_TYPES
                          if t['action'].startswith('registration')]

    def _build_date_range(self, start_date, end_date):
        if start_date:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

    def scrape_authority(self):

        senate = Organization(
            name="United States Senate",
            classification='legislature',
        )

        yield senate

        sopr = Organization(
            name="Office of Public Record, US Senate",
            classification="disclosure-authority",
            parent_id=senate._id,
        )

        sopr.add_contact_detail(type="voice",
                                value="202-224-0322")

        sopr.add_link(url="http://www.senate.gov/pagelayout/legislative/one_item_and_teasers/opr.htm",
                      note="Profile page")

        sopr.add_link(url="http://www.senate.gov/pagelayout/legislative/g_three_sections_with_teasers/lobbyingdisc.htm#lobbyingdisc=lda",
                      note="Disclosure Home")

        sopr.add_link(url="http://soprweb.senate.gov/index.cfm?event=selectfields",
                      note="Disclosure Search Portal")

        sopr.add_link(url="http://soprweb.senate.gov/",
                      note="Disclosure Electronic Filing System")

        self.authority = sopr

        yield sopr

    def search_filings(self):
        search_form = {'datePostedStart': datetime.strftime(self.start_date,
                                                            '%m/%d/%Y'),
                       'datePostedEnd': datetime.strftime(self.end_date,
                                                          '%m/%d/%Y')}

        _search_url_rgx = re.compile(r"window\.open\('(.*?)'\)",
                                     re.IGNORECASE)

        search_params = {'event': 'processSearchCriteria'}

        for filing_type in self.registration_types:
            _form = search_form.copy()
            _form['reportType'] = filing_type['code']
            self.debug('making request with {f}'.format(f=_form))
            resp = requests.post(self.base_url, params=search_params,
                                 data=_form)
            d = etree.fromstring(resp.text, parser=HTMLParser())
            results = d.xpath('//*[@id="searchResults"]/tbody/tr')

            if len(results) >= 3000:
                error_msg = "More than 3000 results for params:\n{}".format(
                            json.dumps(search_params, indent=2))
                raise Exception(error_msg)

            for result in results:
                filing_type = result.xpath('td[3]')[0].text
                filing_year = result.xpath('td[6]')[0].text
                try:
                    m = re.search(_search_url_rgx, result.attrib['onclick'])
                except KeyError:
                    self.error('element {} has no onclick attribute'.format(
                        etree.tostring(result)))
                try:
                    _doc_path = m.groups()[0]
                except AttributeError:
                    self.error('no matches found for search_rgx')
                    self.debug('\n{r}\n{a}\n{u}'.format(
                        r=_search_url_rgx.pattern,
                        a=result.attrib['onclick'],
                        u=resp.request.url
                    ))
                _params = dict(parse_qsl(
                               urlparse(_doc_path).query))
                _params['Type'] = filing_type
                _params['Year'] = filing_year
                if _params:
                    yield _params
                else:
                    self.error('unable to parse {}'.format(
                        etree.tostring(result)))

    def parse_filing(self, filename, response):
        self._parser = UnitedStatesLobbyingRegistrationParser(
            self.jurisdiction,
            PARSE_DIR,
            strict_validation=True
        )

        self._parser.logger.level = logging.DEBUG

        forms = [f for f in self._parser.do_parse(root=response.content,
                                                  document_id=filename)]
        if len(forms) > 1:
            raise Exception('more than one form in a filing?')
        elif len(forms) == 0:
            raise Exception('no forms in filing {}'.format(filename))
        else:
            return forms[0]

    def transform_parse(self, parsed_form, response):

        # basic disclosure fields
        _disclosure = Disclosure(
            disclosure_id=parsed_form['id'],
            effective_date=parsed_form['datetimes']['effective_date']
        )

        _disclosure.add_authority(name=self.authority.name,
                                  id=self.authority._id)

        # disclosure extras
        _disclosure['extras'] = {}
        _disclosure['extras']['registrant'] = {
            'self_employed_individual':
                parsed_form['registrant']['self_employed_individual'],
            'general_description':
                parsed_form['registrant']['registrant_general_description'],
            'signature': {
                    "signature_date":
                        parsed_form['signature']['signature_date'],
                "signature":
                        parsed_form['signature']['signature']
                }
        }

        _disclosure['extras']['client'].update({
            'same_as_registrant':
                parsed_form['client']['client_self'],
            'general_description':
                parsed_form['client']['client_general_description']
        })

        _disclosure['extras']['registration_type'].update({
            'is_amendment':
                parsed_form['registration_type']['is_amendment'],
            'new_registrant':
                parsed_form['registration_type']['new_registrant'],
            'new_client_for_existing_registrant':
                parsed_form['registration_type']['new_client_for_existing_registrant'],
        })

        # # Registrant
        # build registrant
        if parsed_form['registrant']['self_employed_individual']:
            _registrant = Person(
                name=' '.join(
                    [p for p in [
                        parsed_form['registrant']['registrant_individual_prefix'],
                        parsed_form['registrant']['registrant_individual_firstname'],
                        parsed_form['registrant']['registrant_individual_lastname']]
                     if len(p) > 0])
            )
        else:
            _registrant = Organization(
                name=parsed_form['registrant']['registrant_org_name'],
                classification='corporation'
            )

        for scheme, ident in parsed_form['identifiers'].iteritems():
            _registrant.add_identifier(
                scheme='LDA/{}'.format(scheme),
                identifier=ident
            )

        registrant_contact_details = [
            {
                "type": "address",
                "label": "contact address",
                "value": '; '.join([
                    p for p in [
                        parsed_form['registrant']['registrant_address_one'],
                        parsed_form['registrant']['registrant_address_two'],
                        parsed_form['registrant']['registrant_city'],
                        parsed_form['registrant']['registrant_state'],
                        parsed_form['registrant']['registrant_zip'],
                        parsed_form['registrant']['registrant_country']]
                    if len(p) > 0]),
                "note": parsed_form['registrant']['registrant_contact_name']
            },
            {
                "type": "address",
                "label": "principal place of business",
                "value": '; '.join([
                    p for p in [
                        parsed_form['registrant']['registrant_ppb_city'],
                        parsed_form['registrant']['registrant_ppb_state'],
                        parsed_form['registrant']['registrant_ppb_zip'],
                        parsed_form['registrant']['registrant_ppb_country']]
                    if len(p) > 0]),
                "note": parsed_form['registrant']['registrant_contact_name']
            },
            {
                "type": "phone",
                "label": "contact phone",
                "value": parsed_form['registrant']['registrant_contact_phone'],
                "note": parsed_form['registrant']['registrant_contact_name']
            },
            {
                "type": "email",
                "label": "contact email",
                "value": parsed_form['registrant']['registrant_contact_email'],
                "note": parsed_form['registrant']['registrant_contact_name']
            },
        ]

        for cd in registrant_contact_details:
            _registrant.add_contact_detail(**cd)

        _registrant["extras"] = {
            "contact_details_structured": [
                {
                    "type": "address",
                    "label": "contact address",
                    "parts": [
                        {
                            "label": "address_one",
                            "value": parsed_form['registrant']['registrant_address_one'],
                        },
                        {
                            "label": "address_two",
                            "value": parsed_form['registrant']['registrant_address_two'],
                        },
                        {
                            "label": "city",
                            "value": parsed_form['registrant']['registrant_city'],
                        },
                        {
                            "label": "state",
                            "value": parsed_form['registrant']['registrant_state'],
                        },
                        {
                            "label": "zip",
                            "value": parsed_form['registrant']['registrant_zip'],
                        },
                        {
                            "label": "country",
                            "value": parsed_form['registrant']['registrant_country'],
                        }
                    ],
                    "note": "registrant contact on SOPR LD-1"
                },
                {
                    "type": "address",
                    "label": "principal place of business",
                    "parts": [
                        {
                            "label": "city",
                            "value": parsed_form['registrant']['registrant_ppb_city'],
                        },
                        {
                            "label": "state",
                            "value": parsed_form['registrant']['registrant_ppb_state'],
                        },
                        {
                            "label": "zip",
                            "value": parsed_form['registrant']['registrant_ppb_zip'],
                        },
                        {
                            "label": "country",
                            "value": parsed_form['registrant']['registrant_ppb_country'],
                        }
                    ],
                    "note": "registrant contact on SOPR LD-1"
                },
            ]
        }

        # # People
        # build contact
        _main_contact = Person(
            name=parsed_form['registrant']['registrant_contact_name']
        )

        main_contact_contact_details = [
            {
                "type": "phone",
                "label": "contact phone",
                "value": parsed_form['registrant']['registrant_contact_phone'],
                "note": parsed_form['registrant']['registrant_org_name']
            },
            {
                "type": "email",
                "label": "contact email",
                "value": parsed_form['registrant']['registrant_contact_email'],
                "note": parsed_form['registrant']['registrant_org_name']
            }
        ]

        for cd in main_contact_contact_details:
            _main_contact.add_contact_detail(**cd)

        if _registrant._type == 'organization':
            _registrant.add_member(name_or_person=_main_contact,
                                   role='main_contact')
        else:
            _disclosure.add_entity(name=_main_contact.name,
                                   entity_type=_main_contact._type,
                                   id=_main_contact._id,
                                   note='main_contact')
            _disclosure._related.append(_main_contact)

        # # Client
        # build client
        _client = Organization(
            name=parsed_form['client']['client_name'],
            classification='corporation'
        )

        client_contact_details = [
            {
                "type": "address",
                "label": "contact address",
                "value": '; '.join([
                    p for p in [
                        parsed_form['client']['client_address'],
                        parsed_form['client']['client_city'],
                        parsed_form['client']['client_state'],
                        parsed_form['client']['client_zip'],
                        parsed_form['client']['client_country']]
                    if len(p) > 0]),
                "note": parsed_form['client']['client_name']
            },
            {
                "type": "address",
                "label": "principal place of business",
                "value": '; '.join([
                    p for p in [
                        parsed_form['client']['client_ppb_city'],
                        parsed_form['client']['client_ppb_state'],
                        parsed_form['client']['client_ppb_zip'],
                        parsed_form['client']['client_ppb_country']]
                    if len(p) > 0]),
                "note": parsed_form['client']['client_name']
            },
        ]

        for cd in client_contact_details:
            _client.add_contact_detail(**cd)

        _client["extras"] = {
            "contact_details_structured": [
                {
                    "type": "address",
                    "label": "contact address",
                    "parts": [
                        {
                            "label": "address",
                            "value": parsed_form['client']['client_address'],
                        },
                        {
                            "label": "city",
                            "value": parsed_form['client']['client_city'],
                        },
                        {
                            "label": "state",
                            "value": parsed_form['client']['client_state'],
                        },
                        {
                            "label": "zip",
                            "value": parsed_form['client']['client_zip'],
                        },
                        {
                            "label": "country",
                            "value": parsed_form['client']['client_country'],
                        }
                    ],
                    "note": "client contact on SOPR LD-1"
                },
                {
                    "type": "address",
                    "label": "principal place of business",
                    "parts": [
                        {
                            "label": "city",
                            "value": parsed_form['client']['client_ppb_city'],
                        },
                        {
                            "label": "state",
                            "value": parsed_form['client']['client_ppb_state'],
                        },
                        {
                            "label": "zip",
                            "value": parsed_form['client']['client_ppb_zip'],
                        },
                        {
                            "label": "country",
                            "value": parsed_form['client']['client_ppb_country'],
                        }
                    ],
                    "note": "client contact on SOPR LD-1"
                },
            ],
        }

        _foreign_entities = []
        for fe in parsed_form['foreign_entities']['foreign_entities']:
            _foreign_entity = Organization(
                name=fe['foreign_entity_name'],
                classification='corporation'
            )

            foreign_entity_contact_details = [
                {
                    "type": "address",
                    "label": "contact address",
                    "value": '; '.join([
                        p for p in [
                            fe['foreign_entity_address'],
                            fe['foreign_entity_city'],
                            fe['foreign_entity_state'],
                            fe['foreign_entity_country']]
                        if len(p) > 0]),
                },
                {
                    "type": "address",
                    "label": "principal place of business",
                    "value": '; '.join([
                        p for p in [
                            fe['foreign_entity_ppb_state'],
                            fe['foreign_entity_ppb_country']]
                        if len(p) > 0]),
                },
            ]

            for cd in foreign_entity_contact_details:
                _foreign_entity.add_contact_detail(**cd)

            _foreign_entity["extras"] = {
                "contact_details_structured": [
                    {
                        "type": "address",
                        "label": "contact address",
                        "parts": [
                            {
                                "label": "address",
                                "value": fe['foreign_entity_address'],
                            },
                            {
                                "label": "city",
                                "value": fe['foreign_entity_city'],
                            },
                            {
                                "label": "state",
                                "value": fe['foreign_entity_state'],
                            },
                            {
                                "label": "country",
                                "value": fe['foreign_entity_country'],
                            }
                        ],
                        "note": "foreign_entity contact on SOPR LD-1"
                    },
                    {
                        "type": "address",
                        "label": "principal place of business",
                        "parts": [
                            {
                                "label": "state",
                                "value": fe['foreign_entity_ppb_state'],
                            },
                            {
                                "label": "country",
                                "value": fe['foreign_entity_ppb_country'],
                            }
                        ],
                        "note": "foreign_entity contact on SOPR LD-1"
                    },
                ],
            }

            _foreign_entities.append(_foreign_entity)

            # _client['memberships'].append({
            #     "id": _foreign_entity['id'],
            #     "classification": "organization",
            #     "name": _foreign_entity['name'],
            #     "extras": {
            #         "ownership_percentage":
            #             fe['foreign_entity_amount']
            #     }
            # })

        _lobbyists = []
        for l in parsed_form['lobbyists']['lobbyists']:
            _lobbyist = Person(
                name=' '.join([
                    l['lobbyist_first_name'],
                    l['lobbyist_last_name'],
                    l['lobbyist_suffix']
                ])
            )
            _lobbyist['extras']['lda_covered_official_positions'] = []
            if l['lobbyist_covered_official_position']:
                _lobbyist['extras']['lda_covered_official_positions'].append({
                    'date_reported':
                        parsed_form['datetimes']['effective_date'],
                    'disclosure_id':
                        _disclosure['id'],
                    'covered_official_position':
                        l['lobbyist_covered_official_position'],
                })
            _registrant.add_member(_lobbyist, role='lobbyist')
            _lobbyists.append(_lobbyist)

        # # Document
        # build document
        _disclosure.add_document(
            note='submitted filing',
            date=parsed_form['datetimes']['effective_date'],
            url=response.request.url
        )

        # Affiliated orgs
        _affiliated_organizations = []
        for ao in parsed_form['affiliated_organizations']['affiliated_organizations']:
            _affiliated_organization = Organization(
                name=ao['affiliated_organization_name'],
                classification='corporation'
            )
            affiliated_organization_contact_details = [
                {
                    "type": "address",
                    "label": "contact address",
                    "value": '; '.join([
                        p for p in [
                            ao['affiliated_organization_address'],
                            ao['affiliated_organization_city'],
                            ao['affiliated_organization_state'],
                            ao['affiliated_organization_zip'],
                            ao['affiliated_organization_country']]
                        if len(p) > 0]),
                },
                {
                    "type": "address",
                    "label": "principal place of business",
                    "value": '; '.join([
                        p for p in [
                            ao['affiliated_organization_ppb_city'],
                            ao['affiliated_organization_ppb_state'],
                            ao['affiliated_organization_ppb_country']]
                        if len(p) > 0]),
                },
            ]

            for cd in affiliated_organization_contact_details:
                _affiliated_organization.add_contact_detail(**cd)

            _affiliated_organization["extras"] = {
                "contact_details_structured": [
                    {
                        "type": "address",
                        "label": "contact address",
                        "parts": [
                            {
                                "label": "address",
                                "value": ao['affiliated_organization_address'],
                            },
                            {
                                "label": "city",
                                "value": ao['affiliated_organization_city'],
                            },
                            {
                                "label": "state",
                                "value": ao['affiliated_organization_state'],
                            },
                            {
                                "label": "zip",
                                "value": ao['affiliated_organization_zip'],
                            },
                            {
                                "label": "country",
                                "value": ao['affiliated_organization_country'],
                            }
                        ],
                        "note": "affiliated organization contact on SOPR LD-1"
                    },
                    {
                        "type": "address",
                        "label": "principal place of business",
                        "parts": [
                            {
                                "label": "city",
                                "value":
                                    ao['affiliated_organization_ppb_city'],
                            },
                            {
                                "label": "state",
                                "value":
                                    ao['affiliated_organization_ppb_state'],
                            },
                            {
                                "label": "country",
                                "value":
                                    ao['affiliated_organization_ppb_country'],
                            }
                        ],
                        "note": "affiliated organization contact on SOPR LD-1"
                    },
                ],
            }
            _affiliated_organizations.append(_affiliated_organization)

        # # Events & Agendas
        # name (TODO: make fct for name gen)
        # start
        # client-reg registration (w/ issue codes/detail)
        # client-reg-lobbyist registration
        # build lobbyists on the fly

        if parsed_form['registration_type']['new_registrant']:
            registration_type = 'New Client, New Registrant'
        elif parsed_form['registration_type']['is_amendment']:
            registration_type = 'Amended Registration'
        else:
            registration_type = 'New Client for Existing Registrant'

        _event = Event(
            name="{rn} - {rt}".format(rn=_registrant.name,
                                      rt=registration_type),
            start_time=parsed_form['datetimes']['effective_date'],
            classification='disclosed-event'
        )

        _event['participants'].append({
            "entity_type": _registrant._type,
            "id": _registrant['id'],
            "name": _registrant['name'],
            "note": "registrant"
        })

        if _registrant._type == 'person':
            _event['participants'].append({
                "entity_type": "person",
                "id": _registrant['id'],
                "name": _registrant['name'],
                "note": "lobbyist"
            })

        _event['participants'].append({
            "entity_type": "organization",
            "id": _client['id'],
            "name": _client['name'],
            "note": "client"
        })

        for l in _lobbyists:
            _event.add_participant({
                "entity_type": "person",
                "id": l['id'],
                "name": l['name'],
                "note": "lobbyist"
            })

        for fe in _foreign_entities:
            _event.add_participant({
                "entity_type": "organization",
                "id": fe['id'],
                "name": fe['name'],
                "note": "foreign_entity"
            })

        for ao in _affiliated_organizations:
            _event.add_participant({
                "entity_type": "organization",
                "id": ao['id'],
                "name": ao['name'],
                "note": "affiliated_organization"
            })

        _agenda = _event.add_agenda_item(
            description='issues lobbied on',
        )

        _agenda['notes'].append(
            parsed_form['lobbying_issues_detail']['lobbying_issues_detail']
        )

        for li in parsed_form['lobbying_issues']['lobbying_issues']:
            _agenda['subjects'].append(li['general_issue_area'])

        _disclosure.add_disclosed_event(_event)

        # add registrant to disclosure's _related and related_entities fields
        _disclosure.add_registrant(_registrant)

        _disclosure._related.append(_event)
        _disclosure._related.append(_agenda)
        for ao in _affiliated_organizations:
            _disclosure._related.append(ao)
        for fe in _foreign_entities:
            _disclosure._related.append(fe)
        for l in _lobbyists:
            _disclosure._related.append(l)

        return _disclosure

    def scrape(self, start_date=None, end_date=None):

        self._build_date_range(start_date, end_date)
        for authority in self.scrape_authority():
            yield authority

        for params in self.search_filings():
            filename, response = self.urlretrieve(
                self.base_url,
                filename=params['filingID'],
                method='POST',
                body=params,
                dir=self.cache_dir
            )
            parsed_form = self.parse_filing(filename, response)
            disclosure = self.transform_parse(parsed_form, response)
            yield disclosure
