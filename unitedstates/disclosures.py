import os
import json
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qsl

from lxml.html import HTMLParser
from lxml import etree

from pytz import timezone

from pupa import settings
from pupa.scrape import BaseDisclosureScraper
from pupa.scrape import Disclosure, Person, Organization, Event

from unitedstates.ref import sopr_lobbying_reference

from .form_parsing.utils import mkdir_p

from .form_parsing import UnitedStatesLobbyingRegistrationParser

NY_TZ = timezone('America/New_York')


class UnitedStatesLobbyingDisclosureScraper(BaseDisclosureScraper):
    base_url = 'http://soprweb.senate.gov/index.cfm'
    start_date = datetime.today()
    end_date = datetime.today()
    filing_types = sopr_lobbying_reference.FILING_TYPES
    parse_dir = settings.PARSED_FORM_DIR

    def _build_date_range(self, start_date, end_date):
        if start_date:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

    def search_filings(self):
        search_form = {'datePostedStart': datetime.strftime(self.start_date,
                                                            '%m/%d/%Y'),
                       'datePostedEnd': datetime.strftime(self.end_date,
                                                          '%m/%d/%Y')}

        _search_url_rgx = re.compile(r"window\.open\('(.*?)'\)",
                                     re.IGNORECASE)

        search_params = {'event': 'processSearchCriteria'}

        for filing_type in self.filing_types:
            _form = search_form.copy()
            _form['reportType'] = filing_type['code']
            _form.update(search_params)
            self.debug('making request with {f}'.format(f=_form))
            _, response = self.urlretrieve(
                self.base_url,
                method='POST',
                body=_form,
            )
            d = etree.fromstring(response.text, parser=HTMLParser())
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
                        u=response.request.url
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
        mkdir_p(self.parse_dir)

        self.build_parser()
        doc_id = os.path.basename(os.path.splitext(filename)[0])

        forms = [f for f in self._parser.do_parse(root=response.content,
                                                  document_id=doc_id)]
        if len(forms) > 1:
            raise Exception('more than one form in a filing?')
        elif len(forms) == 0:
            raise Exception('no forms in filing {}'.format(filename))
        else:
            return forms[0]

    def scrape(self, start_date=None, end_date=None):
        self.authority = self.jurisdiction._sopr

        self._build_date_range(start_date, end_date)

        for params in self.search_filings():
            filename, response = self.urlretrieve(
                self.base_url,
                filename=os.path.join(
                    settings.CACHE_DIR,
                    '{fn}.html'.format(fn=params['filingID'])
                ),
                method='POST',
                body=params
            )
            parsed_form = self.parse_filing(filename, response)
            disclosure = self.transform_parse(parsed_form, response)
            yield disclosure


class UnitedStatesLobbyingRegistrationDisclosureScraper(
        UnitedStatesLobbyingDisclosureScraper):
    filing_types = [ft for ft in sopr_lobbying_reference.FILING_TYPES
                    if ft['action'] == 'registration']

    def build_parser(self):
        self._parser = UnitedStatesLobbyingRegistrationParser(
            self.jurisdiction,
            self.parse_dir,
            strict_validation=True
        )

    def transform_parse(self, parsed_form, response):

        _source = {
            "url": "?".join([response.request.url, response.request.body]),
            "note": "LDA Form LD-1"
        }

        # basic disclosure fields
        _disclosure = Disclosure(
            effective_date=parsed_form['datetimes']['effective_date'],
            timezone='America/New_York',
            submitted_date=parsed_form['datetimes']['signature_date'],
            classification="lobbying"
        )

        _disclosure.add_authority(name=self.authority.name,
                                  type=self.authority._type,
                                  id=self.authority._id)

        _disclosure.add_identifier(
            identifier=parsed_form['_meta']['document_id'],
            scheme="urn:sopr:filing"
        )

        # disclosure extras
        _disclosure.extras = {}
        _disclosure.extras['registrant'] = {
            'self_employed_individual': parsed_form['registrant']['self_employed_individual'],
            'general_description': parsed_form['registrant']['registrant_general_description'],
            'signature': {
                "signature_date": parsed_form['datetimes']['signature_date'],
                "signature": parsed_form['signature']
            }
        }

        _disclosure.extras['client'] = {
            'same_as_registrant':
                parsed_form['client']['client_self'],
            'general_description':
                parsed_form['client']['client_general_description']
        }

        _disclosure.extras['registration_type'] = {
            'is_amendment':
                parsed_form['registration_type']['is_amendment'],
            'new_registrant':
                parsed_form['registration_type']['new_registrant'],
            'new_client_for_existing_registrant':
                parsed_form['registration_type'][
                    'new_client_for_existing_registrant'],
        }

        # # Registrant
        # build registrant
        if parsed_form['registrant']['self_employed_individual']:
            _registrant = Person(
                name=' '.join(
                    [p for p in [
                        parsed_form['registrant'][
                            'registrant_individual_prefix'],
                        parsed_form['registrant'][
                            'registrant_individual_firstname'],
                        parsed_form['registrant'][
                            'registrant_individual_lastname']]
                     if len(p) > 0]).strip()
            )
        else:
            _registrant = Organization(
                name=parsed_form['registrant']['registrant_org_name'],
                classification='company'
            )

        _registrant.add_identifier(
            identifier=parsed_form['registrant']['registrant_house_id'],
            scheme='urn:house_clerk:registrant'
        )

        _registrant.add_identifier(
            identifier=parsed_form['registrant']['registrant_senate_id'],
            scheme='urn:sopr:registrant'
        )

        registrant_contact_details = [
            {
                "type": "address",
                "note": "contact address",
                "value": '; '.join([
                    p for p in [
                        parsed_form['registrant']['registrant_address_one'],
                        parsed_form['registrant']['registrant_address_two'],
                        parsed_form['registrant']['registrant_city'],
                        parsed_form['registrant']['registrant_state'],
                        parsed_form['registrant']['registrant_zip'],
                        parsed_form['registrant']['registrant_country']]
                    if len(p) > 0]).strip(),
            },
            {
                "type": "voice",
                "note": "contact phone",
                "value": parsed_form['registrant']['registrant_contact_phone'],
            },
            {
                "type": "email",
                "note": "contact email",
                "value": parsed_form['registrant']['registrant_contact_email'],
            },
        ]

        registrant_contact_ppb = {
            "type": "address",
            "note": "principal place of business",
            "value": '; '.join([
                p for p in [
                    parsed_form['registrant']['registrant_ppb_city'],
                    parsed_form['registrant']['registrant_ppb_state'],
                    parsed_form['registrant']['registrant_ppb_zip'],
                    parsed_form['registrant']['registrant_ppb_country']]
                if len(p) > 0]).strip(),
        }

        if registrant_contact_ppb["value"]:
            registrant_contact_details.append(registrant_contact_ppb)

        for cd in registrant_contact_details:
            _registrant.add_contact_detail(**cd)

        _registrant.extras = {
            "contact_details_structured": [
                {
                    "type": "address",
                    "note": "contact address",
                    "parts": [
                        {
                            "note": "address_one",
                            "value": parsed_form['registrant'][
                                'registrant_address_one'],
                        },
                        {
                            "note": "address_two",
                            "value": parsed_form['registrant'][
                                'registrant_address_two'],
                        },
                        {
                            "note": "city",
                            "value": parsed_form['registrant'][
                                'registrant_city'],
                        },
                        {
                            "note": "state",
                            "value": parsed_form['registrant'][
                                'registrant_state'],
                        },
                        {
                            "note": "zip",
                            "value": parsed_form['registrant'][
                                'registrant_zip'],
                        },
                        {
                            "note": "country",
                            "value": parsed_form['registrant'][
                                'registrant_country'],
                        }
                    ],
                },
                {
                    "type": "address",
                    "note": "principal place of business",
                    "parts": [
                        {
                            "note": "city",
                            "value": parsed_form['registrant'][
                                'registrant_ppb_city'],
                        },
                        {
                            "note": "state",
                            "value": parsed_form['registrant'][
                                'registrant_ppb_state'],
                        },
                        {
                            "note": "zip",
                            "value": parsed_form['registrant'][
                                'registrant_ppb_zip'],
                        },
                        {
                            "note": "country",
                            "value": parsed_form['registrant'][
                                'registrant_ppb_country'],
                        }
                    ],
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
                "type": "voice",
                "note": "contact phone",
                "value": parsed_form['registrant']['registrant_contact_phone'],
            },
            {
                "type": "email",
                "note": "contact email",
                "value": parsed_form['registrant']['registrant_contact_email'],
            }
        ]

        for cd in main_contact_contact_details:
            _main_contact.add_contact_detail(**cd)

        if _registrant._type == 'organization':
            _registrant.add_member(name_or_person=_main_contact,
                                   role='main_contact')
        else:
            _disclosure.add_entity(name=_main_contact.name,
                                   type=_main_contact._type,
                                   id=_main_contact._id,
                                   note='main_contact')
            _disclosure._related.append(_main_contact)

        # # Client
        # build client
        _client = Organization(
            name=parsed_form['client']['client_name'],
            classification='company'
        )

        client_contact_details = [
            {
                "type": "address",
                "note": "contact address",
                "value": '; '.join([
                    p for p in [
                        parsed_form['client']['client_address'],
                        parsed_form['client']['client_city'],
                        parsed_form['client']['client_state'],
                        parsed_form['client']['client_zip'],
                        parsed_form['client']['client_country']]
                    if len(p) > 0]).strip(),
            },
        ]

        client_contact_ppb = {
            "type": "address",
            "note": "principal place of business",
            "value": '; '.join([
                p for p in [
                    parsed_form['client']['client_ppb_city'],
                    parsed_form['client']['client_ppb_state'],
                    parsed_form['client']['client_ppb_zip'],
                    parsed_form['client']['client_ppb_country']]
                if len(p) > 0]).strip(),
        }

        if client_contact_ppb["value"]:
            client_contact_details.append(client_contact_ppb)

        for cd in client_contact_details:
            _client.add_contact_detail(**cd)

        _client.extras = {
            "contact_details_structured": [
                {
                    "type": "address",
                    "note": "contact address",
                    "parts": [
                        {
                            "note": "address",
                            "value": parsed_form['client']['client_address'],
                        },
                        {
                            "note": "city",
                            "value": parsed_form['client']['client_city'],
                        },
                        {
                            "note": "state",
                            "value": parsed_form['client']['client_state'],
                        },
                        {
                            "note": "zip",
                            "value": parsed_form['client']['client_zip'],
                        },
                        {
                            "note": "country",
                            "value": parsed_form['client']['client_country'],
                        }
                    ],
                },
                {
                    "type": "address",
                    "note": "principal place of business",
                    "parts": [
                        {
                            "note": "city",
                            "value": parsed_form['client']['client_ppb_city'],
                        },
                        {
                            "note": "state",
                            "value": parsed_form['client']['client_ppb_state'],
                        },
                        {
                            "note": "zip",
                            "value": parsed_form['client']['client_ppb_zip'],
                        },
                        {
                            "note": "country",
                            "value": parsed_form['client'][
                                'client_ppb_country'],
                        }
                    ],
                },
            ],
        }

        _foreign_entities = []
        for fe in parsed_form['foreign_entities']:
            _foreign_entity = Organization(
                name=fe['foreign_entity_name'],
                classification='company'
            )

            foreign_entity_contact_details = [
                {
                    "type": "address",
                    "note": "contact address",
                    "value": '; '.join([
                        p for p in [
                            fe['foreign_entity_address'],
                            fe['foreign_entity_city'],
                            fe['foreign_entity_state'],
                            fe['foreign_entity_country']]
                        if len(p) > 0]).strip(),
                },
                {
                    "type": "address",
                    "note": "principal place of business",
                    "value": '; '.join([
                        p for p in [
                            fe['foreign_entity_ppb_state'],
                            fe['foreign_entity_ppb_country']]
                        if len(p) > 0]).strip(),
                },
            ]

            foreign_entity_contact_ppb = {
                "type": "address",
                "note": "principal place of business",
                "value": '; '.join([
                    p for p in [
                        fe['foreign_entity_ppb_city'],
                        fe['foreign_entity_ppb_state'],
                        fe['foreign_entity_ppb_country']]
                    if len(p) > 0]),
            }

            if foreign_entity_contact_ppb["value"]:
                foreign_entity_contact_details.append(
                    foreign_entity_contact_ppb)

            for cd in foreign_entity_contact_details:
                if cd['value'] != '':
                    _foreign_entity.add_contact_detail(**cd)

            _foreign_entity.extras = {
                "contact_details_structured": [
                    {
                        "type": "address",
                        "note": "contact address",
                        "parts": [
                            {
                                "note": "address",
                                "value": fe['foreign_entity_address'],
                            },
                            {
                                "note": "city",
                                "value": fe['foreign_entity_city'],
                            },
                            {
                                "note": "state",
                                "value": fe['foreign_entity_state'],
                            },
                            {
                                "note": "country",
                                "value": fe['foreign_entity_country'],
                            }
                        ],
                    },
                    {
                        "type": "address",
                        "note": "principal place of business",
                        "parts": [
                            {
                                "note": "state",
                                "value": fe['foreign_entity_ppb_state'],
                            },
                            {
                                "note": "country",
                                "value": fe['foreign_entity_ppb_country'],
                            }
                        ],
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
        for l in parsed_form['lobbyists']:
            _lobbyist = Person(
                name=' '.join([
                    l['lobbyist_first_name'],
                    l['lobbyist_last_name'],
                    l['lobbyist_suffix']
                ]).strip()
            )
            _lobbyist.extras['lda_covered_official_positions'] = []
            if l['lobbyist_covered_official_position']:
                _lobbyist.extras['lda_covered_official_positions'].append({
                    'date_reported':
                        parsed_form['datetimes']['effective_date'],
                    'disclosure_id':
                        _disclosure._id,
                    'covered_official_position':
                        l['lobbyist_covered_official_position'],
                })
            _registrant.add_member(_lobbyist, role='lobbyist')
            _lobbyists.append(_lobbyist)

        # # Document
        # build document
        _disclosure.add_document(
            note='submitted filing',
            date=parsed_form['datetimes']['effective_date'][:10],
            url=response.request.url
        )

        # Affiliated orgs
        _affiliated_organizations = []
        for ao in parsed_form['affiliated_organizations']:
            _affiliated_organization = Organization(
                name=ao['affiliated_organization_name'],
                classification='company'
            )
            affiliated_organization_contact_details = [
                {
                    "type": "address",
                    "note": "contact address",
                    "value": '; '.join([
                        p for p in [
                            ao['affiliated_organization_address'],
                            ao['affiliated_organization_city'],
                            ao['affiliated_organization_state'],
                            ao['affiliated_organization_zip'],
                            ao['affiliated_organization_country']]
                        if len(p) > 0]).strip(),
                },
            ]

            affiliated_organization_contact_ppb = {
                "type": "address",
                "note": "principal place of business",
                "value": '; '.join([
                    p for p in [
                        ao['affiliated_organization_ppb_city'],
                        ao['affiliated_organization_ppb_state'],
                        ao['affiliated_organization_ppb_country']]
                    if len(p) > 0]).strip(),
            }

            if affiliated_organization_contact_ppb["value"]:
                affiliated_organization_contact_details.append(
                    affiliated_organization_contact_ppb)

            for cd in affiliated_organization_contact_details:
                _affiliated_organization.add_contact_detail(**cd)

            _affiliated_organization.extras = {
                "contact_details_structured": [
                    {
                        "type": "address",
                        "note": "contact address",
                        "parts": [
                            {
                                "note": "address",
                                "value": ao['affiliated_organization_address'],
                            },
                            {
                                "note": "city",
                                "value": ao['affiliated_organization_city'],
                            },
                            {
                                "note": "state",
                                "value": ao['affiliated_organization_state'],
                            },
                            {
                                "note": "zip",
                                "value": ao['affiliated_organization_zip'],
                            },
                            {
                                "note": "country",
                                "value": ao['affiliated_organization_country'],
                            }
                        ],
                    },
                    {
                        "type": "address",
                        "note": "principal place of business",
                        "parts": [
                            {
                                "note": "city",
                                "value":
                                    ao['affiliated_organization_ppb_city'],
                            },
                            {
                                "note": "state",
                                "value":
                                    ao['affiliated_organization_ppb_state'],
                            },
                            {
                                "note": "country",
                                "value":
                                    ao['affiliated_organization_ppb_country'],
                            }
                        ],
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

        # Create registration event
        _event = Event(
            name="{rn} - {rt}".format(rn=_registrant.name,
                                      rt=registration_type),
            timezone='America/New_York',
            location='United States',
            start_time=datetime.strptime(
                parsed_form['datetimes']['effective_date'],
                '%Y-%m-%d %H:%M:%S').replace(tzinfo=NY_TZ),
            classification='registration'
        )

        # add participants
        _event.add_participant(type=_registrant._type,
                               id=_registrant._id,
                               name=_registrant.name,
                               note="registrant")

        if _registrant._type == 'person':
            _event.add_participant(type=_registrant._type,
                                   id=_registrant._id,
                                   name=_registrant.name,
                                   note="registrant")

        _event.add_participant(type=_client._type,
                               id=_client._id,
                               name=_client.name,
                               note="client")

        for l in _lobbyists:
            _event.add_participant(type=l._type,
                                   id=l._id,
                                   name=l.name,
                                   note='lobbyist')

        for fe in _foreign_entities:
            _event.add_participant(type=fe._type,
                                   id=fe._id,
                                   name=fe.name,
                                   note='foreign_entity')

        for ao in _affiliated_organizations:
            _event.add_participant(type=ao._type,
                                   id=ao._id,
                                   name=ao.name,
                                   note='affiliated_organization')

        # add agenda item
        _agenda = _event.add_agenda_item(
            description='issues lobbied on',
        )

        _agenda['notes'].append(
            parsed_form['lobbying_issues_detail']
        )

        for li in parsed_form['lobbying_issues']:
            if li['general_issue_area'] != '':
                _agenda.add_subject(li['general_issue_area'])

        _disclosure.add_disclosed_event(_event)

        # add registrant to disclosure's _related and related_entities fields
        _disclosure.add_registrant(name=_registrant.name,
                                   type=_registrant._type,
                                   id=_registrant._id)

        _registrant.add_source(**_source)
        yield _registrant

        _client.add_source(**_source)
        yield _client

        _main_contact.add_source(**_source)
        yield _main_contact

        for ao in _affiliated_organizations:
            ao.add_source(**_source)
            yield ao
        for fe in _foreign_entities:
            fe.add_source(**_source)
            yield fe
        for l in _lobbyists:
            l.add_source(**_source)
            yield l

        _event.add_source(**_source)
        yield _event
        _disclosure.add_source(**_source)
        yield _disclosure
