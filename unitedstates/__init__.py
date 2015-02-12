from pupa.scrape import Jurisdiction, Organization
from .legislative import UnitedStatesLegislativeScraper
from .disclosures import UnitedStatesLobbyingRegistrationDisclosureScraper


class UnitedStates(Jurisdiction):
    classification = 'government'
    division_id = 'ocd-division/country:us'

    name = 'United States Federal Government'
    url = 'http://usa.gov/'

    parties = [
        {"name": "Republican", },
        {"name": "Democratic", },
        {"name": "Independent", },
    ]

    scrapers = {
        "congress": UnitedStatesLegislativeScraper,
        "lobbying_registrations":
            UnitedStatesLobbyingRegistrationDisclosureScraper,
        # Executive Scraper here
    }

    def get_organizations(self):
        legislature = Organization("United States Congress",
                                   classification='legislature')
        yield legislature
