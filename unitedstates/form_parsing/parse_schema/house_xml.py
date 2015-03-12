
from ..utils.data_munge import clean_text, parse_date

from .common import pupa_date


post_employment_schema = {
    "title": "House Post-Employment Lobbying Restriction",
    "description": "Lobbying restriction reported by the House Clerk's Office",
    "type": "object",
    "object_path": "/PostEmployment/Employee",
    "properties": {
        "_meta": {
            "type": "object",
            "properties": {
                "document_id": {
                    "type": "string",
                    "format": "uuid_hex",
                },
            }
        },
        "employee_name": {
            "type": "string",
            'path': 'EmployeeName',
            'parser': clean_text,
        },
        "office_name": {
            "type": ["string"],
            'path': 'OfficeName',
            'parser': clean_text,
        },
        "termination_date": pupa_date({
            'path': 'TerminationDate',
            'parser': parse_date
        }),
        "lobbying_eligibility_date": pupa_date({
            'path': 'LobbyingEligibilityDate',
            'parser': parse_date
        }),
    }
}
