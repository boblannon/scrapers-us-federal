from ..utils.data_munge import clean_text, parse_date

from .common import pupa_date


post_employment_schema = {
    "title": "Senate Post-Employment Lobbying Restriction",
    "description": "Lobbying restriction reported by the Senate Office of Public Record",
    "type": "object",
    "object_path": "/post_employment_lobbying_restrictions/previous_employee",
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
        "name": {
            "type": "object",
            "properties": {
                "name_first": {
                    "type": ["string"],
                    'path': 'name/first',
                    'parser': clean_text,
                },
                "name_middle": {
                    "type": ["string", "null"],
                    'path': 'name/middle',
                    'parser': clean_text,
                    'blank': True,
                    'missing': True
                },
                "name_last": {
                    "type": ["string"],
                    'path': 'name/last',
                    'parser': clean_text,
                },
            }
        },
        "office_name": {
            "type": ["string"],
            'path': 'office_name',
            'parser': clean_text,
        },
        "restriction_period": {
            "type": "object",
            "properties": {
                "restriction_period_begin_date": pupa_date({
                    'path': 'restriction_period/begin_date',
                    'parser': parse_date
                }),
                "restriction_period_end_date": pupa_date({
                    'path': 'restriction_period/end_date',
                    'parser': parse_date
                }),
            }
        }
    }
}
