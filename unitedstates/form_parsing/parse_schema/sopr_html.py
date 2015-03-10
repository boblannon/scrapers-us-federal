import copy

from unitedstates.ref import sopr_lobbying_reference

from ..utils.data_munge import clean_text, checkbox_boolean, parse_datetime

from .common import pupa_datetime_blank 


sopr_general_issue_codes = [i['issue_code'] for i in
                            sopr_lobbying_reference.GENERAL_ISSUE_CODES]


ld1_schema = {
    "title": "Lobbying Registration",
    "description": "Lobbying Disclosure Act of 1995 (Section 4)",
    "type": "object",
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
        "affiliated_organizations_url": {
            "type": ["null", "string"],
            "format": "url_http",
            "missing": True,
            "blank": True,
            'path': '/html/body/table[15]/tbody/td[2]/div',
            'parser': clean_text
        },
        "signature": {
            "type": "string",
            "blank": False,
            'path': '/html/body/table[20]/tbody/tr/td[2]/div',
            'parser': clean_text
        },
        "datetimes": {
            "type": "object",
            "properties": {
                "signature_date": pupa_datetime_blank({
                    'path': '/html/body/table[20]/tbody/tr/td[4]/div',
                    'parser': parse_datetime
                }),
                "effective_date": pupa_datetime_blank({
                    'path': '/html/body/table[2]/tbody/tr[1]/td[3]/div',
                    'parser': parse_datetime
                })
            }
        },
        "registration_type": {
            "type": "object",
            "properties": {
                "new_registrant": {
                    "type": "boolean",
                    'path': '/html/body/div[1]/input[1]',
                    'parser': checkbox_boolean
                },
                "new_client_for_existing_registrant": {
                    "type": "boolean",
                    'path': '/html/body/div[1]/input[2]',
                    'parser': checkbox_boolean
                },
                "is_amendment": {
                    "type": "boolean",
                    'path': '/html/body/div[1]/input[3]',
                    'parser': checkbox_boolean
                }
            }
        },
        "registrant": {
            "type": "object",
            "properties": {
                "organization_or_lobbying_firm": {
                    "type": "boolean",
                    'path': '/html/body/p[3]/input[1]',
                    'parser': checkbox_boolean
                },
                "self_employed_individual": {
                    "type": "boolean",
                    'path': '/html/body/p[3]/input[2]',
                    'parser': checkbox_boolean
                },
                "registrant_org_name": {
                    "type": ["null", "string"],
                    'path': '/html/body/table[3]/tbody/tr/td[contains(.,"Organization")]/following-sibling::td[1]/div',
                    'parser': clean_text,
                    'missing': True,
                },
                "registrant_individual_prefix": {
                    "type": ["null", "string"],
                    'path': '/html/body/table[3]/tbody/tr/td[contains(.,"Prefix")]/following-sibling::td[1]/div',
                    'parser': clean_text,
                    'missing': True,
                },
                "registrant_individual_firstname": {
                    "type": ["null", "string"],
                    'path': '/html/body/table[3]/tbody/tr/td[5]/div',
                    'parser': clean_text,
                    'missing': True,
                },
                "registrant_individual_lastname": {
                    "type": ["null", "string"],
                    'path': '/html/body/table[3]/tbody/tr/td[7]/div',
                    'parser': clean_text,
                    'missing': True,
                },
                "registrant_address_one": {
                    "type": "string",
                    'path': '/html/body/table[4]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "registrant_address_two": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[4]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "registrant_city": {
                    "type": "string",
                    'path': '/html/body/table[5]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "registrant_state": {
                    "type": "string",
                    "pattern": "[A-Z]{2}",
                    'path': '/html/body/table[5]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "registrant_zip": {
                    "type": "string",
                    "pattern": "^\d{5}(?:[-\s]\d{4})?$",
                    'path': '/html/body/table[5]/tbody/tr/td[6]/div',
                    'parser': clean_text
                },
                "registrant_country": {
                    "type": "string",
                    'path': '/html/body/table[5]/tbody/tr/td[8]/div',
                    'parser': clean_text
                },
                "registrant_ppb_city": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[6]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "registrant_ppb_state": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[6]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "registrant_ppb_zip": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[6]/tbody/tr/td[6]/div',
                    'parser': clean_text
                },
                "registrant_ppb_country": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[6]/tbody/tr/td[8]/div',
                    'parser': clean_text
                },
                "registrant_international_phone": {
                    "type": "boolean",
                    'path': '/html/body/table[7]/tbody/tr/td[2]/input',
                    'parser': checkbox_boolean
                },
                "registrant_contact_name": {
                    "type": "string",
                    'path': '/html/body/table[8]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "registrant_contact_phone": {
                    "type": "string",
                    'path': '/html/body/table[8]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "registrant_contact_email": {
                    "type": "string",
                    "format": "email",
                    'path': '/html/body/table[8]/tbody/tr/td[6]/div',
                    'parser': clean_text
                },
                "registrant_general_description": {
                    "type": "string",
                    'path': '/html/body/div[2]',
                    'parser': clean_text
                },
                "registrant_house_id": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[2]/tbody/tr[2]/td[2]/div',
                    'parser': clean_text
                },
                "registrant_senate_id": {
                    "type": "string",
                    'path': '/html/body/table[2]/tbody/tr[2]/td[5]/div',
                    'parser': clean_text
                }
            }
        },
        "client": {
            "type": "object",
            "properties": {
                "client_self": {
                    "type": "boolean",
                    'path': '/html/body/p[4]/input',
                    'parser': checkbox_boolean
                },
                "client_name": {
                    "type": "string",
                    'path': '/html/body/table[9]/tbody/tr[1]/td[2]/div',
                    'parser': clean_text
                },
                "client_general_description": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/div[3]',
                    'parser': clean_text
                },
                "client_address": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[9]/tbody/tr[2]/td[2]/div',
                    'parser': clean_text
                },
                "client_city": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[10]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "client_state": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[10]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "client_zip": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[10]/tbody/tr/td[6]/div',
                    'parser': clean_text
                },
                "client_country": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[10]/tbody/tr/td[8]/div',
                    'parser': clean_text
                },
                "client_ppb_city": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[11]/tbody/tr/td[2]/div',
                    'parser': clean_text
                },
                "client_ppb_state": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[11]/tbody/tr/td[4]/div',
                    'parser': clean_text
                },
                "client_ppb_zip": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[11]/tbody/tr/td[6]/div',
                    'parser': clean_text
                },
                "client_ppb_country": {
                    "type": "string",
                    "blank": True,
                    'path': '/html/body/table[11]/tbody/tr/td[8]/div',
                    'parser': clean_text
                }
            }
        },
        "lobbying_issues_detail": {
            "type": "string",
            "blank": True,
            'path': '/html/body/p[10]',
            'parser': clean_text
        },
        "lobbying_issues": {
            "type": "array",
            'even_odd': False,
            'path': '/html/body/table[13]/tbody',
            "items": {
                "type": "object",
                "path": "tr//td/div",
                "properties": {
                    "general_issue_area": {
                        "type": ["string"],
                        "enum": sopr_general_issue_codes,
                        'path': '.',
                        'parser': clean_text,
                        'blank': True
                    }
                }
            }
        },
        "affiliated_organizations": {
            "type": "array",
            'even_odd': True,
            'path': '/html/body/table[16]/tbody',
            "items": {
                "type": "object",
                'path': 'tr[position() > 3]',
                'missing': True,
                "properties": {
                    "affiliated_organization_name": {
                        "type": "string",
                        "even_odd": "even",
                        'path': 'td[1]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_address": {
                        "type": "string",
                        "even_odd": "even",
                        'path': 'td[2]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_city": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[1]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_state": {
                        "type": "string",
                        "blank": True,
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_zip": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[3]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_country": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[4]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_ppb_state": {
                        "type": "string",
                        "blank": True,
                        "even_odd": "odd",
                        'path': 'td[3]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_ppb_city": {
                        "type": "string",
                        "blank": True,
                        "even_odd": "even",
                        'path': 'td[3]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "affiliated_organization_ppb_country": {
                        "type": "string",
                        "blank": True,
                        "even_odd": "odd",
                        'path': 'td[3]/table/tbody/tr/td[4]/div',
                        'parser': clean_text
                    }
                }
            }
        },
        'foreign_entities_no': {
            'type': 'boolean',
            'path': '/html/body/table[17]/tbody/tr/td[1]/input',
            'parser': checkbox_boolean
        },
        'foreign_entities_yes': {
            'type': 'boolean',
            'path': '/html/body/table[17]/tbody/tr/td[3]/input',
            'parser': checkbox_boolean
        },
        "foreign_entities": {
            "type": "array",
            'even_odd': True,
            'path': '/html/body/table[19]/tbody',
            'missing': True,
            "items": {
                "type": "object",
                "path": "tr",
                'missing': True,
                "properties": {
                    "foreign_entity_name": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[1]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_address": {
                        "type": "string",
                        "even_odd": "even",
                        'path': 'td[2]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_city": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[1]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_state": {
                        "type": "string",
                        "even_odd": "odd",
                        "blank": True,
                        'path': 'td[2]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_country": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[2]/table/tbody/tr/td[3]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_ppb_city": {
                        "type": "string",
                        "even_odd": "even",
                        "blank": True,
                        'path': 'td[3]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_ppb_state": {
                        "type": "string",
                        "even_odd": "odd",
                        "blank": True,
                        'path': 'td[3]/table/tbody/tr/td[2]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_ppb_country": {
                        "type": "string",
                        "even_odd": "odd",
                        "blank": True,
                        'path': 'td[3]/table/tbody/tr/td[4]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_amount": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[4]/div',
                        'parser': clean_text
                    },
                    "foreign_entity_ownership_percentage": {
                        "type": "string",
                        "even_odd": "odd",
                        'path': 'td[5]/div',
                        'parser': clean_text
                    }
                }
            }
        },
        "lobbyists": {
            "type": "array",
            'path': '/html/body/table[12]/tbody',
            "items": {
                "type": "object",
                "path": "tr[position() > 2]",
                "properties": {
                    "lobbyist_suffix": {
                        "type": "string",
                        "blank": True,
                        'path': 'td[3]',
                        'parser': clean_text
                    },
                    "lobbyist_first_name": {
                        "type": "string",
                        'path': 'td[1]',
                        'parser': clean_text
                    },
                    "lobbyist_last_name": {
                        "type": "string",
                        'path': 'td[2]',
                        'parser': clean_text
                    },
                    "lobbyist_covered_official_position": {
                        "type": "string",
                        "blank": True,
                        'path': 'td[4]',
                        'parser': clean_text
                    }
                }
            }
        },
    }
}

transformed_ld2_schema = {
    "type": "object",
    "properties": {
        "document_id": {
            "type": "string",
            "format": "uuid_hex",
        },
        "client_registrant_senate_id": {
            "type": "string",
            "pattern": "[0-9]+-[0-9]"
        },
        "client_registrant_house_id": {
            "type": "string",
            "pattern": "[0-9]+"
        },
        "report_type": {
            "type": "object",
            "properties": {
                "year": {
                    "type": "string",
                    "pattern": "\d{4}"
                },
                "quarter": {
                    "type": "string",
                    "pattern": "Q[1-4]"
                },
                "is_amendment": {
                    "type": "boolean"
                },
                "is_termination": {
                    "type": "boolean"
                },
                "no_activity": {
                    "type": "boolean"
                }
            }
        },
        "signature": {
            "type": "string"
        },
        "income_less_than_five_thousand": {
            "type": ["null", "boolean"]
        },
        "income_amount": {
            "type": ["null", "number"],
            "exclusiveMinimum": 5000
        },
        "expense_less_than_five_thousand": {
            "type": ["null", "boolean"]
        },
        "expense_reporting_method": {
            "type": ["string", "null"],
            "enum": ["a", "b", "c"]
        },
        "expense_amount": {
            "type": ["null", "number"],
            "exclusiveMinimum": 5000
        },
        "datetimes": {
            "type": "object",
            "properties": {
                "signature_date": {
                    "type": "string",
                    "format": "date-time"
                },
                "termination_date": {
                    "type": ["null", "string"],
                    "format": "date-time"
                }
            }
        },
        "registrant": {
            "type": "object",
            "properties": {
                "organization_or_lobbying_firm": {
                    "type": "boolean"
                },
                "self_employed_individual": {
                    "type": "boolean"
                },
                "registrant_name": {
                    "type": "string"
                },
                "registrant_address_one": {
                    "type": "string",
                },
                "registrant_address_two": {
                    "type": "string",
                    "blank": True
                },
                "registrant_city": {
                    "type": "string"
                },
                "registrant_state": {
                    "type": "string",
                    "pattern": "[A-Z]{2}"
                },
                "registrant_zip": {
                    "type": "string",
                    "pattern": "^\d{5}(?:[-\s]\d{4})?$"
                },
                "registrant_country": {
                    "type": "string"
                },
                "registrant_ppb_city": {
                    "type": "string",
                    "blank": True
                },
                "registrant_ppb_state": {
                    "type": "string",
                    "blank": True
                },
                "registrant_ppb_zip": {
                    "type": "string",
                    "blank": True
                },
                "registrant_ppb_country": {
                    "type": "string",
                    "blank": True
                },
                "registrant_contact_name": {
                    "type": "string"
                },
                "registrant_contact_name_prefix": {
                    "type": "string"
                },
                "registrant_contact_phone": {
                    "type": "string"
                },
                "registrant_contact_email": {
                    "type": "string",
                    "format": "email"
                }
            }
        },
        "client": {
            "client_name": {
                "type": "string"
            },
            "client_self": {
                "type": "boolean"
            },
            "client_state_or_local_government": {
                "type": "boolean"
            }
        },
        "lobbying_activities": {
            "items": {
                "type": "object",
                "properties": {
                    "general_issue_area": {
                        "type": "string",
                        "enum": sopr_general_issue_codes
                    },
                    "houses_and_agencies_none": {
                        "type": "boolean"
                    },
                    "specific_issues": {
                        "type": "string",
                        "blank": True
                    },
                    "houses_and_agencies": {
                        "type": "string",
                        "blank": True
                    },
                    "foreign_entity_interest_none": {
                        "type": "boolean"
                    },
                    "foreign_entity_interest": {
                        "type": "string",
                        "blank": True
                    },
                    "lobbyists": {
                        "items": {
                            "type": "object",
                            "properties": {
                                "lobbyist_covered_official_position": {
                                    "type": "string",
                                    "blank": True
                                },
                                "lobbyist_is_new": {
                                    "type": "boolean"
                                },
                                "lobbyist_first_name": {
                                    "type": "string",
                                },
                                "lobbyist_last_name": {
                                    "type": "string",
                                },
                                "lobbyist_suffix": {
                                    "type": "string",
                                    "blank": True
                                }
                            }
                        }
                    }
                }
            }
        },
        "registration_update": {
            "type": "object",
            "properties": {
                "client_address": {
                    "type": "string",
                    "blank": True
                },
                "client_city": {
                    "type": "string",
                    "blank": True
                },
                "client_state": {
                    "type": "string",
                    "blank": True
                },
                "client_zip": {
                    "type": "string",
                    "blank": True
                },
                "client_country": {
                    "type": "string",
                    "blank": True
                },
                "client_ppb_city": {
                    "type": "string",
                    "blank": True
                },
                "client_ppb_state": {
                    "type": "string",
                    "blank": True
                },
                "client_ppb_zip": {
                    "type": "string",
                    "blank": True
                },
                "client_ppb_country": {
                    "type": "string",
                    "blank": True
                },
                "client_general_description": {
                    "type": "string",
                    "blank": True
                },
                "removed_lobbyists": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "lobbyist_first_name": {
                                "type": "string"
                            },
                            "lobbyist_last_name": {
                                "type": "string"
                            }
                        }
                    }
                },
                "removed_lobbying_issues": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "general_issue_area": {
                                "type": "string",
                                "enum": sopr_general_issue_codes
                            }
                        }
                    }
                },
                "removed_foreign_entities": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "foreign_entity_name": {
                                "type": "string"
                            }
                        }
                    }
                },
                "removed_affiliated_organizations": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "affiliated_organization_name": {
                                "type": "string"
                            }
                        }
                    }
                },
                "added_affiliated_organizations": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "affiliated_organization_name": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_address": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_city": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_state": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_zip": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_country": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_ppb_state": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_ppb_city": {
                                "type": "string",
                                "blank": True
                            },
                            "affiliated_organization_ppb_country": {
                                "type": "string",
                                "blank": True
                            }
                        }
                    }
                },
                "added_foreign_entities": {
                    "items": {
                        "type": "object",
                        "properties": {
                            "foreign_entity_name": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_address": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_city": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_state": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_country": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_ppb_state": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_ppb_country": {
                                "type": "string",
                                "blank": True
                            },
                            "foreign_entity_amount": {
                                "type": "number",
                                "blank": True
                            },
                            "foreign_entity_ownership_percentage": {
                                "type": "number",
                                "blank": True
                            }
                        }
                    }
                }
            }
        }
    }
}
