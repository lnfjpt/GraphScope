# coding: utf-8

"""
    GraphScope Interactive API v0.3

    This is the definition of GraphScope Interactive API, including   - AdminService API   - Vertex/Edge API   - QueryService   AdminService API (with tag AdminService) defines the API for GraphManagement, ProcedureManagement and Service Management.  Vertex/Edge API (with tag GraphService) defines the API for Vertex/Edge management, including creation/updating/delete/retrive.  QueryService API (with tag QueryService) defines the API for procedure_call, Ahodc query. 

    The version of the OpenAPI document: 1.0.0
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations

import json
import pprint
import re  # noqa: F401
from typing import Any
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import StrictStr
from typing_extensions import Self


class LongText(BaseModel):
    """
    LongText
    """ # noqa: E501
    long_text: Optional[StrictStr]
    __properties: ClassVar[List[str]] = ["long_text"]

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
        extra= "forbid",
    )


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        return self.model_dump_json(by_alias=True, exclude_unset=True)

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of LongText from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # set to None if long_text (nullable) is None
        # and model_fields_set contains the field
        if self.long_text is None and "long_text" in self.model_fields_set:
            _dict['long_text'] = None

        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of LongText from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)
        
        # Forbid extra fields, in order to avoid matching var_char
        for key in obj:
            if key not in cls.__properties:
                raise ValueError(f"Unexpected field {key} for LongText")

        _obj = cls.model_validate({
            "long_text": obj.get("long_text")
        })
        return _obj

