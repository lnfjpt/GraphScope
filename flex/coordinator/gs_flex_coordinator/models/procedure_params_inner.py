from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator import util


class ProcedureParamsInner(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, name=None, type=None):  # noqa: E501
        """ProcedureParamsInner - a model defined in OpenAPI

        :param name: The name of this ProcedureParamsInner.  # noqa: E501
        :type name: str
        :param type: The type of this ProcedureParamsInner.  # noqa: E501
        :type type: str
        """
        self.openapi_types = {
            'name': str,
            'type': str
        }

        self.attribute_map = {
            'name': 'name',
            'type': 'type'
        }

        self._name = name
        self._type = type

    @classmethod
    def from_dict(cls, dikt) -> 'ProcedureParamsInner':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The Procedure_params_inner of this ProcedureParamsInner.  # noqa: E501
        :rtype: ProcedureParamsInner
        """
        return util.deserialize_model(dikt, cls)

    @property
    def name(self) -> str:
        """Gets the name of this ProcedureParamsInner.


        :return: The name of this ProcedureParamsInner.
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Sets the name of this ProcedureParamsInner.


        :param name: The name of this ProcedureParamsInner.
        :type name: str
        """

        self._name = name

    @property
    def type(self) -> str:
        """Gets the type of this ProcedureParamsInner.


        :return: The type of this ProcedureParamsInner.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this ProcedureParamsInner.


        :param type: The type of this ProcedureParamsInner.
        :type type: str
        """

        self._type = type