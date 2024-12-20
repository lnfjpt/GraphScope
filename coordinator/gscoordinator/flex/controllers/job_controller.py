import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.create_dataloading_job_response import CreateDataloadingJobResponse  # noqa: E501
from gscoordinator.flex.models.dataloading_job_config import DataloadingJobConfig  # noqa: E501
from gscoordinator.flex.models.dataloading_mr_job_config import DataloadingMRJobConfig  # noqa: E501
from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.job_status import JobStatus  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import get_client_wrapper
from gscoordinator.flex.core import handle_api_exception


@handle_api_exception()
def delete_job_by_id(job_id, delete_scheduler=None):  # noqa: E501
    """delete_job_by_id

    Delete job by ID # noqa: E501

    :param job_id:
    :type job_id: str
    :param delete_scheduler:
    :type delete_scheduler: bool

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return get_client_wrapper().delete_job_by_id(job_id, delete_scheduler)


@handle_api_exception()
def get_dataloading_job_config(graph_id, dataloading_job_config):  # noqa: E501
    """get_dataloading_job_config

    Post to get the data loading configuration for MapReduce Task # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param dataloading_job_config:
    :type dataloading_job_config: dict | bytes

    :rtype: Union[DataloadingMRJobConfig, Tuple[DataloadingMRJobConfig, int], Tuple[DataloadingMRJobConfig, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        dataloading_job_config = DataloadingJobConfig.from_dict(connexion.request.get_json())  # noqa: E501
    return get_client_wrapper().get_dataloading_job_config(graph_id, dataloading_job_config)


@handle_api_exception()
def get_job_by_id(job_id):  # noqa: E501
    """get_job_by_id

    Get job status by ID # noqa: E501

    :param job_id: 
    :type job_id: str

    :rtype: Union[JobStatus, Tuple[JobStatus, int], Tuple[JobStatus, int, Dict[str, str]]
    """
    return get_client_wrapper().get_job_by_id(job_id)


@handle_api_exception()
def list_jobs():  # noqa: E501
    """list_jobs

    List all jobs # noqa: E501


    :rtype: Union[List[JobStatus], Tuple[List[JobStatus], int], Tuple[List[JobStatus], int, Dict[str, str]]
    """
    return get_client_wrapper().list_jobs()


@handle_api_exception()
def submit_dataloading_job(graph_id, dataloading_job_config):  # noqa: E501
    """submit_dataloading_job

    Submit a dataloading job # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param dataloading_job_config:
    :type dataloading_job_config: dict | bytes

    :rtype: Union[CreateDataloadingJobResponse, Tuple[CreateDataloadingJobResponse, int], Tuple[CreateDataloadingJobResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        dataloading_job_config = DataloadingJobConfig.from_dict(connexion.request.get_json())  # noqa: E501
    return get_client_wrapper().submit_dataloading_job(graph_id, dataloading_job_config)
