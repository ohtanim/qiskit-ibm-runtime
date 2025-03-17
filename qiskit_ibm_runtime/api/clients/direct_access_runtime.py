# This code is part of Qiskit.
#
# (C) Copyright IBM 2025.
#
# This code is licensed under the Apache License, Version 2.0. You may
# obtain a copy of this license in the LICENSE.txt file in the root directory
# of this source tree or at http://www.apache.org/licenses/LICENSE-2.0.
#
# Any modifications or derivative works of this code must retain this
# copyright notice, and modified files need to carry a notice indicating
# that they have been altered from the originals.

"""Client for accessing IBM Quantum Qiskit Runtime Direct Access service."""

import os
import logging
from typing import Any, Dict, List, Optional
import uuid
import json
from datetime import datetime as python_datetime
import urllib3
from urllib3.util.retry import Retry
from requests import Response
import boto3
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from .backend import BaseBackendClient
from ...utils import RuntimeEncoder
from ..exceptions import RequestsApiError
from ..client_parameters import ClientParameters
from ..session import RetrySession
from ...exceptions import IBMInputValueError, RuntimeJobNotFound

logger = logging.getLogger(__name__)

STATUS_FORCELIST = (
    500,  # General server error
    502,  # Bad Gateway
    504,  # Gateway Timeout
)

class S3Client:
    """A client to access S3"""

    def __init__( # pylint: disable=too-many-positional-arguments
        self,
        endpoint: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str,
        region: str,
    ) -> None:
        self._endpoint = endpoint
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._bucket = bucket_name
        self._region = region
        self._s3api = boto3.client(
            "s3",
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            endpoint_url=self._endpoint,
        )
        self._http = urllib3.PoolManager()

    def create_presigned_url(self, method: str, key_name: str, expires_in: int) -> str:
        """Creates S3 presigined URL for the specified object"""
        method_map = {"GET": "get_object", "PUT": "put_object"}
        return self._s3api.generate_presigned_url(
            ClientMethod=method_map[method],
            Params={"Bucket": self._bucket, "Key": key_name},
            ExpiresIn=expires_in,
            HttpMethod=method,
        )

    def get_object(self, key_name: str) -> str:
        """Get S3 Object and returns as string text"""
        signed_url = self.create_presigned_url("GET", key_name, 3600)
        resp = self._http.request(
            "GET",
            signed_url,
        )
        if resp.status != 200:
            raise RequestsApiError(
                resp.data.decode(encoding="utf-8"),
                resp.status
            )

        return resp.data.decode(encoding="utf-8")

    def put_object(self, key_name: str, data: str) -> None:
        """Put Object"""
        signed_url = self.create_presigned_url("PUT", key_name, 3600)
        resp = self._http.request(
            "PUT",
            signed_url,
            body=data,
        )
        if resp.status != 200:
            raise RequestsApiError(
                resp.data.decode(encoding="utf-8"),
                resp.status
            )


class DirectAccessRuntimeClient(BaseBackendClient):
    """A Direct Access Client"""

    def __init__(
        self,
        params: ClientParameters,
    ) -> None:

        iam_api_url = os.environ.get("IBMQRUN_IAM_ENDPOINT")
        if iam_api_url is None:
            iam_api_url = params.iam_api_url
        if iam_api_url is None:
            raise IBMInputValueError(
                "'iam_api_url' is required for 'ibm_direct_access' channel."
            )

        self._auth = IAMAuthenticator(
            apikey=params.token,
            url=iam_api_url,
        )

        self._instance = os.environ.get("IBMQRUN_SERVICE_CRN")
        if self._instance is None:
            self._instance = params.instance
        if self._instance is None:
            raise IBMInputValueError(
                "'instance' is required for 'ibm_direct_access' channel."
            )

        self._endpoint = os.environ.get("IBMQRUN_DAAPI_ENDPOINT")
        if self._endpoint is None:
            self._endpoint = params.url
        if self._endpoint is None:
            raise IBMInputValueError(
                "'url' is required for 'ibm_direct_access' channel."
            )

        conn_param = params.connection_parameters()
        cert_reqs='CERT_NONE'
        if conn_param.get("verify", False) is True:
            cert_reqs='CERT_REQUIRED'

        self._http = urllib3.PoolManager(cert_reqs=cert_reqs)
        # Use default values in RetrySession
        self._retries = Retry(
            total=5,
            connect=3,
            backoff_factor=0.5,
            status_forcelist=STATUS_FORCELIST)

        self._session = RetrySession(
            base_url=params.url,
            auth=params.get_auth_handler(),
            **params.connection_parameters(),
        )

        self._configuration_registry: Dict[str, Dict[str, Any]] = {}
        self._kwargs = params.kwargs


    def _create_s3client(self) -> S3Client:

        s3_endpoint = os.environ.get("IBMQRUN_S3_ENDPOINT")
        if s3_endpoint is None:
            s3_endpoint = self._kwargs.get("s3_endpoint")
        if s3_endpoint is None:
            raise IBMInputValueError(
                "'s3_endpoint' is required for 'ibm_direct_access' channel."
            )

        aws_access_key_id = os.environ.get("IBMQRUN_AWS_ACCESS_KEY_ID")
        if aws_access_key_id is None:
            aws_access_key_id = self._kwargs.get("aws_access_key_id")
        if aws_access_key_id is None:
            raise IBMInputValueError(
                "'aws_access_key_id' is required for 'ibm_direct_access' channel."
            )

        aws_secret_access_key = os.environ.get("IBMQRUN_AWS_SECRET_ACCESS_KEY")
        if aws_secret_access_key is None:
            aws_secret_access_key = self._kwargs.get("aws_secret_access_key")
        if aws_secret_access_key is None:
            raise IBMInputValueError(
                "'aws_secret_access_key' is required for 'ibm_direct_access' channel."
            )

        s3_bucket = os.environ.get("IBMQRUN_S3_BUCKET")
        if s3_bucket is None:
            s3_bucket = self._kwargs.get("s3_bucket")
        if s3_bucket is None:
            raise IBMInputValueError(
                "'s3_bucket' is required for 'ibm_direct_access' channel."
            )

        s3_region = os.environ.get("IBMQRUN_S3_REGION")
        if s3_region is None:
            s3_region = self._kwargs.get("s3_region")
        if s3_region is None:
            raise IBMInputValueError(
                "'s3_region' is required for 'ibm_direct_access' channel."
            )

        return S3Client(
            s3_endpoint,
            aws_access_key_id,
            aws_secret_access_key,
            s3_bucket,
            s3_region,
        )


    def _get_headers(self) -> dict[str, Any]:
        token = self._auth.token_manager.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Service-CRN": self._instance,
        }


    def list_backends(self) -> List[str]:
        """Returns a list of available backend"""
        resp = self._http.request(
            "GET",
            f"{self._endpoint}/v1/backends",
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 200:
            raise RequestsApiError(resp.data.decode(encoding="utf-8"), resp.status)

        if (backend := os.environ.get("IBMQRUN_BACKEND")) is not None:
            return [backend]

        backends = resp.json()["backends"]
        backend_names = []
        for backend in backends:
            backend_names.append(backend["name"])

        return backend_names


    def _get_backend(self, backend_name: str) -> Dict[str, str]:
        """Returns a backend details"""
        resp = self._http.request(
            "GET",
            f"{self._endpoint}/v1/backends/{backend_name}",
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 200:
            raise RequestsApiError(resp.data.decode(encoding="utf-8"), resp.status)

        return resp.json()


    def backend_status(self, backend_name: str) -> dict[str, Any]:
        """Returns BackendStatus for the specified backend

        Args:
            backend_name(str): backend name

        Returns:
            BackendStatus: backend status
        """
        backend = self._get_backend(backend_name)
        return {
            "backend_name": backend["name"],
            "backend_version": backend.get("version", ""),
            "operational": backend["status"] == "online",
            "pending_jobs": 0,
            "status_msg": "active" if backend["status"] == "online" else backend.get("message", ""),
        }


    def create_session( # pylint: disable=too-many-positional-arguments
        self,
        backend: Optional[str] = None,
        instance: Optional[str] = None,
        max_time: Optional[int] = None,
        channel: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a session.

        Args:
            mode: Execution mode.
        """
        raise NotImplementedError()


    def cancel_session(self, session_id: str) -> None:
        """Close all jobs in the runtime session.

        Args:
            session_id: Session ID.
        """
        raise NotImplementedError()


    def session_details(self, session_id: str) -> Dict[str, Any]:
        """Get session details.

        Args:
            session_id: Session ID.

        Returns:
            Session details.
        """
        raise NotImplementedError()


    def backend_configuration(
        self, backend_name: str, refresh: bool = False
    ) -> Dict[str, Any]:
        """Return the configuration of the IBM backend.

        Args:
            backend_name: The name of the IBM backend.

        Returns:
            Backend configuration.
        """
        if backend_name not in self._configuration_registry or refresh:
            resp = self._http.request(
                "GET",
                f"{self._endpoint}/v1/backends/{backend_name}/configuration",
                headers=self._get_headers(),
                retries=self._retries,
            )
            if resp.status != 200:
                raise RequestsApiError(resp.data.decode(encoding="utf-8"), resp.status)
            self._configuration_registry[backend_name] = resp.json()

        return self._configuration_registry[backend_name].copy()


    def backend_properties(
        self, backend_name: str, datetime: Optional[python_datetime] = None
    ) -> Dict[str, Any]:
        """Return the properties of the IBM backend.

        Args:
            backend_name: The name of the IBM backend.
            datetime: Date and time for additional filtering of backend properties.

        Returns:
            Backend properties.
        """
        resp = self._http.request(
            "GET",
            f"{self._endpoint}/v1/backends/{backend_name}/properties",
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 200:
            raise RequestsApiError(resp.data.decode(encoding="utf-8"), resp.status)

        return resp.json()


    def backend_pulse_defaults(self, _backend_name: str) -> Dict:
        """Return the pulse defaults of the IBM backend."""
        return None


    def update_tags(self, job_id: str, tags: list) -> Response:
        """Update the tags of the job.

        Args:
            job_id: The ID of the job.
            tags: The new tags to be assigned to the job.

        Returns:
            API Response.
        """
        raise NotImplementedError()


    def usage(self) -> Dict[str, Any]:
        """Return monthly open plan usage information.

        Returns:
            API Response.
        """
        raise NotImplementedError()


    def program_run( # pylint: disable=too-many-positional-arguments
        self,
        program_id: str,
        backend_name: Optional[str],
        params: Dict,
        image: Optional[str], # pylint: disable=unused-argument
        hgp: Optional[str], # pylint: disable=unused-argument
        log_level: Optional[str],
        session_id: Optional[str], # pylint: disable=unused-argument
        job_tags: Optional[List[str]] = None, # pylint: disable=unused-argument
        max_execution_time: Optional[int] = None, # pylint: disable=unused-argument
        start_session: Optional[bool] = False, # pylint: disable=unused-argument
        session_time: Optional[int] = None, # pylint: disable=unused-argument
        private: Optional[bool] = False, # pylint: disable=unused-argument
    ) -> Dict:
        """Run the specified program.

        Args:
            program_id: Program ID.
            backend_name: Name of the backend to run the program.
            params: Parameters to use.
            image: The runtime image to use.
            hgp: Hub/group/project to use.
            log_level: Log level to use.
            session_id: Job ID of the first job in a runtime session.
            job_tags: Tags to be assigned to the job.
            max_execution_time: Maximum execution time in seconds.
            start_session: Set to True to explicitly start a runtime session. Defaults to False.
            session_time: Length of session in seconds.
            private: Marks job as private.

        Returns:
            JSON response.
        """
        s3 = self._create_s3client()

        job_id = os.environ.get("IBMQRUN_JOB_ID", str(uuid.uuid4()))

        s3.put_object(
            f"params_{job_id}", json.dumps(params, cls=RuntimeEncoder)
        )

        s3_presigned_url_expires_in = 604800 # 1 weeks as max life
        input_get_signed_url = s3.create_presigned_url(
            "GET", f"params_{job_id}", s3_presigned_url_expires_in
        )
        results_put_signed_url = s3.create_presigned_url(
            "PUT", f"results_{job_id}", s3_presigned_url_expires_in
        )
        logs_put_signed_url = s3.create_presigned_url(
            "PUT", f"logs_{job_id}", s3_presigned_url_expires_in
        )

        job_input = {
            "backend": backend_name,
            "id": job_id,
            "log_level": log_level.lower(),
            "program_id": program_id,
            "timeout_secs": int(os.environ.get("IBMQRUN_TIMEOUT_SECONDS", "86400")),
            "storage": {
                "input": {
                    "type": "s3_compatible",
                    "presigned_url": input_get_signed_url,
                },
                "results": {
                    "type": "s3_compatible",
                    "presigned_url": results_put_signed_url,
                },
                "logs": {
                    "type": "s3_compatible",
                    "presigned_url": logs_put_signed_url,
                },
            },
        }

        resp = self._http.request(
            "POST",
            f"{self._endpoint}/v1/jobs",
            json=job_input,
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 204:
            return {"errors": resp.json()["errors"]}

        return {
            "id": job_id,
            "backend": backend_name,
            "session_id": None, # Direct Access does not have session concept
            "messages": [],
        }


    def job_results(self, job_id: str) -> str:
        """Get the results of a program job.

        Args:
            job_id: Program job ID.

        Returns:
            Job result.
        """
        s3 = self._create_s3client()

        return s3.get_object(f"results_{job_id}")


    def job_interim_results(self, job_id: str) -> str:
        """Get the interim results of a program job.

        Args:
            job_id: Program job ID.

        Returns:
            Job interim results.
        """
        raise NotImplementedError()


    def job_logs(self, job_id: str) -> str:
        """Returns job logs"""
        s3 = self._create_s3client()

        try:
            return s3.get_object(f"logs_{job_id}")
        except RequestsApiError:
            return ""


    def job_cancel(self, job_id: str) -> None:
        """Cancels a job"""
        resp = self._http.request(
            "POST",
            f"{self._endpoint}/v1/jobs/{job_id}/cancel",
            body={},
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 204:
            logger.error(
                "Failed to cancel a job(%s), reason: (%s)",
                job_id, resp.data.decode(encoding="utf-8")
            )


    def job_delete(self, job_id: str) -> None:
        """Delete a job.

        Args:
            job_id: Runtime job ID.
        """
        resp = self._http.request(
            "DELETE",
            f"{self._endpoint}/v1/jobs/{job_id}",
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 204:
            logger.error(
                "Failed to delete a job(%s), reason: (%s)",
                job_id, resp.data.decode(encoding="utf-8")
            )


    def _list_jobs(self) -> List[Dict[str, Any]]:
        resp = self._http.request(
            "GET",
            f"{self._endpoint}/v1/jobs",
            headers=self._get_headers(),
            retries=self._retries,
        )
        if resp.status != 200:
            raise RequestsApiError(resp.data.decode(encoding="utf-8"), resp.status)

        return resp.json()["jobs"]


    def _get_job(self, job_id: str) -> dict[str, Any]:
        for job in self._list_jobs():
            if job["id"] == job_id:
                return job

        raise RuntimeJobNotFound(f"Job not found: {job_id}")


    def job_metadata(self, job_id: str) -> dict[str, Any]:
        """Returns job metadata"""
        job = self._get_job(job_id)

        secs = job["usage"].get("quantum_nanoseconds")
        if secs is not None:
            secs *= 1e-9

        return {
            "timestamps": {
                "created": job["created_time"],
                "finished": job.get("end_time"),
            },
            "usage": {
                "quantum_seconds": secs,
            },
        }


    def job_get(self, job_id: str, _exclude_params: bool = True) -> Dict:
        """Get job data.

        Args:
            job_id: Job ID.

        Returns:
            JSON response.
        """
        job = self._get_job(job_id)
        if job is None:
            return None

        return {
            "id": job_id,
            "state": {
                "status": job["status"],
                "reason": job.get("reason_message", ""),
                "reason_code": job.get("reason_code", ""),
            },
            "created": job["created_time"],
            "ended": job.get("end_time", ""),
            "backend": job["backend"],
            "status": job["status"],
        }


    def jobs_get( # pylint: disable=too-many-positional-arguments
        self,
        limit: int = None,
        skip: int = None,
        backend_name: str = None,
        pending: bool = None,
        program_id: str = None,
        hub: str = None,
        group: str = None,
        project: str = None,
        job_tags: Optional[List[str]] = None,
        session_id: Optional[str] = None,
        created_after: Optional[python_datetime] = None,
        created_before: Optional[python_datetime] = None,
        descending: bool = True,
    ) -> Dict:
        """Get job data for all jobs.

        Args:
            limit: Number of results to return.
            skip: Number of results to skip.
            backend_name: Name of the backend to retrieve jobs from.
            pending: Returns 'QUEUED' and 'RUNNING' jobs if True,
                returns 'DONE', 'CANCELLED' and 'ERROR' jobs if False.
            program_id: Filter by Program ID.
            hub: Filter by hub - hub, group, and project must all be specified.
            group: Filter by group - hub, group, and project must all be specified.
            project: Filter by project - hub, group, and project must all be specified.
            job_tags: Filter by tags assigned to jobs. Matched jobs are associated with all tags.
            session_id: Job ID of the first job in a runtime session.
            created_after: Filter by the given start date, in local time. This is used to
                find jobs whose creation dates are after (greater than or equal to) this
                local date/time.
            created_before: Filter by the given end date, in local time. This is used to
                find jobs whose creation dates are before (less than or equal to) this
                local date/time.
            descending: If ``True``, return the jobs in descending order of the job
                creation date (i.e. newest first) until the limit is reached.

        Returns:
            JSON response.
        """

        if hub is not None or group is not None or project is not None:
            logger.warning(
                "Filtering by Hub/Group/Project value is not supported by 'ibm_direct_access'"
            )

        if session_id is not None:
            logger.warning(
                "Filtering by session ID is not supported by 'ibm_direct_access'"
            )

        if job_tags is not None:
            logger.warning(
                "Filtering by job tags is not supported by 'ibm_direct_access'"
            )

        jobs = []
        for job in self._list_jobs():
            if created_after is not None:
                job_dt = python_datetime.fromisoformat(job["created_time"])
                if job_dt < created_after:
                    continue

            if created_before is not None:
                job_dt = python_datetime.fromisoformat(job["created_time"])
                if job_dt > created_before:
                    continue

            if program_id is not None:
                if job["program_id"] != program_id:
                    continue

            if pending is not None:
                if pending is True and job["status"] != "Running":
                    # Running only
                    continue
                if pending is False and job["status"] == "Running":
                    # Completed, Cancelled and Failed
                    continue

            if backend_name is not None:
                if backend_name != job["backend"]:
                    continue

            jobs.append(
                {
                    "id": job["id"],
                    "state": {
                        "status": job["status"],
                        "reason": job.get("reason_message", ""),
                        "reason_code": job.get("reason_code", ""),
                    },
                    "created": job["created_time"],
                    "ended": job.get("end_time", ""),
                    "backend": job["backend"],
                    "status": job["status"],
                }
            )

        # Direct Access API return a list in ascending order of created time by default.
        if descending is True:
            jobs.reverse()

        return {
            "jobs": jobs,
            "count": len(jobs),
            "limit": limit,
            "offset": skip,
        }
