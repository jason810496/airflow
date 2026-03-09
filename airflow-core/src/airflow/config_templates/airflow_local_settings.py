#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Airflow logging settings."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO, RemoteLogStreamIO

LOG_LEVEL: str = conf.get_mandatory_value("logging", "LOGGING_LEVEL").upper()


# Flask appbuilder's info level log is very verbose,
# so it's set to 'WARN' by default.
FAB_LOG_LEVEL: str = conf.get_mandatory_value("logging", "FAB_LOGGING_LEVEL").upper()

LOG_FORMAT: str = conf.get_mandatory_value("logging", "LOG_FORMAT")
DAG_PROCESSOR_LOG_FORMAT: str = conf.get_mandatory_value("logging", "DAG_PROCESSOR_LOG_FORMAT")

LOG_FORMATTER_CLASS: str = conf.get_mandatory_value(
    "logging", "LOG_FORMATTER_CLASS", fallback="airflow.utils.log.timezone_aware.TimezoneAware"
)

DAG_PROCESSOR_LOG_TARGET: str = conf.get_mandatory_value("logging", "DAG_PROCESSOR_LOG_TARGET")

BASE_LOG_FOLDER: str = os.path.expanduser(conf.get_mandatory_value("logging", "BASE_LOG_FOLDER"))

# This isn't used anymore, but kept for compat of people who might have imported it
DEFAULT_LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "airflow": {
            "format": LOG_FORMAT,
            "class": LOG_FORMATTER_CLASS,
        },
        "source_processor": {
            "format": DAG_PROCESSOR_LOG_FORMAT,
            "class": LOG_FORMATTER_CLASS,
        },
    },
    "filters": {
        "mask_secrets_core": {
            "()": "airflow._shared.secrets_masker._secrets_masker",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            # "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "airflow",
            "stream": "sys.stdout",
            "filters": ["mask_secrets_core"],
        },
        "task": {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "formatter": "airflow",
            "base_log_folder": BASE_LOG_FOLDER,
            "filters": ["mask_secrets_core"],
        },
    },
    "loggers": {
        "airflow.task": {
            "handlers": ["task"],
            "level": LOG_LEVEL,
            # Set to true here (and reset via set_context) so that if no file is configured we still get logs!
            "propagate": True,
            "filters": ["mask_secrets_core"],
        },
        "flask_appbuilder": {
            "handlers": ["console"],
            "level": FAB_LOG_LEVEL,
            "propagate": True,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
        "filters": ["mask_secrets_core"],
    },
}

EXTRA_LOGGER_NAMES: str | None = conf.get("logging", "EXTRA_LOGGER_NAMES", fallback=None)
if EXTRA_LOGGER_NAMES:
    new_loggers = {
        logger_name.strip(): {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": True,
        }
        for logger_name in EXTRA_LOGGER_NAMES.split(",")
    }
    DEFAULT_LOGGING_CONFIG["loggers"].update(new_loggers)

##################
# Remote logging #
##################

REMOTE_LOGGING: bool = conf.getboolean("logging", "remote_logging")
REMOTE_TASK_LOG: RemoteLogIO | RemoteLogStreamIO | None = None
DEFAULT_REMOTE_CONN_ID: str | None = None


def _default_conn_name_from(mod_path, hook_name):
    # Try to set the default conn name from a hook, but don't error if something goes wrong at runtime
    from importlib import import_module

    global DEFAULT_REMOTE_CONN_ID

    try:
        mod = import_module(mod_path)

        hook = getattr(mod, hook_name)

        DEFAULT_REMOTE_CONN_ID = getattr(hook, "default_conn_name")
    except Exception:
        # Lets error in tests though!
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise
        return None


def _default_conn_name_from_class_name(hook_class_name: str | None):
    from airflow._shared.module_loading import import_string

    global DEFAULT_REMOTE_CONN_ID

    if not hook_class_name:
        return None

    try:
        hook = import_string(hook_class_name)
        DEFAULT_REMOTE_CONN_ID = getattr(hook, "default_conn_name")
    except Exception:
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise
        return None


def _remote_logging_info_for(
    remote_base_log_folder: str, elasticsearch_host: str | None, opensearch_host: str | None
):
    from airflow.providers_manager import ProvidersManager

    providers_manager = ProvidersManager()
    if remote_logging_info := providers_manager.get_remote_logging_info_for_uri(remote_base_log_folder):
        return remote_logging_info
    if elasticsearch_host:
        return providers_manager.get_remote_logging_info_for_backend("elasticsearch")
    if opensearch_host:
        return providers_manager.get_remote_logging_info_for_backend("opensearch")
    return None


def _create_remote_task_log(
    remote_log_io_class_name: str,
    *,
    base_log_folder: str,
    remote_base_log_folder: str,
    delete_local_logs: bool,
    remote_task_handler_kwargs: dict[str, Any],
):
    from airflow._shared.module_loading import import_string

    remote_log_io_class = import_string(remote_log_io_class_name)
    from_airflow_config = getattr(remote_log_io_class, "from_airflow_config", None)
    if from_airflow_config is None:
        raise AirflowException(
            f"Remote logging backend {remote_log_io_class_name} must implement from_airflow_config()."
        )
    return from_airflow_config(
        base_log_folder=base_log_folder,
        remote_base_log_folder=remote_base_log_folder,
        delete_local_logs=delete_local_logs,
        remote_task_handler_kwargs=remote_task_handler_kwargs,
    )


if REMOTE_LOGGING:
    ELASTICSEARCH_HOST: str | None = conf.get("elasticsearch", "HOST")
    OPENSEARCH_HOST: str | None = conf.get("opensearch", "HOST")
    # Storage bucket URL for remote logging
    # S3 buckets should start with "s3://"
    # Cloudwatch log groups should start with "cloudwatch://"
    # GCS buckets should start with "gs://"
    # WASB buckets should start with "wasb"
    # HDFS path should start with "hdfs://"
    # just to help Airflow select correct handler
    remote_base_log_folder: str = conf.get_mandatory_value("logging", "remote_base_log_folder")
    remote_task_handler_kwargs = conf.getjson("logging", "remote_task_handler_kwargs", fallback={})
    if not isinstance(remote_task_handler_kwargs, dict):
        raise ValueError(
            "logging/remote_task_handler_kwargs must be a JSON object (a python dict), we got "
            f"{type(remote_task_handler_kwargs)}"
        )
    delete_local_copy = conf.getboolean("logging", "delete_local_logs")
    if remote_logging_info := _remote_logging_info_for(
        remote_base_log_folder, ELASTICSEARCH_HOST, OPENSEARCH_HOST
    ):
        _default_conn_name_from_class_name(remote_logging_info.hook_class_name)
        REMOTE_TASK_LOG = _create_remote_task_log(
            remote_logging_info.remote_log_io_class_name,
            base_log_folder=BASE_LOG_FOLDER,
            remote_base_log_folder=remote_base_log_folder,
            delete_local_logs=delete_local_copy,
            remote_task_handler_kwargs=remote_task_handler_kwargs,
        )
        remote_task_handler_kwargs = {}
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' or 'opensearch' if you are using those backends. Otherwise, "
            "make sure 'remote_base_log_folder' uses a scheme exported by a provider RemoteIO backend."
        )
    DEFAULT_LOGGING_CONFIG["handlers"]["task"].update(remote_task_handler_kwargs)
