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
from urllib.parse import urlsplit

from airflow.configuration import conf

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

if REMOTE_LOGGING:
    OPENSEARCH_HOST: str | None = conf.get("opensearch", "HOST")
    remote_base_log_folder: str = conf.get("logging", "remote_base_log_folder", fallback="")
    remote_task_handler_kwargs = conf.getjson("logging", "remote_task_handler_kwargs", fallback={})
    if not isinstance(remote_task_handler_kwargs, dict):
        raise ValueError(
            "logging/remote_task_handler_kwargs must be a JSON object (a python dict), we got "
            f"{type(remote_task_handler_kwargs)}"
        )
    if remote_base_log_folder.startswith("stackdriver://"):
        key_path = conf.get_mandatory_value("logging", "GOOGLE_KEY_PATH", fallback=None)
        # stackdriver:///airflow-tasks => airflow-tasks
        log_name = urlsplit(remote_base_log_folder).path[1:]
        STACKDRIVER_REMOTE_HANDLERS = {
            "task": {
                "class": "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler",
                "formatter": "airflow",
                "gcp_log_name": log_name,
                "gcp_key_path": key_path,
            }
        }

        DEFAULT_LOGGING_CONFIG["handlers"].update(STACKDRIVER_REMOTE_HANDLERS)
        DEFAULT_LOGGING_CONFIG["handlers"]["task"].update(remote_task_handler_kwargs)
    elif OPENSEARCH_HOST:
        OPENSEARCH_END_OF_LOG_MARK: str = conf.get_mandatory_value("opensearch", "END_OF_LOG_MARK")
        OPENSEARCH_PORT: str = conf.get_mandatory_value("opensearch", "PORT")
        OPENSEARCH_USERNAME: str = conf.get_mandatory_value("opensearch", "USERNAME")
        OPENSEARCH_PASSWORD: str = conf.get_mandatory_value("opensearch", "PASSWORD")
        OPENSEARCH_WRITE_STDOUT: bool = conf.getboolean("opensearch", "WRITE_STDOUT")
        OPENSEARCH_JSON_FORMAT: bool = conf.getboolean("opensearch", "JSON_FORMAT")
        OPENSEARCH_JSON_FIELDS: str = conf.get_mandatory_value("opensearch", "JSON_FIELDS")
        OPENSEARCH_HOST_FIELD: str = conf.get_mandatory_value("opensearch", "HOST_FIELD")
        OPENSEARCH_OFFSET_FIELD: str = conf.get_mandatory_value("opensearch", "OFFSET_FIELD")

        OPENSEARCH_REMOTE_HANDLERS: dict[str, dict[str, str | bool | None]] = {
            "task": {
                "class": "airflow.providers.opensearch.log.os_task_handler.OpensearchTaskHandler",
                "formatter": "airflow",
                "base_log_folder": BASE_LOG_FOLDER,
                "end_of_log_mark": OPENSEARCH_END_OF_LOG_MARK,
                "host": OPENSEARCH_HOST,
                "port": OPENSEARCH_PORT,
                "username": OPENSEARCH_USERNAME,
                "password": OPENSEARCH_PASSWORD,
                "write_stdout": OPENSEARCH_WRITE_STDOUT,
                "json_format": OPENSEARCH_JSON_FORMAT,
                "json_fields": OPENSEARCH_JSON_FIELDS,
                "host_field": OPENSEARCH_HOST_FIELD,
                "offset_field": OPENSEARCH_OFFSET_FIELD,
            },
        }
        DEFAULT_LOGGING_CONFIG["handlers"].update(OPENSEARCH_REMOTE_HANDLERS)
        DEFAULT_LOGGING_CONFIG["handlers"]["task"].update(remote_task_handler_kwargs)
