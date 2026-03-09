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
from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

from airflow._shared.logging.remote import discover_remote_log_handler
from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from airflow.logging.remote import RemoteLogIO

log = logging.getLogger(__name__)


class _ActiveLoggingConfig:
    """Private class to hold active logging config variables."""

    logging_config_loaded: bool = False
    remote_task_log: RemoteLogIO | None
    default_remote_conn_id: str | None = None

    @classmethod
    def set(cls, remote_task_log: RemoteLogIO | None, default_remote_conn_id: str | None) -> None:
        """Set remote logging configuration atomically."""
        cls.remote_task_log = remote_task_log
        cls.default_remote_conn_id = default_remote_conn_id
        cls.logging_config_loaded = True


def get_remote_task_log() -> RemoteLogIO | None:
    if not _ActiveLoggingConfig.logging_config_loaded:
        load_logging_config()
    return _ActiveLoggingConfig.remote_task_log


def get_default_remote_conn_id() -> str | None:
    if not _ActiveLoggingConfig.logging_config_loaded:
        load_logging_config()
    return _ActiveLoggingConfig.default_remote_conn_id


def _discover_remote_log_handler_from_providers() -> tuple[RemoteLogIO | None, str | None]:
    """Discover provider-defined RemoteLogIO implementations."""
    if not conf.getboolean("logging", "remote_logging"):
        return None, None

    remote_task_handler_kwargs = conf.getjson("logging", "remote_task_handler_kwargs", fallback={})
    if not isinstance(remote_task_handler_kwargs, dict):
        raise ValueError(
            "logging/remote_task_handler_kwargs must be a JSON object (a python dict), "
            f"we got {type(remote_task_handler_kwargs)}"
        )

    remote_base_log_folder = conf.get("logging", "remote_base_log_folder", fallback="")
    base_log_folder = conf.get_mandatory_value("logging", "base_log_folder")
    delete_local_logs = conf.getboolean("logging", "delete_local_logs")

    for remote_logging_class_name in ProvidersManager().remote_logging_class_names:
        try:
            remote_logging_class = import_string(remote_logging_class_name)
            factory = getattr(remote_logging_class, "from_config", None)
            if factory is None:
                continue
            remote_task_log, default_remote_conn_id = factory(
                base_log_folder=base_log_folder,
                remote_base_log_folder=remote_base_log_folder,
                delete_local_logs=delete_local_logs,
                remote_task_handler_kwargs=remote_task_handler_kwargs,
            )
            if remote_task_log is not None:
                log.info("Using provider-defined RemoteLogIO from %s", remote_logging_class_name)
                return remote_task_log, default_remote_conn_id
        except Exception:
            log.exception("Failed loading provider-defined RemoteLogIO from %s", remote_logging_class_name)

    return None, None


def load_logging_config() -> tuple[dict[str, Any], str]:
    """Configure & Validate Airflow Logging."""
    fallback = "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
    logging_class_path = conf.get("logging", "logging_config_class", fallback=fallback)

    # Sometimes we end up with `""` as the value!
    logging_class_path = logging_class_path or fallback

    user_defined = logging_class_path != fallback

    try:
        logging_config = import_string(logging_class_path)

        # Make sure that the variable is in scope
        if not isinstance(logging_config, dict):
            raise ValueError("Logging Config should be of dict type")

        if user_defined:
            log.info("Successfully imported user-defined logging config from %s", logging_class_path)

    except Exception as err:
        # Import default logging configurations.
        raise ImportError(
            f"Unable to load {'custom ' if user_defined else ''}logging config from {logging_class_path} due "
            f"to: {type(err).__name__}:{err}"
        )
    else:
        # Load remote logging configuration using shared discovery logic
        remote_task_log, default_remote_conn_id = discover_remote_log_handler(
            logging_class_path, fallback, import_string
        )
        if remote_task_log is None:
            provider_remote_task_log, provider_default_remote_conn_id = (
                _discover_remote_log_handler_from_providers()
            )
            if provider_remote_task_log is not None:
                remote_task_log = provider_remote_task_log
                default_remote_conn_id = provider_default_remote_conn_id
        _ActiveLoggingConfig.set(remote_task_log, default_remote_conn_id)

    return logging_config, logging_class_path


def configure_logging():
    from airflow._shared.logging import configure_logging, init_log_folder, translate_config_values

    logging_config, logging_class_path = load_logging_config()
    try:
        level: str = getattr(
            logging_config, "LOG_LEVEL", conf.get("logging", "logging_level", fallback="INFO")
        ).upper()

        colors = getattr(
            logging_config,
            "COLORED_LOG",
            conf.getboolean("logging", "colored_console_log", fallback=True),
        )
        # Try to init logging

        log_fmt, callsite_params = translate_config_values(
            log_format=getattr(logging_config, "LOG_FORMAT", conf.get("logging", "log_format", fallback="")),
            callsite_params=conf.getlist("logging", "callsite_parameters", fallback=[]),
        )
        configure_logging(
            log_level=level,
            namespace_log_levels=conf.get("logging", "namespace_levels", fallback=None),
            stdlib_config=logging_config,
            log_format=log_fmt,
            callsite_parameters=callsite_params,
            colors=colors,
        )
    except (ValueError, KeyError) as e:
        log.error("Unable to load the config, contains a configuration error.")
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

    validate_logging_config()

    new_folder_permissions = int(
        conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"),
        8,
    )

    base_log_folder = conf.get("logging", "base_log_folder")

    return init_log_folder(
        base_log_folder,
        new_folder_permissions=new_folder_permissions,
    )


def validate_logging_config():
    """Validate the provided Logging Config."""
    # Now lets validate the other logging-related settings
    task_log_reader = conf.get("logging", "task_log_reader")

    logger = logging.getLogger("airflow.task")

    def _get_handler(name):
        return next((h for h in logger.handlers if h.name == name), None)

    if _get_handler(task_log_reader) is None:
        # Check for pre 1.10 setting that might be in deployed airflow.cfg files
        if task_log_reader == "file.task" and _get_handler("task"):
            warnings.warn(
                f"task_log_reader setting in [logging] has a deprecated value of {task_log_reader!r}, "
                "but no handler with this name was found. Please update your config to use task. "
                "Running config has been adjusted to match",
                DeprecationWarning,
                stacklevel=2,
            )
            conf.set("logging", "task_log_reader", "task")
        else:
            raise AirflowConfigException(
                f"Configured task_log_reader {task_log_reader!r} was not a handler of "
                f"the 'airflow.task' logger."
            )
