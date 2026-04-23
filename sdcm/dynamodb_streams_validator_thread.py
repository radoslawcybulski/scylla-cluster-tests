# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

from glob import glob
import logging, os
import uuid

from sdcm.remote.base import FailuresWatcher
from sdcm.reporting.tooling_reporter import DynamoDBStreamsValidatorVersionReporter
from sdcm.sct_events.loaders import DynamoDBStreamsValidatorEvent
from sdcm.stress.base import DockerBasedStressThread
from sdcm.stress.latte_thread import format_stress_cmd_error
from sdcm.utils.docker_remote import RemoteDocker

LOGGER = logging.getLogger(__name__)


class DynamoDBStreamsValidatorThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = "stress_image.dynamodb-streams-validator"

    def __init__(self, loader_set, node_index, timeout, params, node_list):
        stress_cmd = 'java -jar /app/app.jar'
        super().__init__(loader_set=loader_set, stress_cmd=stress_cmd, timeout=timeout, params=params, node_list=node_list)
        self._uuid_val = uuid.uuid4()
        self._node_index = node_index
        self.directory_for_output_files = os.path.join(self.loader_set.logdir, f"dynamodb-streams-validator-{self._uuid_val}")
        LOGGER.info("Output files directory: %s", self.directory_for_output_files)
        os.makedirs(self.directory_for_output_files, exist_ok=True)
        with open(os.path.join(self.directory_for_output_files, "a.log"), "w") as f:
            f.write(f"Test\n")
        with open(os.path.join(self.directory_for_output_files, "a.txt"), "w") as f:
            f.write(f"Test\n")

    def target_file(self):
        target = os.path.join(self._output_files_directory_on_master_node(), f"output_produced.log")
        return target
    
    def _output_files_directory_inside_container(self, loader_idx, cpu_idx):
        return f"/tmp/dynamodb-streams-validator-output/{self._uuid_val}-container/{loader_idx}/{cpu_idx}"

    def _output_files_directory_on_master_node(self):
        return self.directory_for_output_files

    def _output_files_main_dir_on_loaders_node(self):
        return f"/tmp/dynamodb-streams-validator-output/{self._uuid_val}"

    def _output_files_directory_on_loaders_node(self, loader_idx, cpu_idx):
        return f"{self._output_files_main_dir_on_loaders_node()}/{loader_idx}/{cpu_idx}"

    # def _initialize_output_file_logger(self, loader, loader_idx, cpu_idx):
    #     class HDRHistogramFileLoggerCheckForExistingFile(HDRHistogramFileLogger):
    #         @cached_property
    #         def _logger_cmd_template(self) -> str:
    #             return f"test -f {self._remote_log_file} && tail -f {self._remote_log_file} -c +0"

    #         def stop(self):
    #             LOGGER.info(f"Stopping HDR logger {self._remote_log_file} -> {self.target_log_file}")
    #             super().stop()
    #             try:
    #                 if os.path.isfile(self.target_log_file) and os.path.getsize(self.target_log_file) == 0:
    #                     LOGGER.info(f"Removing empty hdr file {self.target_log_file}")
    #                     os.remove(self.target_log_file)
    #             except Exception as e:
    #                 LOGGER.exception(f"Error removing empty hdr file {self.target_log_file}, error is ignored: {e}")

    #     contextes = []
    #     for work_type in self.WORK_TYPES:
    #         loaders_node_path = self._hdr_files_directory_on_loaders_node(loader_idx, cpu_idx)
    #         master_node_path = self._hdr_files_directory_on_master_node()
    #         LOGGER.info(f"Creating masters node HDR files directory: {master_node_path}")
    #         os.makedirs(master_node_path, exist_ok=True)
    #         LOGGER.info(
    #             f"Initializing HDR logger with remote={loaders_node_path}/hdrh-{work_type}.hdr and target={master_node_path}/hdrh-{loader_idx}-{work_type}-{cpu_idx}.hdr"
    #         )
    #         hdrh_logger = HDRHistogramFileLoggerCheckForExistingFile(
    #             node=loader,
    #             remote_log_file=f"{loaders_node_path}/hdrh-{work_type}.hdr",
    #             target_log_file=f"{master_node_path}/hdrh-{loader_idx}-{work_type}-{cpu_idx}.hdr",
    #         )
    #         contextes.append(hdrh_logger)
    #         hdrh_logger.remove_remote_log_file()
    #         hdrh_logger.start()
    #     return contextes

    def _prepare_directory_for_output_files_on_loader_node(self, loader_idx, cpu_idx):
        loaders_node_path = self._output_files_directory_on_loaders_node(loader_idx, cpu_idx)
        LOGGER.info(f"Preparing output files directory: {loaders_node_path}")
        os.makedirs(loaders_node_path, exist_ok=True)
        return loaders_node_path

    def fetch_output_files(self, loader, loader_idx, cpu_idx):
        dr = self._prepare_directory_for_output_files_on_loader_node(loader_idx, cpu_idx)
        source = os.path.join(dr, "output_produced.log")
        target = self.target_file()
        LOGGER.info(f'downloading output file from loader {loader} with command: cat "{dr}/output_produced.log" to target {target}')
        try:
            loader.remoter.receive_files(source, target)
        except Exception as exc:
            LOGGER.error(f"Failed to fetch output file from loader {loader} with source {source} to target {target}: {exc}", exc_info=exc)
            raise
        finally:
            LOGGER.info(f"Finished fetching output file from loader {loader} with source {source} to target {target}")

    def _run_stress(self, loader, loader_idx, cpu_idx): 
        result = {}
        failure_event = finish_event = None
        if cpu_idx == 0 and self._node_index == loader_idx:
            LOGGER.info(f"running stress command with loader {loader.name} loader_idx {loader_idx} cpu_idx {cpu_idx}")
            LOGGER.info(f"Using image {self.docker_image_name} for DynamoDB Streams Validator")
            web_protocol = "http"
            is_kubernetes = self.node_list[0].is_kubernetes()
            if is_kubernetes:
                target_address = self.node_list[0].k8s_lb_dns_name
                web_protocol = "http" + ("s" if self.params.get("alternator_port") == 8043 else "")
            elif self.params.get("alternator_use_dns_routing"):
                target_address = "alternator"
            else:  # noqa: PLR5501
                if hasattr(self.node_list[0], "parent_cluster"):
                    target_address = self.node_list[0].parent_cluster.get_node().cql_address
                else:
                    target_address = self.node_list[0].cql_address
            target_port = self.params.get("alternator_port")

            if "k8s" in self.params.get("cluster_backend"):
                assert False # unsupported
            else:
                extra_docker_opts = ''
                for k,v in (
                    # we care about table name here only, rest is just to make ARN looks valid for KCL
                    ('STREAM_ARN', "arn:aws:dynamodb:us-east-1:000000000000:table/alternator_dynamodb_streams_verification_table_rc@dynamodb_streams_verification_table_rc_scylla_cdc_log/stream/2026-03-12T09:23:21.805000192"),
                    ('STREAMS_ENDPOINT', f"{web_protocol}://{target_address}:{target_port}"),
                    ('LEASE_TABLE_ENDPOINT', f"{web_protocol}://{target_address}:{target_port}"),
                    ('AWS_REGION', "us-east-1"),
                ):
                    extra_docker_opts += f' -e "{k}={v}"'
                output_files_directory = self._prepare_directory_for_output_files_on_loader_node(loader_idx, cpu_idx)
                extra_docker_opts += f" -v {output_files_directory}:{self._output_files_directory_inside_container(loader_idx, cpu_idx)}:z"
                cmd_runner = RemoteDocker(
                    loader,
                    self.docker_image_name,
                    command_line="",
                    extra_docker_opts=extra_docker_opts,
                    docker_network=self.params.get("docker_network"),
                )
                cmd_runner_name = str(loader)

            stress_cmd = f'cd {self._output_files_directory_inside_container(loader_idx, cpu_idx)} && {self.stress_cmd}'

            try:
                reporter = DynamoDBStreamsValidatorVersionReporter(
                    cmd_runner, "", loader.parent_cluster.test_config.argus_client(), stress_cmd=stress_cmd
                )
                reporter.report()
            except Exception:  # noqa: BLE001
                LOGGER.info("Failed to collect DynamoDB Streams Validator version information", exc_info=True)

            if not os.path.exists(loader.logdir):
                os.makedirs(loader.logdir, exist_ok=True)
            log_file_name = os.path.join(loader.logdir, "dynamodb-streams-validator-l%s-c%s-%s.log" % (loader_idx, cpu_idx, self._uuid_val))
            LOGGER.info("dynamodb-streams-validator local log: %s", log_file_name)

            LOGGER.info("running: %s", stress_cmd)

            DynamoDBStreamsValidatorEvent.start(node=cmd_runner_name, stress_cmd=stress_cmd).publish()

            LOGGER.info(f"starting DynamoDBStreamsValidator stress command: {stress_cmd}")
            try:
                LOGGER.info(f"running DynamoDBStreamsValidator stress command: {stress_cmd}")
                result = cmd_runner.run(
                    cmd=stress_cmd,
                    timeout=self.timeout + self.shutdown_timeout,
                    log_file=log_file_name,
                    watchers=[],
                    retry=0,
                    timestamp_logs=True,
                )
                LOGGER.info(f"DynamoDBStreamsValidator command finished: {result}")
                self.fetch_output_files(loader, loader_idx, cpu_idx)
            except Exception as exc:
                LOGGER.exception(f"DynamoDBStreamsValidator command failed: {exc}")
                errors_str = format_stress_cmd_error(exc)
                failure_event = DynamoDBStreamsValidatorEvent.failure(
                    node=cmd_runner_name,
                    stress_cmd=self.stress_cmd,
                    log_file_name=log_file_name,
                    errors=[
                        errors_str,
                    ],
                )
                failure_event.publish()
                raise
            finally:
                LOGGER.info("DynamoDBStreamsValidator stress command finished, cleaning up")
                finish_event = DynamoDBStreamsValidatorEvent.finish(
                    node=cmd_runner_name, stress_cmd=stress_cmd, log_file_name=log_file_name
                )
                finish_event.publish()
            LOGGER.info("DynamoDBStreamsValidator stress command done")
        return loader, result, failure_event or finish_event
