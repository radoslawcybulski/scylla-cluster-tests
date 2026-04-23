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
from sdcm.reporting.tooling_reporter import DynamoDBStreamsStressVersionReporter
from sdcm.sct_events.loaders import DynamoDBStreamsStressEvent
from sdcm.stress.base import DockerBasedStressThread
from sdcm.stress.latte_thread import format_stress_cmd_error
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.utils import alternator

LOGGER = logging.getLogger(__name__)


class DynamoDBStreamsStressThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = "stress_image.dynamodb-streams-stress"

    def __init__(self, loader_set, node_index, stress_cmd, timeout, params, node_list):
        super().__init__(loader_set=loader_set, stress_cmd=stress_cmd, timeout=timeout, params=params, node_list=node_list)
        self._uuid_val = uuid.uuid4()
        self._node_index = node_index
        self.directory_for_output_files = os.path.join(self.loader_set.logdir, f"dynamodb-streams-stress-{self._uuid_val}")
        LOGGER.info("Output files directory: %s", self.directory_for_output_files)
        os.makedirs(self.directory_for_output_files, exist_ok=True)
        with open(os.path.join(self.directory_for_output_files, "a.log"), "w") as f:
            f.write(f"Test\n")
        with open(os.path.join(self.directory_for_output_files, "a.txt"), "w") as f:
            f.write(f"Test\n")


    def target_file(self):
        target = os.path.join(self._output_files_directory_on_master_node(), f"output_expected.log")
        return target
    
    def _output_files_directory_inside_container(self, loader_idx, cpu_idx):
        return f"/tmp/dynamodb-streams-stress-output/{self._uuid_val}-container/{loader_idx}/{cpu_idx}"

    def _output_files_directory_on_master_node(self):
        return self.directory_for_output_files

    def _output_files_main_dir_on_loaders_node(self):
        return f"/tmp/dynamodb-streams-stress-output/{self._uuid_val}"

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
        source = os.path.join(dr, "output_expected.log")
        target = self.target_file()
        LOGGER.info(f'downloading output file from loader {loader} with command: cat "{dr}/output_expected.log" to target {target}')
        try:
            loader.remoter.receive_files(source, target)
        except Exception as exc:
            LOGGER.error(f"Failed to fetch output file from loader {loader} with source {source} to target {target}: {exc}", exc_info=exc)
            raise
        finally:
            LOGGER.info(f"Finished fetching output file from loader {loader} with source {source} to target {target}")

    def validate_against_validator_file(self, expected_file, produced_file):
        for loader in self.loaders:
            if loader.node_index == self._node_index:
                cmd_runner = self._get_remote_docker(loader, self._node_index, 0)
                log_file_name = os.path.join(loader.logdir, "dynamodb-streams-stress-compare-l%s-c%s-%s.log" % (self._node_index, 0, self._uuid_val))
                def run(cmd):
                    try:
                        LOGGER.info(f"running validation command: {cmd}")
                        result = cmd_runner.run(
                            cmd=cmd,
                            timeout=self.timeout + self.shutdown_timeout,
                            log_file=log_file_name,
                            watchers=[],
                            retry=0,
                            timestamp_logs=True,
                        )
                        LOGGER.info(f"DynamoDBStreamsStress validation command finished: {result}")
                    except Exception as exc:
                        LOGGER.exception(f"DynamoDBStreamsStress validation command failed: {exc}")
                        pass
                output_files_directory = self._prepare_directory_for_output_files_on_loader_node(self._node_index, 0)
                run("whoami")
                run(f"ls -al {self._output_files_directory_inside_container(self._node_index, 0)}")
                run(f"chmod -R 0777 {self._output_files_directory_inside_container(self._node_index, 0)}")
                target_produced_file = os.path.join(output_files_directory, "output_produced_.log")
                assert os.path.isfile(produced_file), f"Produced file {produced_file} does not exist"
                LOGGER.info(f"Sending produced_file from {produced_file} to loader {loader} at {target_produced_file}")
                loader.remoter.send_files(produced_file, target_produced_file)

                assert os.path.isfile(expected_file), f"Expected file {expected_file} does not exist"
                target_expected_file = os.path.join(output_files_directory, "output_expected_.log")
                LOGGER.info(f"Sending expected_file from {expected_file} to loader {loader} at {target_expected_file}")
                loader.remoter.send_files(expected_file, target_expected_file)

#                validation_cmd = f'cd "{inside_container_directory}" && ls -al && /app/run_table_mod.py compare "{target_expected_file}" "{target_produced_file}"'
                cmds = [
                    f'cd "{self._output_files_directory_inside_container(self._node_index, 0)}" && ls -al',
                    f'cd "{self._output_files_directory_inside_container(self._node_index, 0)}" && /app/run_table_mod.py compare "{target_expected_file}" "{target_produced_file}"',
                ]
                for cmd in cmds:
                    run(cmd)
                break
        else:
            assert False, f"Loader with node_index {self._node_index} not found among loaders"

    def _get_remote_docker(self, loader, loader_idx, cpu_idx):
        if "k8s" in self.params.get("cluster_backend"):
            assert False # unsupported
        else:
            extra_docker_opts = ''

            output_files_directory = self._prepare_directory_for_output_files_on_loader_node(loader_idx, cpu_idx)
            extra_docker_opts += f" -v {output_files_directory}:{self._output_files_directory_inside_container(loader_idx, cpu_idx)}:z -v /tmp/ddd:/tmp/ddd:Z"
            cmd_runner = RemoteDocker(
                loader,
                self.docker_image_name,
                command_line="",
                extra_docker_opts=extra_docker_opts,
                docker_network=self.params.get("docker_network"),
            )
            return cmd_runner

    def _run_stress(self, loader, loader_idx, cpu_idx):  # noqa: PLR0914
        result = {}
        failure_event = finish_event = None
        if cpu_idx == 0 and self._node_index == loader_idx:
            LOGGER.info(f"running stress command with loader {loader.name} loader_idx {loader_idx} cpu_idx {cpu_idx}")
            LOGGER.info(f"Using image {self.docker_image_name} for DynamoDB Streams stress command")
            web_protocol = "http"
            is_kubernetes = self.node_list[0].is_kubernetes()
            if is_kubernetes:
                target_address = self.node_list[0].k8s_lb_dns_name
                LOGGER.info(f"target_address1: {target_address}")
                web_protocol = "http" + ("s" if self.params.get("alternator_port") == 8043 else "")
            elif self.params.get("alternator_use_dns_routing"):
                target_address = "alternator"
                LOGGER.info(f"target_address2: {target_address}")
            else:  # noqa: PLR5501
                if hasattr(self.node_list[0], "parent_cluster"):
                    target_address = self.node_list[0].parent_cluster.get_node().cql_address
                    LOGGER.info(f"target_address3: {target_address}")
                else:
                    target_address = self.node_list[0].cql_address
                    LOGGER.info(f"target_address4: {target_address}")
            target_port = self.params.get("alternator_port")
            LOGGER.info(f"target_port: {target_port}")

            access_key = self.params.get("alternator_access_key_id")
            if self.params.get("alternator_enforce_authorization"):
                secret_key = alternator.api.Alternator.get_salted_hash(node=self.node_list[0], username=access_key)
            else:
                secret_key = self.params.get("alternator_secret_access_key")

            # This is a workaround to make AWS SDK v2 Happy
            # as it will fail if the credentials are not provided,
            # even if the alternator is running in a mode that does not require authentication.
            if access_key is None or access_key == "":
                access_key = "test"
            if secret_key is None or secret_key == "":
                secret_key = "test"

            cmd_runner = self._get_remote_docker(loader, loader_idx, cpu_idx)
            cmd_runner_name = str(loader)

            stress_cmd = self.stress_cmd

            for k,v in (
                ('SCYLLADB_HOST', target_address),
                ('SCYLLADB_PORT', '9042'),
                ('SCYLLADB_USER', 'access_key'),
                ('SCYLLADB_PASS', 'secret_key'),
                ('ALTERNATOR_HOST', target_address),
                ('ALTERNATOR_PORT', target_port),
                ('ALTERNATOR_USER', access_key),
                ('ALTERNATOR_PASS', secret_key),
            ):
                stress_cmd = f'{k}="{v}" ' + stress_cmd
            stress_cmd += f" --output '{self._output_files_directory_inside_container(loader_idx, cpu_idx)}/output_expected.log'"
            try:
                reporter = DynamoDBStreamsStressVersionReporter(
                    cmd_runner, "", loader.parent_cluster.test_config.argus_client(), stress_cmd=stress_cmd
                )
                reporter.report()
            except Exception:  # noqa: BLE001
                LOGGER.info("Failed to collect DynamoDB Streams stress command version information", exc_info=True)

            if not os.path.exists(loader.logdir):
                os.makedirs(loader.logdir, exist_ok=True)
            log_file_name = os.path.join(loader.logdir, "dynamodb-streams-stress-l%s-c%s-%s.log" % (loader_idx, cpu_idx, self._uuid_val))
            LOGGER.info("dynamodb-streams-stress local log: %s", log_file_name)

            LOGGER.info("running: %s", stress_cmd)

            DynamoDBStreamsStressEvent.start(node=cmd_runner_name, stress_cmd=stress_cmd).publish()

            LOGGER.info(f"starting DynamoDBStreamsStress command: {stress_cmd}")
            try:
                LOGGER.info(f"running DynamoDBStreamsStress command: {stress_cmd}")
                result = cmd_runner.run(
                    cmd=stress_cmd,
                    timeout=self.timeout + self.shutdown_timeout,
                    log_file=log_file_name,
                    watchers=[],
                    retry=0,
                    timestamp_logs=True,
                )
                LOGGER.info(f"DynamoDBStreamsStress command finished: {result}")
                self.fetch_output_files(loader, loader_idx, cpu_idx)
            except Exception as exc:
                LOGGER.exception(f"DynamoDBStreamsStress command failed: {exc}")
                errors_str = format_stress_cmd_error(exc)
                failure_event = DynamoDBStreamsStressEvent.failure(
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
                LOGGER.info("DynamoDBStreamsStress command finished, cleaning up")
                finish_event = DynamoDBStreamsStressEvent.finish(
                    node=cmd_runner_name, stress_cmd=stress_cmd, log_file_name=log_file_name
                )
                finish_event.publish()
            LOGGER.info("DynamoDBStreamsStress command done")
        return loader, result, failure_event or finish_event
