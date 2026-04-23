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
# Copyright (c) 2020 ScyllaDB

import contextlib
import os
import traceback

from sdcm.dynamodb_streams_stress_thread import DynamoDBStreamsStressThread
from sdcm.dynamodb_streams_validator_thread import DynamoDBStreamsValidatorThread
from performance_regression_test import PerformanceRegressionTest


class RegressionAlternatorStreamsWithKCLTest(PerformanceRegressionTest):
    def test_validate(self):
        stress_queue = []

        self.log.info(f"Constructing stress threads")
        stress_queue.append(
            DynamoDBStreamsStressThread(
                self.loaders, 1,
                '/app/run_table_mod.py run-splits-only --count 1000 --rate 100 --keep-table',
                timeout=60,
                params=self.params,
                node_list=self.db_cluster.nodes
            )
        )
        stress_queue.append(
            DynamoDBStreamsValidatorThread(
                self.loaders, 2,
                timeout=300,
                params=self.params,
                node_list=self.db_cluster.nodes
            )
        )
        self.log.info(f"Starting stress threads")
        for stress in stress_queue:
            stress.run()

        self.log.info(f"Waiting for stress threads")
        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)
        
        for stress in stress_queue:
            sz = os.path.getsize(stress.target_file()) if os.path.exists(stress.target_file()) else -1
            self.log.info(f"file {stress.target_file()} size {sz}")
            if os.path.exists(stress.target_file()):
                with open(stress.target_file(), "r") as f:
                    for e, line in enumerate(f, start=1):
                        self.log.info(f"file {stress.target_file()} content: {e: 4}:{line.rstrip()}")

        stress, validator = stress_queue
        stress.validate_against_validator_file(stress.target_file(), validator.target_file())