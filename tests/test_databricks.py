import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime

import pandas as pd
import os
import luisy

from luisy import Config
from luisy.cli import build
from luisy.tasks.base import DatabricksTask
from luisy.targets import DeltaTableTarget
from luisy.decorators import (
    deltatable_input,
    deltatable_output,
)
from luisy.testing import create_testing_config
from luisy.helpers import create_hash_of_string
from luisy.code_inspection import (
    get_requirements_path,
    get_requirements_dict,
)

from luisy.hashes import (
    HashMapping,
    get_upstream_tasks,
)


@deltatable_input(catalog='A', schema='B', table_name='raw')
@deltatable_output(catalog='A', schema='B', table_name='interim')
class ToySparkTask(DatabricksTask):

    def run(self):

        df = self.input().read()
        self.write(df)


@luisy.requires(ToySparkTask)
@luisy.auto_filename
@luisy.interim
class LocalTask(DatabricksTask):

    def run(self):
        df = self.input().read()

        df_pandas = df.toPandas()

        self.write(df_pandas)


class TestSparkTask(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.df_test = pd.DataFrame(data={'a': [1], 'b': [2]})
        Config().spark.data['A.B.raw'] = self.df_test

        self.hashes = {
            "/A.B.interim.DeltaTable": "2",
            "/tests/interim/LocalTask.pkl": "3"
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    @patch("luisy.hashes.compute_hashes")
    def test_local_task(self, compute_hashes):

        # Detour hash_computation
        compute_hashes.return_value = self.hashes

        task = LocalTask()

        build(task=task, download=False)

        self.assertTrue(os.path.exists(task.get_outfile()))
        pd.testing.assert_frame_equal(task.read(), self.df_test)

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):

        # Detour hash_computation
        compute_hashes.return_value = self.hashes

        task = ToySparkTask()

        # Make sure that table does not exist before run
        self.assertNotIn('A.B.interim', Config().spark.tables)

        build(task=task, download=False)

        # Make sure that table is written
        self.assertIn('A.B.interim', Config().spark.tables)

    @patch("luisy.targets.DeltaTableTarget.get_last_modified_date")
    def test_hashes_without_target_hash(self, get_last_modified_date):

        get_last_modified_date.return_value = datetime(2023, 1, 1)

        req_path = get_requirements_path(ToySparkTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.7"

        with patch("luisy.code_inspection.get_requirements_dict", return_value=return_value):
            task = ToySparkTask()
            tasks = get_upstream_tasks(task)

            Config().set_param('include_file_hashes', False)
            mapping_with = HashMapping.compute_from_tasks(tasks, "some_project")

            Config().set_param('include_file_hashes', True)
            mapping_without = HashMapping.compute_from_tasks(tasks, "some_project")
            # TODO: fundamental problem with file hashes: hash changes after pipeline is executed.
            # Here, code has not changed, but data has

        self.assertNotEqual(
            mapping_with.hashes["/A.B.interim.DeltaTable"],
            mapping_without.hashes["/A.B.interim.DeltaTable"],
        )



class TestDeltaTableTarget(unittest.TestCase):

    @patch("luisy.targets.DeltaTableTarget.get_last_modified_date")
    def test_hash(self, get_last_modified_date):
        mock_date = datetime(2023, 1, 1)
        get_last_modified_date.return_value = mock_date
        target = DeltaTableTarget()
        self.assertEqual(
            target.get_hash(),
            create_hash_of_string(str(mock_date))
        )
