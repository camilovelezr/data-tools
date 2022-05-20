from __future__ import absolute_import
import unittest
from polus_pipelines.pipelines import PluginNode, ParameterNode, Link, Pipeline
from polus.data import collections
from polus.plugins import plugins


class TestPipelines(unittest.TestCase):
    def test_plugin_node(self):
        p1 = plugins.ApplyFlatfield
        p2 = plugins.FeatureSubsetting
        with self.assertRaises(ValueError):
            ParameterNode(p1)
        with self.assertRaises(ValueError):
            ParameterNode(p2)
        pln1 = PluginNode(p1)
        pln2 = PluginNode(p2)
        self.assertIsInstance(pln1, PluginNode)
        self.assertIsInstance(pln2, PluginNode)
