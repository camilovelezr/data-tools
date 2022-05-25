from __future__ import absolute_import
import random
import unittest
from polus_pipelines.pipelines import (
    PluginNode,
    ParameterNode,
    Link,
    Pipeline,
    InvalidLink,
    DisconnectedPipeline,
)
from polus.data import collections
import polus.plugins as pp
from polus.plugins import plugins
from grandalf.graphs import Vertex, Edge


class TestPipelines(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "OmeZarrConverter" not in plugins.list:
            pp.submit_plugin(
                "https://raw.githubusercontent.com/PolusAI/polus-plugins/master/formats/polus-ome-zarr-converter-plugin/plugin.json"
            )
        if "FeatureExtraction" not in plugins.list:
            pp.submit_plugin(
                "https://raw.githubusercontent.com/PolusAI/polus-plugins/master/features/polus-feature-extraction-plugin/plugin.json",
                refresh=True,
            )

    def test_plugin_node(self):
        p1 = plugins.OmeZarrConverter
        p2 = plugins.FeatureExtraction
        pln1 = PluginNode(p1)
        pln2 = PluginNode(p2)
        # ComputePlugin
        p1_new = p1.new_schema()
        p2_new = p2.new_schema()
        pln1_new = PluginNode(p1_new)
        pln2_new = PluginNode(p2_new)

        test_list_1 = [p1, p2, p1_new, p2_new]  # Plugins
        test_list_2 = [pln1, pln2, pln1_new, pln2_new]  # PluginNodes
        for plugin in test_list_1:  # Plugins
            with self.assertRaises(ValueError):
                ParameterNode(plugin)
        for pnode in test_list_2:  # PluginNodes
            self.assertIsInstance(pnode, PluginNode)

    def test_parameter_node(self):
        c1 = collections.MaricRatBrain2019
        c2 = collections.TissueNet
        parn1 = ParameterNode(c1)
        parn2 = ParameterNode(c2)
        test_list_1 = [c1, c2]  # Collections
        test_list_2 = [parn1, parn2]  # ParameterNodes
        for collection in test_list_1:  # Collections
            with self.assertRaises(ValueError):
                PluginNode(collection)
        for parnode in test_list_2:  # ParameterNodes
            self.assertIsInstance(parnode, ParameterNode)

    def test_link(self):
        c1 = collections.MaricRatBrain2019
        c2 = collections.TissueNet
        parn1 = ParameterNode(c1)
        parn2 = ParameterNode(c2)
        p1 = plugins.ApplyFlatfield
        p2 = plugins.FeatureSubsetting
        pln1 = PluginNode(p1)
        pln2 = PluginNode(p2)
        # ComputePlugin
        p1_new = p1.new_schema()
        p2_new = p2.new_schema()
        pln1_new = PluginNode(p1_new)
        pln2_new = PluginNode(p2_new)
        par_nodes = [parn1, parn2]  # ParameterNodes
        plug_nodes = [pln1, pln2, pln1_new, pln2_new]  # PluginNodes
        for parnode in par_nodes:  # ParameterNodes
            self.assertIsInstance(parnode, ParameterNode)
        for pnode in plug_nodes:  # PluginNodes
            self.assertIsInstance(pnode, PluginNode)
        rand_par_nodes = random.choices(par_nodes, k=2)
        with self.assertRaises(InvalidLink):
            Link(*rand_par_nodes)
        rand_plug_nodes = random.choices(plug_nodes, k=2)
        with self.assertRaises(InvalidLink):
            Link(*rand_plug_nodes)
        with self.assertRaises(ValueError):
            Link(random.choice(plug_nodes), random.random())
        with self.assertRaises(ValueError):
            Link(random.choice(par_nodes), c2)
        l1 = Link(rand_par_nodes[0], rand_plug_nodes[1])
        l2 = Link(rand_par_nodes[1], rand_plug_nodes[0])
        links = [l1, l2]  # Links
        for link in links:
            self.assertIsInstance(link, Link)

    def test_pipeline(self):
        c1 = collections.MaricRatBrain2019
        c2 = collections.TissueNet
        parn1 = ParameterNode(c1)
        parn2 = ParameterNode(c2)
        p1 = plugins.ApplyFlatfield
        p2 = plugins.FeatureSubsetting
        pln1 = PluginNode(p1)
        pln2 = PluginNode(p2)
        # ComputePlugin
        p1_new = p1.new_schema()
        p2_new = p2.new_schema()
        pln1_new = PluginNode(p1_new)
        pln2_new = PluginNode(p2_new)
        par_nodes = [parn1, parn2]  # ParameterNodes
        plug_nodes = [pln1, pln2, pln1_new, pln2_new]  # PluginNodes
        for parnode in par_nodes:  # ParameterNodes
            self.assertIsInstance(parnode, ParameterNode)
        for pnode in plug_nodes:  # PluginNodes
            self.assertIsInstance(pnode, PluginNode)
        rand_par_nodes = random.choices(par_nodes, k=2)
        rand_plug_nodes = random.choices(plug_nodes, k=2)
        l1 = Link(rand_par_nodes[0], rand_plug_nodes[1])
        l2 = Link(rand_plug_nodes[1], rand_par_nodes[1])
        l3 = Link(rand_par_nodes[1], rand_plug_nodes[0])
        links = [l1, l2, l3]  # Links
        for link in links:
            self.assertIsInstance(link, Link)
        v1 = Vertex(random.randbytes(1))
        v2 = Vertex(random.randint(2, 42))
        v3 = Vertex("vertex3")
        v4 = Vertex(random.random())
        e1 = Edge(v2, v1)
        e2 = Edge(v3, v4)
        e3 = Edge(rand_par_nodes[0], rand_plug_nodes[1])
        with self.assertRaises(ValueError):
            Pipeline([v1, v2], [e1])
        with self.assertRaises(ValueError):
            Pipeline([v3, v4], [e2])
        with self.assertRaises(ValueError):
            Pipeline(par_nodes, [e3])
        nodes = rand_par_nodes + rand_plug_nodes
        pipe1 = Pipeline(nodes, links)
        pipelines = [pipe1]
        for pipeline in pipelines:
            self.assertIsInstance(pipeline, Pipeline)
        with self.assertRaises(DisconnectedPipeline):
            Pipeline(nodes, [l2])  # not connected
        with self.assertRaises(DisconnectedPipeline):
            Pipeline(nodes, [l2, l1])  # not connected
