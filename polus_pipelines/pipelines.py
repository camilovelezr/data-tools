# from __future__ import absolute_import
from polus.plugins import Plugin, ComputePlugin
from polus.data import Collection
from grandalf.graphs import Vertex, Edge, Graph
from typing import Union


class InvalidLink(Exception):
    pass


class DisconnectedPipeline(Exception):
    pass


class PluginNode(Vertex):
    def __init__(self, plugin: Union[Plugin, ComputePlugin]):
        if not isinstance(plugin, (Plugin, ComputePlugin)):
            raise ValueError("The node should be created with a Plugin object")
        super().__init__(plugin)


class ParameterNode(Vertex):
    def __init__(self, parameter: Collection):
        if not isinstance(parameter, Collection):
            raise ValueError("The node should be created with a Collection object")
        super().__init__(parameter)


class Link(Edge):
    def __init__(self, x, y, w=1, data=None, connect=False):
        if isinstance(x, PluginNode) and isinstance(y, PluginNode):  # two PluginNodes
            raise InvalidLink("Cannot link two PluginNodes")
        elif isinstance(x, ParameterNode) and isinstance(
            y, ParameterNode
        ):  # two ParameterNodes
            raise InvalidLink("Cannot link two ParameterNodes")
        elif not isinstance(x, (PluginNode, ParameterNode)) or not isinstance(
            y, (PluginNode, ParameterNode)
        ):  # something other than PluginNode or ParameterNode
            raise ValueError("Can only link ParameterNodes and PluginNodes")
        super().__init__(x, y, w, data, connect)


class Pipeline(Graph):
    def __init__(self, V=list[Union[ParameterNode, PluginNode]], E=list[Link]):
        if not all(isinstance(x, (PluginNode, ParameterNode)) for x in V):
            raise ValueError("Vertices must only be PluginNodes and ParameterNodes")
        if not all(isinstance(x, Link) for x in E):
            raise ValueError("All edges must be of type Link")
        super().__init__(V, E, True)  # True for directed graph
        if not self.connected():
            raise DisconnectedPipeline("The pipeline must be connected")
