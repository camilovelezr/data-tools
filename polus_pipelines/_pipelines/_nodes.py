from uuid import UUID, uuid4
from typing import Union, Optional, Any
from pathlib import Path
from grandalf.graphs import Vertex, Edge

# from ._config_model import PluginNodeModel
from polus.plugins import Plugin, ComputePlugin, load_config
from ._utils import _add_to_dict
import json
import logging

logger = logging.getLogger("polus.pipelines")


class Node(Vertex):
    def __init__(self, data):
        super().__init__(data)


class ParameterNode(Node):
    uuid: UUID

    def __init__(self, parameter: Optional[Path] = None):
        if parameter:
            if not isinstance(parameter, Path):
                raise ValueError("The node should be created from a Path")

        super().__init__(parameter)
        self.uuid = uuid4()
        logger.debug(
            "Initialized ParameterNode with UUID %s and data %s"
            % (self.uuid, self.data)
        )

    def get_data(self):
        return str(self.data)


class PluginNode(Node):
    uuid: UUID
    _links: dict
    _vertices: dict

    def __init__(self, plugin: Union[Plugin, ComputePlugin]):
        if not isinstance(plugin, (Plugin, ComputePlugin)):
            raise ValueError("The node should be created from a Plugin object")
        self._in = [x.name for x in plugin.inputs]
        self._out = [x.name for x in plugin.outputs]
        super().__init__(plugin)
        self.uuid = uuid4()
        self._links = {}
        self._vertices = {}
        logger.debug(
            "Initialized PluginNode with UUID %s and data %s"
            % (self.uuid, self.data.name)
        )
        if "outDir" in self._out:
            self.outDir = ParameterNode()

    def __getattribute__(self, __name: str) -> Any:
        if __name == "_in" or __name == "_out":
            return super().__getattribute__(__name)
        elif (
            __name in self._in or __name in self._out
        ) and __name not in self._vertices.keys():
            return getattr(self.data, __name)
        return super().__getattribute__(__name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "_in" or __name == "_out":
            return super().__setattr__(__name, __value)
        if hasattr(self, "_in") and __name in self._in:
            if isinstance(__value, Path):
                v = ParameterNode(__value)
                super().__setattr__(__name, v)
                self._vertices[__name] = v
                self._links[__name] = Link(v, self)
            elif isinstance(__value, ParameterNode):
                super().__setattr__(__name, __value)
                self._vertices[__name] = __value
                self._links[__name] = Link(__value, self)
            else:
                setattr(self.data, __name, __value)
            logger.debug("Input attribute %s set to %s" % (__name, __value))
        elif hasattr(self, "_out") and __name in self._out:
            if isinstance(__value, Path):
                v = ParameterNode(__value)
                super().__setattr__(__name, v)
                self._vertices[__name] = v
                self._links[__name] = Link(self, v)
            elif isinstance(__value, ParameterNode):
                super().__setattr__(__name, __value)
                self._vertices[__name] = __value
                self._links[__name] = Link(self, __value)
            else:
                setattr(self.data, __name, __value)
            logger.debug("Output attribute %s set to %s" % (__name, __value))
        else:
            super().__setattr__(__name, __value)
            logger.debug("Node attribute %s set to %s" % (__name, __value))
            return

    def _extend_vertices(self):
        return {
            k: {"data": str(v.data), "uuid": str(v.uuid)}
            for k, v in self._vertices.items()
        }

    def get_data(self):
        return self.data.name

    @classmethod
    def load_config(cls, config):
        plugin = load_config(config.config)  # from polus-plugins
        node = PluginNode(plugin)
        for io in plugin._io_keys.items():
            if io[1].value:
                setattr(node, io[0], io[1].value)
        return node

        # dump workflow config into CWL.yml workflow
        # even if it is buggy, do CWL
        # create epic to create unittest for plugins and pipelinese


def find_shared(tuple):
    """Find shared ParameterNodes between a pair of PluginNodes.

    Assumes that `itertools.combinations()` will give a list containing tuples
    of the form (n, m) where n < m (that is, n goes before m in the order
    of execution).
    """
    base_dict = {v: [k] for (k, v) in tuple[0]._vertices.items()}
    for kv in tuple[1]._vertices.items():
        _add_to_dict(base_dict, kv)
    return [(v, k.data) for (k, v) in base_dict.items() if len(v) > 1]


class InvalidLink(Exception):
    pass


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
