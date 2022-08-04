# Pipeline config model
from pydantic import BaseModel, Field, validator
from enum import Enum
from pathlib import Path
from ._nodes import Node, PluginNode, ParameterNode, Link
from ._utils import _filter


class NodeEnum(str, Enum):
    PluginNode = "PluginNode"
    ParameterNode = "ParameterNode"


class PluginNodeModel(BaseModel):
    config: dict

    def __init__(self, node):
        data = {}
        data["config"] = node.data._config_file
        super().__init__(**data)


class PipelineConfig(BaseModel):
    nodes: list[PluginNodeModel]

    def __init__(self, **data):
        data["nodes"] = [
            PluginNodeModel(node) for node in _filter(PluginNode, data["nodes"])
        ]
        super().__init__(**data)


def create_parameter_nodes(nodes: list[PluginNodeModel]) -> dict[Path, ParameterNode]:
    """Create ParameterNodes used for setting I/O values in config.
    Return a dict {Path: ParameterNode} of all ParameterNodes in a list of
    PluginNodeModels.
    """
    return {
        v["value"]: ParameterNode(v["value"])
        for p in nodes
        for v in p.config["_io_keys"].values()
        if isinstance(v["value"], Path)
    }
