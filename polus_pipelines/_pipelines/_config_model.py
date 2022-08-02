# Pipeline config model
from pydantic import BaseModel, Field, validator
from enum import Enum
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
