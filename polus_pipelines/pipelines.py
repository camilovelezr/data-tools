# from __future__ import absolute_import
# from functools import singledispatch
import json
from tkinter import W
from polus.plugins import load_config
from grandalf.graphs import Edge, graph_core
from grandalf.layouts import SugiyamaLayout
from typing import Generator, Tuple, Union, Optional, Type, Any
from pathlib import Path
from uuid import UUID, uuid4
import fsspec
from fsspec.implementations.local import LocalFileSystem
from os import getcwd
from tqdm import tqdm
import logging
from itertools import combinations
from ._pipelines._nodes import ParameterNode, PluginNode, Link, set_parameter_nodes
from ._pipelines._config_model import PipelineConfig, create_parameter_nodes
from ._pipelines._utils import _filter

"""
Set up logging for the module
"""
logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("polus.pipelines")
logger.setLevel(logging.INFO)


class DisconnectedPipeline(Exception):
    pass


class CyclicPipeline(Exception):
    pass


class InvalidIO(Exception):
    pass


class Pipeline(graph_core):
    uuid: UUID
    data_path: Path
    _fs: Type[fsspec.spec.AbstractFileSystem]

    def __init__(
        self,
        V: list[Union[ParameterNode, PluginNode]],
        edges: Optional[list[Link]] = None,
        data_path: Optional[Union[Path, str]] = Path(".workflow"),
        fs: Optional[Type[fsspec.spec.AbstractFileSystem]] = None,
    ):
        if not all(isinstance(x, (PluginNode, ParameterNode)) for x in V):
            raise ValueError("Vertices must only be PluginNodes and ParameterNodes")
        vertices = []
        if not edges:
            edges = []
        for x in _filter(PluginNode, V):
            vertices.extend(x._vertices.values())
            edges.extend(x._links.values())
        V = list(set(list(V) + vertices))
        try:
            super().__init__(V, edges, directed=True)
        except BaseException as e:
            if "unconnected" in e.args[0]:
                raise DisconnectedPipeline("The pipeline must be connected") from e
            else:
                raise e
        self.uuid = uuid4()

        if not fs:
            self._fs = LocalFileSystem()  # default fs
            cwd = Path(getcwd())
            data_path = cwd.joinpath(Path(data_path))
            self.data_path = data_path.joinpath(str(self.uuid))

        else:
            if not issubclass(type(fs), fsspec.spec.AbstractFileSystem):
                raise ValueError("fs argument must be a fsspec FileSystem")
            self._fs = fs
            if data_path == Path(".workflow"):
                raise ValueError(
                    "A data_path must be specified when specifying a FileSystem"
                )
            self.data_path = Path(data_path).joinpath(str(self.uuid))

        self._check_cycles()  # check for cycles
        self.__mkdirdone = False

    @property
    def links(self):
        return self.E()

    @property
    def nodes(self) -> Generator:
        return self.V()

    def _check_cycles(self) -> None:
        unvisited_links = list(self.links)
        out_counts = {node: len(node.e_out()) for node in self.nodes}
        while len(out_counts) > 0:
            ln = [x for (x, y) in out_counts.items() if y == 0]
            if len(ln) == 0:
                raise CyclicPipeline("The Pipeline cannot contain any cycles.")
            for node in ln:
                for ilink in filter(lambda x: x in unvisited_links, node.e_in()):
                    nt = list(filter(lambda x: x != node, ilink.v))[0]  # tail node
                    out_counts[nt] -= 1
                    unvisited_links.remove(ilink)
                out_counts.pop(node)

    def _traverse(
        self,
        start: Optional[Union[ParameterNode, PluginNode]] = None,
        plugins: bool = False,
    ) -> list:
        if not start and len(self.roots()) == 1:
            start = self.roots()[0]
        assert start in self.roots(), "Invalid starting node"
        visited = []
        frontier = [start]
        while len(frontier) > 0:
            new_frontier = [
                n
                for v in frontier
                for n in v.N(1)  # outward neighbors of V
                if not ((n in visited) or (n in frontier))
            ]
            visited.extend(frontier)
            frontier = new_frontier
        if plugins:
            return _filter(PluginNode, visited)
        return visited

    def _make_dirs(self) -> None:
        """Creates dir structure and sets I/O values in PluginNodes"""
        if not self.__mkdirdone:
            logger.debug("Making DIRS")
            # create data_path dir
            if not self._fs.exists(str(self.data_path)):
                self._fs.mkdir(str(self.data_path))

            for node in self.nodes:
                d = self.data_path.joinpath(str(node.uuid))
                self._fs.mkdir(str(d))

            for x in _filter(PluginNode, self.nodes):
                x.data._fs = self._fs  # set filesystem in Plugin object
                logger.debug(f"x is {x} and x._vertices is {x._vertices}")
                for k, v in x._vertices.items():
                    if v.is_empty():  # Empty ParameterNodes (outDir...)
                        v.data = self.data_path.joinpath(
                            str(v.uuid)
                        )  # create dir with uuid of parnode
                        logger.debug("%s is empty, k=%s, uuid=%s" % (v, k, v.uuid))
                        setattr(x.data, k, v.data)  # set I/O value now that dir exists
                    else:
                        setattr(
                            x.data, k, v.data
                        )  # set I/O values in plugin object LAST. NEEDED FOR FILESYSTEMS
            self.__mkdirdone = True

    def _create_index(self):
        self._make_dirs()
        index = {}
        for node in _filter(PluginNode, self.nodes):
            index[f"{str(node.data.__class__.__name__)}"] = node._extend_vertices()
            index[f"{str(node.data.__class__.__name__)}"].update(
                {"uuid": str(node.uuid)}
            )
            node.data.save_config(
                self.data_path.joinpath(str(node.uuid), "config.json")
            )
        index["PluginOrder"] = [x.data.name for x in self._traverse(_plugins=True)]
        with open(self.data_path.joinpath("index.json"), "w") as file:
            json.dump(index, file, indent=4)

    @property
    def _config(self):
        self._make_dirs()
        config = {}
        config["nodes"] = list(self.nodes)
        return config

    @property
    def config(self):
        return PipelineConfig(**{"nodes": self._config["nodes"]})

    def __parse_config(config: Union[PipelineConfig, Path]):
        if isinstance(config, Path):
            with open(config, "r") as file:
                config = PipelineConfig(json.load(file))
        elif not isinstance(config, PipelineConfig):
            raise TypeError("config must be one of PipelineConfig or Path")
        return config

    # @singledispatch
    # def __parse_config(config):
    #     raise TypeError("config must be one of PipelineConfig or Path")

    # @__parse_config.register
    # def _(config: Path):
    #     with open(config, "r") as file:
    #         config = PipelineConfig(json.load(file))
    #     return config

    # @__parse_config.register
    # def _(config: PipelineConfig):
    #     return config

    @classmethod
    def load_config(cls, config: Union[PipelineConfig, Path]):
        config = cls.__parse_config(config)
        plugin_nodes = [PluginNode.load_config(model) for model in config.nodes]
        # pluginnodes ready without parameternodes
        parameter_nodes = create_parameter_nodes(config.nodes)
        set_parameter_nodes(plugin_nodes, parameter_nodes)
        return cls(plugin_nodes)

    @property
    def SugiyamaLayout(self):
        class _default_view:
            def __init__(self):
                self.w = 10
                self.h = 10

        for v in self.nodes:
            v.view = _default_view()
        return SugiyamaLayout(self)

    def run(self, start: Optional[Union[ParameterNode, PluginNode]] = None, **kwargs):
        plugins = self._traverse(start, _plugins=True)
        # data hash, based on data, change in parnodes?
        # unique hash for each set of plugin settings
        # I want to tweak this one plugin at the end of my pipeline
        # but I only want to rerun the necessary plugins
        # needed to get the new result
        # rerun hash in inpnodes, change in input data? no: all pluginnodes
        # that didnt change, skip
        # yes change: run. Useful to increment a pipeline
        for plugin in tqdm(plugins, desc="Executing workflow"):
            plugin.data.run(**kwargs)

        # when reload a pipeline: new UID


# class Workflow(SugiyamaLayout):
#     def __init__(self, pipeline: Pipeline):
#         for v in pipeline.V():
#             v.view = _default_view
#         super().__init__(pipeline.C[0])
#         # self.init_all(inverted_edges=[pipeline.C[0].sE[-1]])

#     def __getattribute__(self, __name: str) -> Any:
#         return super().__getattribute__(__name)
