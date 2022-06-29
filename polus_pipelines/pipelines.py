# from __future__ import absolute_import
from polus.plugins import Plugin, ComputePlugin
from grandalf.graphs import Vertex, Edge, Graph
from typing import Union, Optional, Type, Any
from pathlib import Path
from uuid import UUID, uuid4
import fsspec
from fsspec.implementations.local import LocalFileSystem
from os import getcwd
from tqdm import tqdm


class InvalidLink(Exception):
    pass


class DisconnectedPipeline(Exception):
    pass


class CyclicPipeline(Exception):
    pass


class InvalidIO(Exception):
    pass


class ParameterNode(Vertex):
    uuid: UUID

    def __init__(self, parameter: Optional[Path] = None):
        if parameter:
            if not isinstance(parameter, Path):
                raise ValueError("The node should be created from a Path")

        super().__init__(parameter)
        self.uuid = uuid4()


class PluginNode(Vertex):
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
        # elif (
        #     hasattr(self, "_in")
        #     and hasattr(self, "_out")
        #     and not (__name in self._in or __name in self._out)
        # ):
        #     raise InvalidIO("Invalid I/O parameter")
        else:
            return super().__setattr__(__name, __value)


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
        for x in filter(lambda x: isinstance(x, PluginNode), V):
            vertices.extend(x._vertices.values())
            edges.extend(x._links.values())
        V = list(set(V + vertices))
        super().__init__(V, edges, True)
        self.uuid = uuid4()
        if not self.connected():
            raise DisconnectedPipeline("The pipeline must be connected")

        if not fs:
            self._fs = LocalFileSystem()  # default fs
            cwd = Path(getcwd())
            data_path = cwd.joinpath(Path(data_path))
            data_path = data_path.joinpath(str(self.uuid))

        else:
            if not issubclass(type(fs), fsspec.spec.AbstractFileSystem):
                raise ValueError("fs argument must be an fsspec FileSystem")
            self._fs = fs
            if data_path == Path(".workflow"):
                raise ValueError(
                    "A data_path must be specified when specifying a FileSystem"
                )
            self.data_path = Path(data_path).joinpath(str(self.uuid))

        # self._traverse(_cyclic=True)  # check if cyclic. Throw error if yes

    def __create_dirs(self):
        # create data_path dir
        if not self._fs.exists(str(self.data_path)):
            self._fs.mkdir(str(self.data_path))

        for node in V:
            d = self.data_path.joinpath(str(node.uuid))
            self._fs.mkdir(str(d))

        for x in filter(lambda x: isinstance(x, PluginNode), V):
            x.data._fs = self._fs  # set filesystem in Plugin object
            for k, v in x._vertices.items():
                if v.data is None:  # Empty ParameterNodes (outDir...)
                    v.data = self.data_path.joinpath(str(x.uuid))
                    setattr(x.data, k, v.data)
                else:
                    setattr(
                        x.data, k, v.data
                    )  # set I/O values in plugin object LAST. NEEDED FOR FILESYSTEMS

    @property
    def _starting(self):  # possible starting nodes
        st = []
        for node in self.C[0].sV:
            if len(node.e_in()) == 0:
                st.append(node)
        return st

    def _traverse(
        self,
        start: Optional[Union[ParameterNode, PluginNode]] = None,
        _plugins: bool = False,
        _cyclic: bool = False,
    ):
        if not start and len(self._starting) == 1:
            start = self._starting[0]
        assert start in self._starting, "Invalid starting node"
        # order = self.order()
        visited = []
        frontier = [start]
        while len(frontier) > 0:
            new_frontier = [
                n
                for v in frontier
                for n in v.N(1)  # outward neighbors of V
                if not ((n in visited) or (n in frontier))
            ]
            # else:
            #     new_frontier = [
            #         n for v in frontier for n in v.N(1)  # outward neighbors of V
            #     ]
            #     if any(
            #         [
            #             v in filter(lambda q: isinstance(q, PluginNode), visited)
            #             for v in new_frontier
            #         ]
            #     ):
            #         raise CyclicPipeline("The pipeline cannot contain cycles")
            visited.extend(frontier)
            frontier = new_frontier
        if _plugins:
            return list(filter(lambda x: isinstance(x, PluginNode), visited))
        return visited

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


# class _default_view:
#     w, h = 10, 10


# class Workflow(SugiyamaLayout):
#     def __init__(self, pipeline: Pipeline):
#         for v in pipeline.V():
#             v.view = _default_view
#         super().__init__(pipeline.C[0])
#         # self.init_all(inverted_edges=[pipeline.C[0].sE[-1]])

#     def __getattribute__(self, __name: str) -> Any:
#         return super().__getattribute__(__name)
