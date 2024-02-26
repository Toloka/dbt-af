import collections
import typing as tp

from dbt_af.parser.dbt_node_model import DbtNode
from dbt_af.parser.dbt_source_model import DbtSource


class DbtModelPathGraph:
    def __init__(self, nodes: tp.List[DbtNode], sources: tp.List[DbtSource]):
        self.dbt_nodes = nodes
        self.dbt_sources = sources

        self.node_to_path = {}
        self.path_to_path = collections.defaultdict(set)
        self.full_path_to_path = collections.defaultdict(set)

        self._build_path_dependencies()

    @classmethod
    def from_manifest(cls, manifest: dict) -> 'DbtModelPathGraph':
        nodes = []
        for node_info in manifest['nodes'].values():
            node = DbtNode(**node_info)
            if node.resource_type in ('model', 'snapshot', 'test'):
                nodes.append(node)

        sources = [DbtSource(**source_info) for source_info in manifest['sources'].values()]

        return cls(nodes, sources)

    def _build_path_dependencies(self):
        # step1 Create dict node->path

        self.node_to_path = {node.unique_id: node.original_file_path_dirname for node in self.dbt_nodes}
        self.node_to_path.update({source.unique_id: source.original_file_path_dirname for source in self.dbt_sources})

        # step2 Create dict path (depends on) set of path
        for node in self.dbt_nodes:
            for dep in node.depends_on:
                dep_path = self.node_to_path[dep]
                if dep_path != node.original_file_path_dirname:
                    self.path_to_path[node.original_file_path_dirname].add(self.node_to_path[dep])
            for dep in node.depends_on_sources:
                dep_path = self.node_to_path[dep]
                if dep_path != node.original_file_path_dirname:
                    self.path_to_path[node.original_file_path_dirname].add(self.node_to_path[dep])

        # step3 Create dict path (depends on) set of all pathes
        for path, dep_path in self.path_to_path.copy().items():
            # all pathes form step2 above
            self.full_path_to_path[path].update(dep_path)

            # create queue with pathes we depends on
            queue = collections.deque()
            queue.extend(dep_path)

            # while queue is not empty
            while len(queue) > 0:
                # take one path
                item = queue.popleft()

                # extend our dependencies and queue by dependencies of this taken path form queue
                for item_path in self.path_to_path[item]:
                    if item_path not in self.full_path_to_path[path]:
                        self.full_path_to_path[path].add(item_path)
                        queue.append(item_path)
