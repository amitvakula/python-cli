import collections 
import copy

from abc import ABC, abstractmethod

CONTAINERS = ['group', 'project', 'subject', 'session', 'acquisition']

def combine_path(path, child):
    if path:
        return path + '/' + child
    return child

class ContainerNode(object):
    def __init__(self, container_type, uid=None, label=None, exists=False):
        self.container_type = container_type
        self.id = uid
        self.label = label
        self.children = []
        self.exists = exists
        self.context = None
        self.files = []
        self.packfiles = []

    def path_el(self):
        if self.container_type == 'group':
            return self.id
        if self.id:
            return '<id:{}>'.format(self.id)
        return self.label

class ContainerResolver(ABC):
    def __init__(self):
        """Interface that handles resolution and creation of containers"""
        pass

    @abstractmethod
    def resolve_path(self, container_type, path):
        """Resolve a container id by path
        
        Arguments:
            container_type (str): The container type hint
            path (str): The resolver path

        Returns:
            str: The id if the container exists, otherwise None
        """
        return None

    @abstractmethod
    def create_container(self, parent, container):
        """Create the container described by container as a child of parent.

        Arguments:
            parent (ContainerNode): The parent container (or None if creating a group)
            container (ContainerNode): The container to create

        Returns:
            str: The id of the newly created container
        """
        return None


class ContainerFactory(object):
    def __init__(self, resolver):
        """Manages discovery and creation of containers by looking at context objects.

        Arguments:
            resolver (ContainerResolver): The container resolver strategy
        """
        self.resolver = resolver

        # The root container
        self.root = ContainerNode('root', exists=True)

    def resolve(self, context):
        """Given a context with hierarchy definitions, returns a ContainerNode if resolved.

        If the node definition is ambiguous (i.e. missing an intermediate level such as project), then this
        function returns None. This is fine if there is nothing to upload at the given node.

        Arguments:
            context (dict): The context containing the node definition

        Returns:
            ContainerNode: If the container node was resolved in the tree.
        """
        # Create containers as we go
        last = None
        current = self.root
        path = ''
        for container in CONTAINERS:
            if container in context:
                # Missing node, return None
                if current is None:
                    return None

                last = current
                current = self._resolve_child(current, container, context, path)
                path = combine_path(path, current.path_el())
            else:
                if current:
                    last = current
                current = None

        return current or last

    def create_containers(self):
        """Invoke resolver.create_container for each container that doesn't exist"""
        for parent, child in self.walk_containers():
            if not child.exists:
                child.id = self.resolver.create_container(parent, child)
                child.exists = True

    def walk_containers(self):
        """Breadth-first walk of containers resolved by this factory

        Yields:
            ContainerNode, ContainerNode: parent, child container nodes
        """
        queue = collections.deque() 
        for child in self.root.children:
            queue.append((None, child))

        while queue:
            parent, current = queue.popleft()

            for child in current.children:
                queue.append((current, child))

            yield parent, current

    def _resolve_child(self, parent, container_type, context, path):
        """Resolve a child by searching the parent, or creating a new node
        
        Arguments:
            parent (ContainerNode): The parent node
            container_type (str): The container type
            context (dict): The context object

        Returns:
            ContainerNode: The new or existing container node
        """
        subcon = context[container_type]
        uid = subcon.get('_id')
        label = subcon.get('label')

        for child in parent.children:
            # Prefer resolve by id
            if uid and child.id == uid:
                return child

            if label and child.label == label:
                # In case we resolved this elsewhere, update the child id
                if uid and not child.id:
                    child.id = uid

                return child

        # Create child
        child = ContainerNode(container_type, uid=uid, label=label)
        child.context = copy.deepcopy(subcon)

        # Check if exists
        if self.resolver and parent.exists:
            path = combine_path(path, child.path_el())
            uid = self.resolver.resolve_path(container_type, path)
            if uid:
                child.id = uid
                child.exists = True

        parent.children.append(child)
        return child

