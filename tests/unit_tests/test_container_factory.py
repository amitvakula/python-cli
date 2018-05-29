from flywheel_cli.importers.container_factory import ContainerFactory, ContainerResolver

class MockContainerResolver(ContainerResolver):
    def __init__(self, paths=None):
        if paths:
            self.paths = paths
        else:
            self.paths = {}
        self.created_nodes = []

    def resolve_path(self, container_type, path):
        if path in self.paths:
            return self.paths[path]
        return None

    def create_container(self, parent, container):
        self.created_nodes.append((parent, container))
        return 'created_' + container.label.lower()

def test_resolve_group_not_exist():
    resolver = MockContainerResolver()
    factory = ContainerFactory(resolver)
    context = {'group': {'_id': 'foo'}}

    result = factory.resolve(context)
    assert result is not None
    assert result.container_type == 'group'
    assert result.id == 'foo'
    assert not result.exists

    result2 = factory.resolve(context)
    assert id(result) == id(result2)

def test_resolve_group_exists():
    resolver = MockContainerResolver({
        'foo': 'foo'    
    })

    factory = ContainerFactory(resolver)
    context = {'group': {'_id': 'foo'}}

    result = factory.resolve(context)
    assert result is not None
    assert result.container_type == 'group'
    assert result.id == 'foo'
    assert result.exists

def test_resolve_missing_level():
    resolver = MockContainerResolver()
    factory = ContainerFactory(resolver)

    context = {
        'group': {'_id': 'scitran'},
        'project': {'label': 'Project1'},
        'subject': {'label': 'Subject1'},
        'acquisition': {'label': 'Acquisition1'}
    }

    result = factory.resolve(context)
    assert result is None

def test_resolve_acquisition():
    resolver = MockContainerResolver({
        'scitran': 'scitran',
        'scitran/Project1': 'project1',
        'scitran/Project1/Subject1/Session1/Acquisition1': 'NOT_EXIST'
    })

    factory = ContainerFactory(resolver)

    group_context = {
        'group': {'_id': 'scitran'},
    }
    
    project_context = {
        'group': {'_id': 'scitran'},
        'project': {'label': 'Project1'},
    }

    acquisition_context = {
        'group': {'_id': 'scitran'},
        'project': {'label': 'Project1'},
        'subject': {'label': 'Subject1'},
        'session': {'label': 'Session1'},
        'acquisition': {'label': 'Acquisition1'}
    }

    result = factory.resolve(acquisition_context)
    assert result is not None
    assert result.container_type == 'acquisition'
    assert result.id is None
    assert result.label == 'Acquisition1'
    assert not result.exists

    result = factory.resolve(group_context)
    assert result is not None
    assert result.container_type == 'group'
    assert result.id is 'scitran' 
    assert result.exists
    
    result = factory.resolve(project_context)
    assert result is not None
    assert result.container_type == 'project'
    assert result.id is 'project1' 
    assert result.label == 'Project1'
    assert result.exists


def test_creation():
    resolver = MockContainerResolver({
        'scitran': 'scitran',
        'scitran/Project1': 'project1',
    })

    factory = ContainerFactory(resolver)

    context = {
        'group': {'_id': 'scitran'},
        'project': {'label': 'Project1'},
        'subject': {'label': 'Subject1'},
        'session': {'label': 'Session1'},
        'acquisition': {'label': 'Acquisition1'}
    }

    factory.resolve(context)

    nodes_to_create = []
    def create_fn(parent, child):
        nodes_to_create.append((parent, child))
        return 'created_' + child.label.lower()

    factory.create_containers()

    # Should be in hierarchical order for a single tree
    itr = iter(resolver.created_nodes)
    
    parent, child = next(itr)
    assert parent.container_type == 'project'
    assert parent.id == 'project1'
    assert child.container_type == 'subject'
    assert child.id == 'created_subject1'
    assert child.exists

    parent, child = next(itr)
    assert parent.container_type == 'subject'
    assert parent.id == 'created_subject1'
    assert child.container_type == 'session'
    assert child.id == 'created_session1'
    assert child.exists

    parent, child = next(itr)
    assert parent.container_type == 'session'
    assert parent.id == 'created_session1'
    assert child.container_type == 'acquisition'
    assert child.id == 'created_acquisition1'
    assert child.exists

    try:
        next(itr)
        pytest.fail('Unexpected invocation to create node')
    except StopIteration:
        pass


