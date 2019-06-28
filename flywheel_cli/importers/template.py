import copy
import re

from abc import ABC, abstractmethod
from typing.re import Pattern

from . import match_util
from ..util import KeyWithOptions

from .dicom_scan import DicomScanner
from .file_scan import FilenameScanner
from .parrec_scan import ParRecScanner
from .slurp_scan import SlurpScanner

SCANNER_CLASSES = {
    'dicom': DicomScanner,
    'parrec': ParRecScanner,
    'slurp': SlurpScanner,
    'filename': FilenameScanner
}

class ImportTemplateNode(ABC):
    """The node type, either folder or scanner"""
    ignore = False
    node_type = 'folder'

    def extract_metadata(self, name, context, walker=None, path=None):
        """Extract metadata from a folder-level node

        Args:
            name (str): The current folder name
            context (dict): The context object to update
            walker (fs): The parent walker object, if available
            path (str): The full path to the folder

        Returns:
            ImportTemplateNode: The next node in the tree if match succeeded, otherwise None
        """
        return None

    def scan(self, src_fs, path_prefix, context, container_factory):
        """Scan directory contents, rather than walking.

        Called if this is a scanner node.

        Args:
            src_fs (fs): The filesystem to scan
            path_prefix (str): The path prefix
            context (dict): The current context object
            container_factory: The container factory where nodes should be added

        Returns:
            list: The list of warning/error messages
        """
        return []

class TerminalNode(ImportTemplateNode):
    """Terminal node"""
    def extract_metadata(self, name, context, walker=None, path=None):
        return None

    def __repr__(self):
        return 'TerminalNode'

TERMINAL_NODE = TerminalNode()

class StringMatchNode(ImportTemplateNode):
    def __init__(self, template=None, packfile_type=None, metadata_fn=None, packfile_name=None, ignore=False):
        """Create a new container-level node.

        Args:
            template (str|Pattern): The metavar or regular expression
            packfile_type (str): The optional packfile type if this is a packfile folder
            metadata_fn (function): Optional function to extract additional metadata
            packfile_name (str): The optional packfile name, if not using the default
            ignore (bool): Whether or not to ignore this node
        """
        self.template = template
        self.next_node = TERMINAL_NODE
        self.packfile_type = packfile_type
        self.metadata_fn = metadata_fn
        self.packfile_name = packfile_name
        self.ignore = ignore

    def set_next(self, next_node):
        """Set the next node"""
        self.next_node = next_node

    def extract_metadata(self, name, context, walker=None, path=None):
        if self.ignore:
            context['ignore'] = True
            return None

        if not match_util.extract_metadata_attributes(name, self.template, context):
            return None

        if callable(self.metadata_fn):
            self.metadata_fn(name, context, walker, path=path)

        if self.packfile_type:
            context['packfile'] = self.packfile_type
            context['packfile_name'] = self.packfile_name
            return TERMINAL_NODE

        return self.next_node

    def __repr__(self):
        if isinstance(self.template, Pattern):
            tmpl = self.template.pattern
        else:
            tmpl = self.template
        return 'StringMatchNode({}, packfile_type={}, ignore={})'.format(
            tmpl, self.packfile_type, self.ignore)

class CompositeNode(ImportTemplateNode):
    def __init__(self, children=None):
        """Returns the first node that matches out of children."""
        if children:
            self.children = copy.copy(children)
        else:
            self.children = []

    def add_child(self, child):
        """Add a child to the composite node

        Args:
            child (ImportTemplateNode): The child to add
        """
        self.children.append(child)

    def extract_metadata(self, name, context, walker=None, path=None):
        for child in self.children:
            next_node = child.extract_metadata(name, context, walker, path=path)
            if next_node:
                return next_node
        return None

    def __repr__(self):
        result = 'CompositeNode([\n'
        for child in self.children:
            result += '  {}\n'.format(child)
        return result + '])'

class ScannerNode(ImportTemplateNode):
    node_type = 'scanner'

    def __init__(self, config, scanner_cls, opts=None):
        self.config = config
        self.scanner_cls = scanner_cls
        self.opts = opts or {}

    def set_next(self, next_node):
        """Set the next node"""
        raise ValueError('Cannot declare nodes after dicom scanner!')

    def scan(self, src_fs, path_prefix, context, container_factory, audit_log):
        """Scan directory contents, rather than walking.

        Called if this is a scanner node.

        Args:
            src_fs (fs): The filesystem to scan
            path_prefix (str): The path prefix
            context (dict): The current context object
            container_factory: The container factory where nodes should be added
            audit_log: The audit log instance

        Returns:
            list: The list of warning/error messages
        """
        scanner = self.scanner_cls(self.config, **self.opts)
        scanner.discover(src_fs, context, container_factory,
                path_prefix=path_prefix, audit_log=audit_log)
        return scanner.messages

    def __repr__(self):
        return 'ScannerNode(scanner={})'.format(type(self.scanner_cls))


def parse_list_item(item, last=None, config=None):
    if 'select' in item:
        # Composite node
        children = [parse_list_item(child, config=config) for child in item['select']]
        node = CompositeNode(children)
    else:
        # Ensure dict, allows shorthand in config file
        item = KeyWithOptions(item, key='pattern')

        # Otherwise, expect a pattern
        match = match_util.compile_regex(item.key)

        #Create the next node
        scan = item.config.pop('scan', None)
        node = StringMatchNode(template=match, **item.config)

        # Add scanner node
        if scan:
            next_node = _create_scanner_node(scan, config)
            node.set_next(next_node)

    if last is not None:
        last.set_next(node)

    return node


def parse_template_list(value, config=None):
    """Parses a template list, creating an ImportTemplateNode tree.

    Args:
        value (list): The list of template values

    Returns:
        The created ImportTemplateNode tree
    """
    root = None
    last = None

    for item in value:
        last = parse_list_item(item, last, config)
        if root is None:
            root = last

    return root


def parse_template_string(value, config=None):
    """Parses a template string, creating an ImportTemplateNode tree.

    Args:
        value (str): The template string

    Returns:
        The created ImportTemplateNode tree
    """
    root = None
    last = None
    sections = re.split(r'(?<!\\):', value)
    for section in sections:
        parts = re.split(r'(?<!\\),', section, maxsplit=1)
        if len(parts) == 1:
            match = parts[0]
            optstr = ''
        else:
            match, optstr = parts

        # Compile the match string into a regular expression
        match = match_util.compile_regex(match)

        # Parse the options
        opts = _parse_optstr(optstr)
        scan = opts.pop('scan', None)

        # Create the next node
        node = StringMatchNode(template=match, **opts)
        if root is None:
            root = last = node
        else:
            last.set_next(node)
            last = node

        # Add scanner node
        if scan:
            node = _create_scanner_node(scan, config)
            last.set_next(node)
            last = node

    return root


def _create_scanner_node(scan, config):
    scan = KeyWithOptions(scan)

    scanner_cls = SCANNER_CLASSES.get(scan.key)
    if not scanner_cls:
        raise ValueError(f'Unknown scanner class: {scan.key}')

    # Validate opts
    scanner_cls.validate_opts(scan.config)
    return ScannerNode(config, scanner_cls, opts=scan.config)


def _parse_optstr(val):
    result = {}

    pairs = val.split(',')
    for pair in pairs:
        pair = pair.strip()
        if pair:
            key, _, value = pair.partition('=')
            result[key.strip()] = value.strip()

    return result
