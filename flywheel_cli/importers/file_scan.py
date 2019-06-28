"""Provides a scanner that will produce context from filenames"""
import copy
import logging
import os

from . import match_util
from .abstract_scanner import AbstractScanner

log = logging.getLogger(__name__)

class FilenameScanner(AbstractScanner):
    """FilenameScanner groups files together by a common prefix.

    This works by looking at the first slash (or if there is no slash, the first dot) in
    each file path, and using that as the acquisition label.
    """
    def __init__(self, config, pattern=None):
        """Class that handles filename attribute extraction"""
        super(FilenameScanner, self).__init__(config)
        self.template = match_util.compile_regex(pattern)

    @staticmethod
    def validate_opts(opts):
        if opts is None or 'pattern' not in opts:
            raise ValueError('Filename scanner requires pattern!')

        try:
            match_util.compile_regex(opts['pattern'])
        except Exception as ex:
            log.debug('Cannot compile filename pattern.', exc_info=True)
            raise ValueError(f'Invalid filename pattern: {ex}')

    def discover(self, walker, context, container_factory, path_prefix=None, audit_log=None):
        # Discover files first
        files = list(sorted(walker.files(subdir=path_prefix)))

        prefix_len = len(path_prefix or '')

        current_prefix = None
        current_files = []

        for path in files:
            path = path.lstrip('/')

            filename = os.path.basename(path)
            file_context = copy.deepcopy(context)
            if not match_util.extract_metadata_attributes(filename, self.template, file_context):
                log.debug(f'File {filename} did not match the template')

            container = container_factory.resolve(file_context)
            if container is not None:
                container.files.append(path)
            else:
                self.messages.append(('warn', 'Ignoring file {} because it represents an ambiguous node'.format(path)))
