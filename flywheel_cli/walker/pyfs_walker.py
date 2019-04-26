"""Abstract file-system walker class"""
import fs
from ..custom_walker import CustomWalker
from .abstract_walker import AbstractWalker, FileInfo

class PyFsWalker(AbstractWalker):
    """Walker that is implemented in terms of PyFs"""
    def __init__(self, fs_url, ignore_dot_files=True, follow_symlinks=False, filter=None, exclude=None,
            filter_dirs=None, exclude_dirs=None, src_fs=None):
        """Initialize the abstract walker

        Args:
            root (str): The starting directory for walking
            ignore_dot_files (bool): Whether or not to ignore files starting with '.'
            follow_symlinks(bool): Whether or not to follow symlinks
            filter (list): An optional list of filename patterns to INCLUDE
            exclude (list): An optional list of filename patterns to EXCLUDE
            filter_dirs (list): An optional list of directories to INCLUDE
            exclude_dirs (list): An optional list of patterns of directories to EXCLUDE
            delim (str): The path delimiter, if not '/'
            src_fs (fs): The fs instance or None
        """
        super(PyFsWalker, self).__init__('/', ignore_dot_files=ignore_dot_files,
                follow_symlinks=follow_symlinks, filter=filter, exclude=exclude,
                filter_dirs=filter_dirs, exclude_dirs=exclude_dirs)

        self.fs_url = fs_url
        self.src_fs = src_fs or fs.open_fs(fs_url)

    def _listdir(self, path):
        for info in self.src_fs.scandir(path, namespaces=['basic', 'details', 'link']):
            result = FileInfo(info.name, info.is_dir)

            if info.has_namespace('link'):
                result.is_link = info.target is not None

            if info.has_namespace('details'):
                result.created = info.created
                result.modified = info.modified
                result.size = info.size

            yield result

    def open(self, path, mode='rb', **kwargs):
        try:
            return self.src_fs.open(path, mode, **kwargs)
        except fs.errors.ResourceNotFound as ex:
            raise FileNotFoundError('File {} not found'.format(path))

    def close(self):
        self.src_fs.close()

    def get_fs_url(self):
        return self.fs_url

if __name__ == '__main__':
    import sys

    walker = PyFsWalker('osfs://{}'.format(sys.argv[1]))
    for root, subdirs, files in walker.walk():
        print('root: {}'.format(root))
        for subdir in subdirs:
            print('  subdir: {}'.format(subdir))
        for filename in files:
            print('  file: {}'.format(filename))
