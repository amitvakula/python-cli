import os
import pytest
import shutil
import tempfile

import fs

DATA_ROOT = os.path.join('tests', 'data')
DICOM_ROOT = os.path.join(DATA_ROOT, 'DICOM')

@pytest.fixture(scope='function')
def dicom_file():
    def get_dicom_file(folder, filename):
        fd, path = tempfile.mkstemp(suffix='.dcm')
        os.close(fd)

        src_path = os.path.join(DICOM_ROOT, folder, filename)
        shutil.copy(src_path, path)

        return path

    return get_dicom_file

@pytest.fixture(scope='function')
def dicom_data():
    def get_dicom_file_data(folder, filename):
        src_path = os.path.join(DICOM_ROOT, folder, filename)
        with open(src_path, 'rb') as f:
            data = f.read()

        return data

    return get_dicom_file_data

@pytest.fixture(scope='function')
def temp_fs():
    tempdirs = []

    def make_mock_fs(structure):
        tempdir = tempfile.TemporaryDirectory()
        tempdirs.append(tempdir)

        tmpfs_url = 'osfs://{}'.format(tempdir.name)
        tmpfs = fs.open_fs(tmpfs_url)

        for path, files in structure.items():
            with tmpfs.makedirs(path, recreate=True) as subdir:
                for name in files:
                    if isinstance(name, tuple):
                        name, content = name
                    else:
                        content = b'Hello World'

                    with subdir.open(name, 'wb') as f:
                        f.write(content)

        return tmpfs, tmpfs_url

    yield make_mock_fs

