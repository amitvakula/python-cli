import copy
import gzip
import itertools
import logging
import os
import sys

from .abstract_importer import AbstractImporter
from .abstract_scanner import AbstractScanner
from .packfile import PackfileDescriptor
from .. import util

from flywheel_migration.dcm import DicomFileError, DicomFile

from pydicom.datadict import tag_for_keyword
from pydicom.tag import Tag

log = logging.getLogger(__name__)

class DicomSession(object):
    def __init__(self, context):
        """Helper class that holds session properties and acquisitions"""
        self.context = context
        self.acquisitions = {}
        self.secondary_acquisitions = {}  # Acquisitions that we don't have all
                                          # of the info for yet
class DicomAcquisition(object):
    def __init__(self, context):
        """Helper class that holds acquisition properties and files"""
        self.context = context
        self.files = {}  # Map of primary_series_uids to maps of series uids to filepaths
                         # So that the primary series uid can be used to group multiple dicom series into one acquisition
        self.filenames = {}  # A map of series uid to filenames

# Specifying just the list of tags we're interested in speeds up dicom scanning
DICOM_TAGS = [
    'Manufacturer',
    'AcquisitionNumber',
    'AcquisitionDate',
    'AcquisitionTime',
    'SeriesDate',
    'SeriesTime',
    'SeriesInstanceUID',
    'ImageType',
    'StudyDate',
    'StudyTime',
    'StudyInstanceUID',
    'OperatorsName',
    'PatientName',
    'PatientID',
    'StudyID',
    'SeriesDescription',
    'PatientBirthDate',
    'SOPInstanceUID'
]

def _at_stack_id(related_acquisitions):
    if related_acquisitions:
        stop_tag = (0x3006, 0x0011)
    else:
        stop_tag = (0x0020, 0x9056)
    def f(tag, VR, length):
        return tag == stop_tag
    return f

class DicomScanner(AbstractScanner):
    # The session label dicom header key
    session_label_key = 'StudyDescription'

    def __init__(self, config):
        """Class that handles generic dicom import"""
        super(DicomScanner, self).__init__(config)

        if config:
            self.deid_profile = config.deid_profile
            self.related_acquisitions = config.related_acquisitions
        else:
            self.deid_profile = None
            self.related_acquisitions = False

        self.profile = None  # Dicom file profile
        self.subject_map = None  # Provides subject mapping services
        if self.deid_profile:
            self.subject_map = self.deid_profile.map_subjects
            self.profile = self.deid_profile.get_file_profile('dicom')

        self.sessions = {}

    def save_subject_map(self):
        if self.subject_map:
            self.subject_map.save()

    def discover(self, walker, context, container_factory, path_prefix=None, audit_log=None):
        tags = [ Tag(tag_for_keyword(keyword)) for keyword in DICOM_TAGS ]

        # If we're mapping subject fields to id, then include those fields in the scan
        if self.subject_map:
            subject_cfg = self.subject_map.get_config()
            tags += [ Tag(tag_for_keyword(keyword)) for keyword in subject_cfg.fields ]
        if self.related_acquisitions:
            tags += [ Tag(tag_for_keyword('ReferencedFrameOfReferenceSequence')) ]

        # First step is to walk and sort files
        sys.stdout.write('Scanning directories...'.ljust(80) + '\r')
        sys.stdout.flush()

        # Discover files first
        files = list(walker.files(subdir=path_prefix))
        file_count = len(files)
        files_scanned = 0

        for path in files:
            sys.stdout.write('Scanning {}/{} files...'.format(files_scanned, file_count).ljust(80) + '\r')
            sys.stdout.flush()
            files_scanned = files_scanned+1

            try:
                full_path = path_prefix + path if path_prefix else path
                with walker.open(path, 'rb', buffering=self.config.buffer_size) as f:
                    # Unzip gzipped files
                    _, ext = os.path.splitext(path)
                    if ext.lower() == '.gz':
                        f = gzip.GzipFile(fileobj=f)

                    # Don't decode while scanning, stop as early as possible
                    # TODO: will we ever rely on fields after stack id for subject mapping
                    dcm = DicomFile(f, parse=False, session_label_key=self.session_label_key,
                        decode=self.related_acquisitions, stop_when=_at_stack_id(self.related_acquisitions), update_in_place=False, specific_tags=tags)
                    acquisition = self.resolve_acquisition(context, dcm)

                    sop_uid = self.get_value(dcm, 'SOPInstanceUID', required=True)
                    series_uid = self.get_value(dcm, 'SeriesInstanceUID', required=True)
                    if sop_uid in acquisition.files.setdefault(series_uid, {}):
                        orig_path = acquisition.files[series_uid][sop_uid]

                        if not util.files_equal(walker, full_path, orig_path):
                            message = ('DICOM conflicts with {}! Both files have the '
                                'same IDs, but contents differ!').format(orig_path)
                            self.report_file_error(audit_log, full_path, msg=message)
                    else:
                        acquisition.files[series_uid][sop_uid] = path

                    # Add a filename for that series uid
                    if series_uid not in acquisition.filenames:
                        acquisition_timestamp = self.determine_acquisition_timestamp(dcm)
                        series_label = self.determine_acquisition_label(acquisition.context,
                            dcm, series_uid, timestamp=acquisition_timestamp)
                        filename = DicomScanner.determine_dicom_zipname(acquisition.filenames, series_label)
                        acquisition.filenames[series_uid] = filename

            except DicomFileError as exc:
                if util.is_dicom_file(path):
                    self.report_file_error(audit_log, full_path, exc=exc, msg='Not a DICOM - {}'.format(exc))
                else:
                    log.debug('Ignoring non-DICOM file: %s', full_path)
            except Exception as exc:
                self.report_file_error(audit_log, full_path, exc=exc)

        sys.stdout.write(''.ljust(80) + '\n')
        sys.stdout.flush()

        # Create context objects
        for session in self.sessions.values():
            session_context = copy.deepcopy(context)
            session_context.update(session.context)

            for acquisition in itertools.chain(session.acquisitions.values(), session.secondary_acquisitions.values()):
                acquisition_context = copy.deepcopy(session_context)
                acquisition_context.update(acquisition.context)
                for series_uid, files in acquisition.files.items():

                    files = list(files.values())
                    filename = acquisition.filenames.get(series_uid)

                    container = container_factory.resolve(acquisition_context)
                    container.packfiles.append(PackfileDescriptor('dicom', files, len(files), filename))

    @staticmethod
    def determine_dicom_zipname(filenames, series_label):
        """Return a filename for the dicom series that is unique to a container

        Args:
            filenames (dict): A map of series uids to filenames
            series_label (str): The base to use for the filename

        Returns:
            str: The filename for the zipped up series
        """
        filename = series_label + '.dicom.zip'
        duplicate = 1
        while filename in filenames.values():
            filename = series_label + '_dup-{}.dicom.zip'.format(duplicate)
        return filename

    def resolve_session(self, context, dcm):
        """Find or create a sesson from a dcm file. """
        session_uid = self.get_value(dcm, 'StudyInstanceUID', required=True)
        if session_uid not in self.sessions:
            subject_label = context.get('subject', {}).get('label')
            # Map subject
            if subject_label:
                subject_code = subject_label
            elif self.subject_map:
                subject_code = self.subject_map.get_code(dcm)
            else:
                subject_code = self.get_value(dcm, 'PatientID', '')

            session_timestamp = self.get_timestamp(dcm, 'StudyDate', 'StudyTime')

            # Create session
            self.sessions[session_uid] = DicomSession({
                'session': {
                    'uid': session_uid.replace('.', ''),
                    'label': self.determine_session_label(context, dcm, session_uid, timestamp=session_timestamp),
                    'timestamp': session_timestamp,
                    'timezone': str(util.DEFAULT_TZ)
                },
                'subject': {
                    'label': subject_code
                }
            })

        return self.sessions[session_uid]

    def resolve_acquisition(self, context, dcm):
        """Find or create an acquisition from a dcm file. """
        session = self.resolve_session(context, dcm)
        series_uid = self.get_value(dcm, 'SeriesInstanceUID', required=True)
        primary_acquisition_file = True
        if self.related_acquisitions and dcm.get('ReferencedFrameOfReferenceSequence'):
            # We need to add it to the acquisition of the primary series uid
            try:
                series_uid = dcm.get(
                    'ReferencedFrameOfReferenceSequence'
                )[0].get(
                    'RTReferencedStudySequence'
                )[0].get(
                    'RTReferencedSeriesSequence'
                )[0].get(
                    'SeriesInstanceUID'
                )
                primary_acquisition_file = False
            except (TypeError, IndexError, AttributeError) as e:
                log.warning('Unable to find related series for file {}. Uploading into its own acquisition')


        if series_uid not in session.acquisitions:
            # full acquisition doesn't exists
            if not primary_acquisition_file and series_uid in session.secondary_acquisitions:
                # The secondary acquisition exists
                return session.secondary_acquisitions[series_uid]

            acquisition_timestamp = self.determine_acquisition_timestamp(dcm)
            acquisition = DicomAcquisition({
                'acquisition': {
                    'uid': series_uid.replace('.', ''),
                    'label': self.determine_acquisition_label(context, dcm, series_uid, timestamp=acquisition_timestamp),
                    'timestamp': acquisition_timestamp,
                    'timezone': str(util.DEFAULT_TZ)
                }
            })

            if primary_acquisition_file:
                # Check for a secondary and add it the files and filenames to the primary
                if series_uid in session.secondary_acquisitions:
                    acquisition.files = session.secondary_acquisitions.get(series_uid).files
                    acquisition.filenames = session.secondary_acquisitions.pop(series_uid).filenames

                session.acquisitions[series_uid] = acquisition
                return session.acquisitions[series_uid]
            else:
                session.secondary_acquisitions[series_uid] = acquisition
                return session.secondary_acquisitions[series_uid]

        else:
            # Acquisition already exists
            return session.acquisitions[series_uid]

    def determine_session_label(self, context, _dcm, uid, timestamp=None):
        """Determine session label from DICOM headers"""
        session_label = context.get('session', {}).get('label')
        if session_label:
            return session_label

        if timestamp:
            return timestamp.strftime('%Y-%m-%d %H:%M:%S')

        return uid

    def determine_acquisition_label(self, context, dcm, uid, timestamp=None):
        """Determine acquisition label from DICOM headers"""
        acquisition_label = context.get('acquisition', {}).get('label')
        if acquisition_label:
            return acquisition_label

        name = self.get_value(dcm, 'SeriesDescription')
        if not name and timestamp:
            name = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        if not name:
            name = uid
        return name

    def determine_acquisition_timestamp(self, dcm):
        # Create the acquisition because the acqusition doesn't exist
        # Get acquisition timestamp (based on manufacturer)
        if dcm.get_manufacturer() == 'SIEMENS':
            timestamp = self.get_timestamp(dcm, 'SeriesDate', 'SeriesTime')
        else:
            timestamp = self.get_timestamp(dcm, 'AcquisitionDate', 'AcquisitionTime')
        return timestamp

    def get_timestamp(self, dcm, date_key, time_key):
        """Get a timestamp value"""
        date_value = self.get_value(dcm, date_key)
        time_value = self.get_value(dcm, time_key)

        return DicomFile.timestamp(date_value, time_value, util.DEFAULT_TZ)

    def get_value(self, dcm, key, default=None, required=False):
        """Get a transformed value"""
        if self.profile:
            result = self.profile.get_value(None, dcm.raw, key)
            if not result:
                result = default
        else:
            result = dcm.get(key, default)

        if result is None and required:
            raise ValueError('DICOM is missing {}'.format(key))

        return result


class DicomScannerImporter(AbstractImporter):
    # Archive filesystems are not supported, because zipfiles are not seekable
    support_archive_fs = False

    # Subject mapping is supported
    support_subject_mapping = True

    def __init__(self, group, project, config, context=None, subject_label=None, session_label=None):
        """Class that handles state for dicom scanning import.

        Arguments:
            group (str): The optional group id
            project (str): The optional project label or id in the format <id:xyz>
            config (Config): The config object
        """
        super(DicomScannerImporter, self).__init__(group, project, False, context, config)

        # Initialize the scanner
        self.scanner = DicomScanner(config)

        self.subject_label = subject_label
        self.session_label = session_label

    def before_begin_upload(self):
        # Save subject map
        self.scanner.save_subject_map()

    def initial_context(self):
        """Creates the initial context for folder import.

        Returns:
            dict: The initial context
        """
        context = super(DicomScannerImporter, self).initial_context()

        if self.subject_label:
            util.set_nested_attr(context, 'subject.label', self.subject_label)

        if self.session_label:
            util.set_nested_attr(context, 'session.label', self.session_label)

        return context

    def perform_discover(self, walker, context):
        """Performs discovery of containers to create and files to upload in the given folder.

        Arguments:
            walker (AbstractWalker): The filesystem to query
            context (dict): The initial context
        """
        self.scanner.discover(walker, context, self.container_factory, audit_log=self.audit_log)
        self.messages += self.scanner.messages
