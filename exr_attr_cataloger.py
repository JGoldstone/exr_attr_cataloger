
import os
import subprocess
import re
from pathlib import Path
import sqlite3

import fileseq
from OpenImageIO import ImageInput, TypeDesc, TypeInt, TypeRational, TypeFloat, TypeVector2, TypeString

#
# Things for Larry, from the docs:
#  Is it the case that:
#    dataWindow -> x, y, z, width, height, depth (abd BTW, "upper left corner" -> "upper left front corner"?)
#    displayWindow -> full_x, full_y, full_z, full_width, full_height, full_depth
#    originalDataWindow would require original_x, original_y, original_z?
#
TRIVIALLY_IGNORABLE_DIRS = ['.AppleDB', '.AppleDesktop', 'Network Trash Folder', 'Temporary Items', '.apdisk]']

REQUIRED_ATTRIBUTE_NAMES = (
    # ImageSpec.full_x: int, full_y, full_z, full_width, full_height, full_depth
    # TODO Ask Larry if docu's 'upper left corner' should be 'upper left front corner'
    # TODO Mention to Larry that builtinplugins.rst doesn't mention full_z or z
    'displayWindow',
    # ImageSpec.x: int, y, z, width, height, depth
    'dataWindow',
    # TODO Are pixelAspectRatio, screenWindowCenter, screenWindowWidth, lineOrder, compression 'vanilla' attrs
    'pixelAspectRatio',
    'screenWindowCenter',
    'screenWindowWidth',
    # TODO: Did randomY show up 'recently'? It's not in SMPTE ST 2065-4:2013 ('ACES Container File Layout')
    'lineOrder',
    'compression',
    # ImageSpec.nchannels
    # ImageSpec.format with ImageSpec.channelformats being None, or
    # ImageSpec.channelformats as a tuple
    # TODO Ask Larry if in the latter case, ImageSpec.format is None
    'channels'
)

CANONICAL_STANDARD_ATTRIBUTE_TYPES = {
    # displayWindow, dataWindow, channels split up: see comments above on REQUIRED_ATTRIBUTE_NAMES
    'pixelAspectRatio': TypeFloat,
    'screenWindowCenter': TypeVector2,
    'screenWindowWidth': TypeFloat,
    'lineOrder': TypeString,
    'compression': TypeString
}

TEST_DIR = '/Users/jgoldstone/tfe/experiments/amc/202012xx_compare_AMB_use/colorfront/'

# map OpenEXR types to OIIO


class ExrAttrCataloger(object):
    def __init__(self):
        self._canonical_name = None
        self.canonical_name = {}
        self.connection = sqlite3.connect('exr_attrs.sqlite')
        self.cursor = self.connection.cursor()
        self._volume_dir_for_root = None
        self.volume_dir_for_root = ExrAttrCataloger.find_volume_dir_for_root()
        self.create_attr_table_if_nonexistent()
        self.create_int_values_table_if_nonexistent()
        self.create_rational_values_table_if_nonexistent()
        self.create_real_values_table_if_nonexistent()
        self.create_string_values_table_if_nonexistent()
        self.create_chromaticity_values_table_if_nonexistent()\

    @staticmethod
    def find_volume_dir_for_root():
        # TODO replace this with something that works on all systems, not just macOS >= Mojave
        for line in subprocess.run(['system_profiler', 'SPSoftwareDataType'],
                                   capture_output=True, text=True).stdout.split('\n'):
            # Boot Volume: Pikachu
            m = re.match(r".*Boot Volume: (\w+).*", line)
            if m:
                return m.group(1)
        raise RuntimeError("could not find root volume")

    @property
    def canonical_name(self):
        return self._canonical_name

    @canonical_name.setter
    def canonical_name(self, value):
        self._canonical_name = value

    def name_is_canonical(self, attrib_name):
        return False

    def type_for_canonical_name(self, attrib_name):
        attrib_type = CANONICAL_STANDARD_ATTRIBUTE_TYPES.get(attrib_name)
        if not attrib_type:
            raise ValueError(f"{attrib_name} is not the name of a ")
        return attrib_type

    @property
    def volume_dir_for_root(self):
        return self._volume_dir_for_root

    @volume_dir_for_root.setter
    def volume_dir_for_root(self, value):
        self._volume_dir_for_root = value

    def path_including_volume(self, path: Path):
        # TODO remove macOS-specific expectations of filesystem layout
        if path.parents[-2] == '/Volumes':
            return path  # there, that was easy
        if not path.is_absolute():
            return self.volume_dir_for_root.joinpath(path)
        return self.volume_dir_for_root.joinpath(path.relative_to('/'))

    def create_attr_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='attrs'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE attrs "
                                "(full_path text, name text, canonical_name text, type text, "
                                "aggregate text, vec_semantics text, count int)")

    def create_int_values_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='int_values'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE int_values "
                                "(full_path text, name text, quantity int)")

    def create_rational_values_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='rational_values'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE rational_values "
                                "(full_path text, name text, quantity_numerator int, quantity_denominator int)")

    def create_real_values_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='real_values'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE real_values "
                                "(full_path text, name text, quantity real)")

    def create_string_values_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='string_values'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE string_values "
                                "(full_path text, name text, string text)")

    def create_chromaticity_values_table_if_nonexistent(self):
        self.cursor.execute("SELECT count(name) from sqlite_master WHERE type='table' AND name='chromaticity_values'")
        if self.cursor.fetchone()[0] != 1:
            self.cursor.execute("CREATE TABLE chromaticity_values "
                                "(full_path text, name text, rx real, ry real, gx real, gy real, "
                                "bx real, by real, wx real, wy real)")

    def catalog_attribute(self, path, attrib):
        attr_values = {'name': attrib.name, 'canonical_name': self.canonical_name[attrib.name],
                       'base_type': attrib.type.basetype, 'aggregate': attrib.type.aggregate,
                       'vec_semantics': attrib.type.vecsemantics, 'arraylen': attrib.type.arraylen}
        self.cursor.execute("INSERT INTO attrs VALUES "
                            "(:name, :canonical_name, :base_type, :aggregate, :vecsemantics, :arraylen)",
                            attr_values)
        if attrib.name == 'chromaticities':
            # OpenImageIO returns chromaticities as an 8-tuple of float
            chromaticity_attr_values = {'full_path': path, 'name': attrib.name,
                                        'rx': attrib.value[0], 'ry': attrib.value[1],
                                        'gx': attrib.value[2], 'gy': attrib.value[2],
                                        'bx': attrib.value[4], 'by': attrib.value[5],
                                        'wx': attrib.value[6], 'wy': attrib.value[7]}
            self.cursor.execute("INSERT INTO chromaticity_values VALUES "
                                "(:full_path, :name, :rx, :ry, :gx, :gy, :bx, :by, :wx, :wy)",
                                chromaticity_attr_values)
        elif attrib.type == TypeInt:
            int_attr_values = {'full_path': path, 'name': attrib.name, 'quantity': attrib.value}
            self.cursor.execute("INSERT INTO int_values "
                                "(:full_path, :name, :quantity)",
                                int_attr_values)
        elif attrib.type == TypeRational:
            rational_attr_values = {'full_path': path, 'name': attrib.name,
                                    'quantity_numerator': attrib.value[0],
                                    'quantity_denominator': attrib.value[1]}
            self.cursor.execute("INSERT INTO rational_values "
                                "(:full_path, :name, :quantity_numerator, :quantity_denominator)",
                                rational_attr_values)
        elif attrib.type == TypeFloat:
            float_attr_values = {'full_path': path, 'name': attrib.name, 'quantity': attrib.value}
            self.cursor.execute("INSERT INTO real_values "
                                "(:full_path, :name, :quantity)",
                                float_attr_values)
        elif attrib.type == TypeString:
            string_attr_values = {'full_path': path, 'name': attrib.name, 'string': attrib.value}
            self.cursor.execute("INSERT INTO string_values "
                                "(:full_path, :name, :string",
                                string_attr_values)
        else:
            print(f"--> don't know how to insert attribute `{attrib.name}' of type `{attrib.type}'")

    def catalog_attributes_for_file(self, path: Path):
        print(f"--> {path}")
        input_ = ImageInput.open(str(path))
        if not input_:
            print(f"could not open file {path}")
        spec = input_.spec()
        if not spec:
            print(f"could not get image spec for {path}")
        for attrib in spec.extra_attribs:
            if attrib.name in REQUIRED_ATTRIBUTE_NAMES:
                continue
            if self.name_is_canonical(attrib.name):
                if attrib.type != self.type_for_canonical_name(attrib.name):
                    print(f"saw unexpected type {attrib.type} for canonical attribute `{attrib.name}'")
            # for now
            print(f"{attrib.name} (type {attrib.type})")
            # print(f"{attrib.name} (type {attrib.type}): {attrib.value}")

    def walk_ignoring_chaff(self, root_dir):
        for dir_, subdirs, files in os.walk(root_dir):
            for subdir in subdirs:
                if subdir.startswith('.') or subdir in TRIVIALLY_IGNORABLE_DIRS:
                    subdirs.remove(subdir)
            # print(f"dir is {dir_}, subdirs {subdirs}")
            seqs = fileseq.findSequencesOnDisk(dir_)
            for seq in seqs:
                first_frame_path = Path(seq[0])
                if first_frame_path.suffix == '.exr':
                    self.catalog_attributes_for_file(first_frame_path)


if __name__ == '__main__':
    cataloger = ExrAttrCataloger()
    cataloger.walk_ignoring_chaff(TEST_DIR)
