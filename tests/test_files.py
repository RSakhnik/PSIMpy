import errno
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from bim_bridge._files import publish


class FileTests(unittest.TestCase):
    def test_drive_fallback_and_existing_file(self):
        with tempfile.TemporaryDirectory() as folder:
            temp, dest = Path(folder) / 'temp', Path(folder) / 'output'
            temp.write_bytes(b'payload')
            with patch('bim_bridge._files.os.link', side_effect=OSError(errno.ENOTSUP, 'unsupported')):
                publish(temp, dest)
                self.assertEqual(dest.read_bytes(), b'payload')
                self.assertFalse(temp.exists())
                temp.write_bytes(b'new')
                with self.assertRaises(FileExistsError):
                    publish(temp, dest)
                self.assertEqual(dest.read_bytes(), b'payload')

    def test_drive_interruption_removes_partial(self):
        with tempfile.TemporaryDirectory() as folder:
            temp, dest = Path(folder) / 'temp', Path(folder) / 'output'
            temp.write_bytes(b'payload')
            with patch('bim_bridge._files.os.link', side_effect=OSError(errno.ENOTSUP, 'unsupported')):
                with patch('bim_bridge._files.shutil.copyfileobj', side_effect=KeyboardInterrupt):
                    with self.assertRaises(KeyboardInterrupt):
                        publish(temp, dest)
            self.assertFalse(dest.exists())
            self.assertTrue(temp.exists())
