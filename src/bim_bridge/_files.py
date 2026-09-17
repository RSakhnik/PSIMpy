"""Публикация файла без перезаписи, включая диски без поддержки hard link."""
import errno
import os
import shutil


def publish(temp, destination, overwrite=False):
    if overwrite:
        os.replace(temp, destination)
        return
    try:
        os.link(temp, destination)
    except OSError as exc:
        if exc.errno not in {errno.EXDEV, errno.EPERM, errno.EACCES,
                             errno.ENOSYS, errno.ENOTSUP}:
            raise
        # Google Drive/FUSE может не поддерживать hard link. Режим xb сохраняет
        # чужой существующий файл. Копирование на таком диске не атомарно.
        created = False
        try:
            with open(destination, 'xb') as target:
                created = True
                with open(temp, 'rb') as source:
                    shutil.copyfileobj(source, target, 1024 * 1024)
        except BaseException:
            if created:
                os.unlink(destination)
            raise
    os.unlink(temp)
