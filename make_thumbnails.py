import os
import pathlib
from os.path import join, splitdrive
import ntpath


from PIL import Image


def make_thumbnail(file_name, image, output_folder):
    target_width = 805
    width, height = image.size
    target_height = int(height * (1.0 * target_width / width))
    thumb = image.resize((target_width, target_height), Image.ANTIALIAS)

    thumbnail_name = join(output_folder, splitdrive(file_name)[1].strip("\\"))
    pathlib.Path(os.path.dirname(thumbnail_name)).mkdir(parents=True, exist_ok=True)
    thumb.save(thumbnail_name, "JPEG", quality=90, optimize=True, progressive=True)
    return ntpath.basename(file_name)
