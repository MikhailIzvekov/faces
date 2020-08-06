import os
from os.path import join
import numpy as np
import base64
import face_recognition
from elastic import Face
from PIL import Image


def extract_faces(file_name, image, output_folder, named):
    image_np = np.array(image)
    locations = face_recognition.face_locations(image_np)
    encodings = face_recognition.face_encodings(image_np, locations)
    face_count = 0
    for location, features in zip(locations, encodings):
        face = Face()
        face.file_name = file_name
        face.features = base64.b64encode(features).decode()
        face.position.top = location[0]
        face.position.right = location[1]
        face.position.bottom = location[2]
        face.position.left = location[3]
        if named:
            face.person = os.path.splitext(os.path.basename(file_name))[0]
        face.save()
        face_count += 1

        if output_folder:
            face_image = image_np[location[0]:location[2], location[3]:location[1]]
            pil_image = Image.fromarray(face_image)
            pil_image.save(join(output_folder, face.meta.id + '.jpg'), "JPEG", quality=90, optimize=True,
                           progressive=True)
    return face_count
