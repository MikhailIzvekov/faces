import requests

from elastic import DetectedObject


def extract_objects(file_name):
    url = 'http://localhost:5001/detect/'
    files = {'file': open(file_name, 'rb')}
    r = requests.post(url, files=files)
    result = set()
    for item in r.json():
        for k, v in item.items():
            obj = DetectedObject()
            obj.type = k
            obj.position.x = v["x"]
            obj.position.y = v["y"]
            obj.position.w = v["w"]
            obj.position.h = v["h"]
            obj.save()
            result.add(k)

    return list(result)
