from __future__ import print_function
import requests
import json
import pprint as pp

r = requests.get("http://192.168.1.45:8529/_api/version?details=true")
pp.pprint(json.loads(r.text))
