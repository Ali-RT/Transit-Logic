#!/usr/bin/env python3
"""Spin data collector

Data collector from Spin API
data columns: bike_id, is_disabled, is_reserved, last_updated, lat, lon, operator, vehicle_type
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.1'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

import requests
from datetime import datetime
from requests.models import Response


OPERATOR = "Spin"

while True:
    try:
        response: Response = requests.get("https://web.spin.pm/api/gbfs/v1/washington_dc/free_bike_status")
        if response.status_code == 200:
            dict_data = response.json()['data']['bikes']

            with open(OPERATOR+'.txt', 'a') as file:
                for d in dict_data:
                    d.update({"last_updated": int(datetime.utcnow().timestamp()), "operator": OPERATOR})
                    file.write(', '.join("{}".format(val) for (key, val) in sorted(d.items())) + '\n')
            file.close()

    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))
        continue

    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
        continue

    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
        continue

    except KeyboardInterrupt:
        print("Someone closed the program")
