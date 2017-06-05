import requests, zipfile
import StringIO
import pandas as pd
import os
import pdb

def get_data(name, url):
    """ Download and cache Citibike data
    """
    out_path = 'tripdata/'
    csv_path = out_path + name[:-3] + 'csv'
    if os.path.exists(csv_path):
        print "\t{} already downloaded".format(csv_path)
    else:
        # request zipfile and extract
        r = requests.get(url, timeout=5)
        z = zipfile.ZipFile(StringIO.StringIO(r.content))
        orig_name = z.namelist()[0]
        z.extract(orig_name, out_path)
        z.close()

        #rename extracted file
        os.rename(out_path + orig_name, csv_path)
        print '\tzip file removed'
        print '\t{} saved'.format(csv_path)


