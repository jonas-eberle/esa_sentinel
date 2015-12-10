#!python
import sentinel_api as api

# use username and password for ESA DATA Hub authentication
username = '****YOUR_ESA_DATA_HUB_USERNAME****'
password = '****YOUR_ESA_DATA_HUB_PASSWORD****'

# please also specify the Hub URL:
# All Sentinel-1 and -2 scenes beginning from 15th Nov. 2015: https://scihub.esa.int/apihub/
# All historic Sentinel-1 scenes: https://scihub.esa.int/dhus/
s2 = api.SentinelDownloader(username, password, api_url='https://scihub.esa.int/apihub/')

# set directory for
# - filter scenes list with existing files
# - set directory path for data download
s2.set_download_dir('./')

# load geometries from shapefile
s2.load_sites('wetlands_v8.shp')

# search for scenes with some restrictions (minimum overlap: 1%)
s2.search('S2A*', min_overlap=0.01)

# you can either write results to a bash file for wget or download files directly in this script
# s2.write_results('wget', 'sentinel_api_s2_download.sh')
s2.download_all()