ESA Sentinel Search & Download API
=====

With this API you can easily search and download scenes from the ESA Copernicus program using the ESA Sentinels Scientific Data Hub. It has been developed for data acquisition at Friedrich-Schiller-University Jena, Department for Earth Observation (www.eo.uni-jena.de) and is part of the EU-H2020 Satellite-based Wetland Observation Service (SWOS) project (www.swos-service.eu). 

Installation
-----------
*Needed Python libraries:*
* GDAL
* Shapely
* requests
* progressbar

On Windows, using PIP you can install GDAL and Shapely with the whl packages from Gohlke:
http://www.lfd.uci.edu/~gohlke/pythonlibs/

The API has been tested with Python2.7 under Unix and Windows 7.

Usage of API in Python
-----------
Please see example_s1.py, example_s1_setgeom.py, and example_s2.py with three examples. 

To load features/geometries you can use either the "load_sites" method given a geospatial file or the "set_geometries" method to directly set a geometry in the Well-known-Text (Wkt) format. 
 
Please note: Using the "set_geometries" method the projection has to be in Lat/Long, EPSG:4326. 

The "search" method can be used with further parameters according to the OpenSearch protocol from the ESA Data Hub (see "productType" and "sensoroperationalmode" as an example below): 
https://scihub.esa.int/twiki/do/view/SciHubUserGuide/3FullTextSearch#Search_Keywords

Example:

    import sentinel_api as api
    
    # use username and password for ESA DATA Hub authentication
    username = '****YOUR_ESA_DATA_HUB_USERNAME****'
    password = '****YOUR_ESA_DATA_HUB_PASSWORD****'
    
    # please also specify the Hub URL:
    # All Sentinel-1 and -2 scenes beginning from 15th Nov. 2015: https://scihub.esa.int/apihub/
    # All historic Sentinel-1 scenes: https://scihub.esa.int/dhus/
    s1 = api.SentinelDownloader(username, password, api_url='https://scihub.esa.int/apihub/')
    
    # set directory for
    # - filter scenes list with existing files
    # - set directory path for data download
    s1.set_download_dir('./')
    
    # load geometries from shapefile
    s1.load_sites('wetlands_v8.shp')
    
    # search for scenes with some restrictions
    s1.search('S1A*', max_overlap=0.01, start_date="2015-12-01", date_type="beginPosition", productType='GRD', sensoroperationalmode='IW')
    
    # you can either write results to a bash file for wget or download files directly in this script
    # s1.write_results('wget', 'sentinel_api_s1_download.sh')
    s1.download_all()

Help
-----------
For now, please see the docstrings for each method within the code or within python
 
    import sentinel_api as api
    help(api)