"""
Sentinel Search & Download API
Authors: Jonas Eberle <jonas.eberle@uni-jena.de>, Felix Cremer <felix.cremer@uni-jena.de>, John Truckenbrodt <john.truckenbrodt@uni-jena.de>

Example usage: Please see the "main" function at the end of this file

TODO:
- Documentation
"""

###########################################################
# imports
###########################################################

import os
import re
import zlib
import sys
import requests

from osgeo import ogr

from spatialist.vector import Vector, wkt2vector, intersect

import json
import progressbar as pb
import zipfile as zf
from datetime import datetime, date

ogr.UseExceptions()


class SentinelDownloader(object):
    """Class to search and download for Sentinel data"""
    
    __esa_username = None
    __esa_password = None
    __esa_api_url = None
    
    __geometries = []
    __scenes = []
    __download_dir = './'
    __data_dirs = []
    
    def __init__(self, username, password, api_url='https://scihub.copernicus.eu/apihub/'):
        self.__esa_api_url = api_url
        self.__esa_username = username
        self.__esa_password = password
    
    def download_all(self, download_dir=None):
        """Download all scenes

        Args:
            download_dir: Define a directory where to download the scenes
                (Default: Use default from class -> current directory)

        Returns:
            Dictionary of failed ('failed') and successfully ('success') downloaded scenes

        """
        if download_dir is None:
            download_dir = self.__download_dir
        
        downloaded = []
        downloaded_failed = []
        
        for scene in self.__scenes:
            url = scene['url']
            filename = scene['title'] + '.zip'
            path = os.path.join(download_dir, filename)
            print('===========================================================')
            print('Download file path: %s' % path)
            
            try:
                response = requests.get(url, auth=(self.__esa_username, self.__esa_password), stream=True)
            except requests.exceptions.ConnectionError:
                print('Connection Error')
                continue
            if 'Content-Length' not in response.headers:
                print('Content-Length not found')
                print(url)
                continue
            size = int(response.headers['Content-Length'].strip())
            if size < 1000000:
                print('The found scene is too small: %s (%s)' % (scene['title'], size))
                print(url)
                continue
            
            print('Size of the scene: %s MB' % (size / 1024 / 1024))  # show in MegaBytes
            my_bytes = 0
            widgets = ["Downloading: ", pb.Bar(marker="*", left="[", right=" "),
                       pb.Percentage(), " ", pb.FileTransferSpeed(), "] ",
                       " of {0}MB".format(str(round(size / 1024 / 1024, 2))[:4])]
            pbar = pb.ProgressBar(widgets=widgets, maxval=size).start()
            
            try:
                down = open(path, 'wb')
                for buf in response.iter_content(1024):
                    if buf:
                        down.write(buf)
                        my_bytes += len(buf)
                        pbar.update(my_bytes)
                pbar.finish()
                down.close()
            except KeyboardInterrupt:
                print("\nKeyboard interruption, remove current download and exit execution of script")
                os.remove(path)
                sys.exit(0)
            
            # Check if file is valid
            print("Check if file is valid: ")
            valid = self._is_valid(path)
            
            if not valid:
                downloaded_failed.append(path)
                print('invalid file is being deleted.')
                os.remove(path)
            else:
                downloaded.append(path)
        
        return {'success': downloaded, 'failed': downloaded_failed}
    
    def get_geometries(self):
        """Return list of geometries"""
        return self.__geometries
    
    def get_scenes(self):
        """Return searched and filtered scenes"""
        return self.__scenes
    
    def load_sites(self, input_file):
        """
        Load features from input file and transform geometries to Lat/Lon (EPSG 4326)

        Args:
            input_file: Path to file that can be read by OGR library

        """
        print('===========================================================')
        print('Loading sites from file %s' % input_file)
        
        with Vector(input_file) as vec:
            vec.reproject(4326)
            self.__geometries = vec.convert2wkt()
        
        print('Found %s features' % len(self.__geometries))
    
    @staticmethod
    def multipolygon2list(wkt):
        geom = ogr.CreateGeometryFromWkt(wkt)
        if geom.GetGeometryName() == 'MULTIPOLYGON':
            return [x.ExportToWkt() for x in geom]
        else:
            return [geom.ExportToWkt()]
    
    def print_scenes(self):
        """Print title of searched and filtered scenes"""
        
        def sorter(x): return re.findall('[0-9T]{15}', x)[0]
        
        titles = sorted([x['title'] for x in self.__scenes], key=sorter)
        print('\n'.join(titles))
    
    def search(self, platform, min_overlap=0.001, download_dir=None, start_date=None, end_date=None,
               date_type='beginPosition', **keywords):
        """Search in ESA Data Hub for scenes with given arguments

        Args:
            platform: Define which data to search for (either 'S1A*' for Sentinel-1A or 'S2A*' for Sentinel-2A)
            min_overlap: Define minimum overlap (0-1) between area of interest and scene footprint (Default: 0)
            download_dir: Define download directory to filter prior downloaded scenes (Default: None)
            start_date: Define starting date of search (Default: None, all data)
            end_date: Define ending date of search (Default: None, all data)
            date_type: Define the type of the given dates (please select from 'beginPosition', 'endPosition', and
                'ingestionDate') (Default: beginPosition)
            **keywords: Further OpenSearch arguments can be passed to the query according to the ESA Data Hub Handbook
                (please see https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/3FullTextSearch#Search_Keywords)
                missing under this link:
                - slicenumber: the graticule along an orbit; particularly important for interferometric applications
                    to identify overlapping scene pairs

        Mandatory args:
            platform

        Example usage:
            s1.search('S1A*', min_overlap=0.5, productType='GRD')

        """
        print('===========================================================')
        print('Searching data for platform %s' % platform)
        if platform not in ['S1A*', 'S1B*', 'S2A*', 'S2B*', 'S3A*', 'S3B*']:
            raise Exception('platform parameter has to be S1A*, S1B*, S2A*, S2B*, S3A* or S3B*')
        
        if download_dir is not None:
            self.set_download_dir(download_dir)
        
        date_filtering = ''
        if start_date is not None or end_date is not None:
            if start_date is None:
                raise Exception('Please specify also a starting date!')
            if end_date is None:
                end_date = datetime.now()
            if date_type not in ['beginPosition', 'endPosition', 'ingestionDate']:
                raise Exception('dateType parameter must be one of beginPosition, endPosition, ingestionDate')
            if isinstance(start_date, (datetime, date)):
                start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                start_date = datetime.strptime(start_date, '%Y-%m-%d') \
                    .strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            if isinstance(end_date, (datetime, date)):
                end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                end_date = datetime.strptime(end_date + ' 23:59:59.999', '%Y-%m-%d %H:%M:%S.%f') \
                    .strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            date_filtering = ' AND %s:[%s TO %s]' % (date_type, start_date, end_date)
        
        for geom in self.__geometries:
            print('===========================================================')
            
            index = 0
            scenes = []
            while True:
                url = self._format_url(index, geom, platform, date_filtering, **keywords)
                print('Search URL: %s' % url)
                subscenes = self._search_request(url)
                if len(subscenes) > 0:
                    print('found %s scenes on page %s' % (len(subscenes), index // 100 + 1))
                    scenes += subscenes
                    index += 100
                    print('=============================')
                if len(subscenes) < 100:
                    break
            
            print('%s scenes after initial search' % len(scenes))
            if len(scenes) > 0:
                scenes = self._filter_existing(scenes)
                scenes = self._filter_overlap(scenes, geom, min_overlap)
                print('%s scenes after filtering before merging' % len(scenes))
                self.__scenes = self._merge_scenes(self.__scenes, scenes)
        
        print('===========================================================')
        print('%s total scenes after merging' % len(self.__scenes))
        print('===========================================================')
    
    def set_data_dir(self, data_dir):
        """Set directory for check against existing downloaded files; this can be repeated multiple times to create a list of data directories

        Args:
            data_dir: Path to directory

        """
        print('Adding data directory {}'.format(data_dir))
        self.__data_dirs.append(data_dir)
    
    def set_download_dir(self, download_dir):
        """Set directory for check against existing downloaded files and as directory where to download

        Args:
            download_dir: Path to directory

        """
        print('Setting download directory to %s' % download_dir)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        self.__download_dir = download_dir
    
    def set_geometries(self, geometries):
        """Manually set one or more geometries for data search

        Args:
            geometries: String or List representation of one or more Wkt Geometries,
                Geometries have to be in Lat/Lng, EPSG:4326 projection!

        """
        # print('Set geometries:')
        # print(geometries)
        if isinstance(geometries, list):
            self.__geometries = geometries
        
        elif isinstance(geometries, str):
            self.__geometries = [geometries]
        
        else:
            raise Exception('geometries parameter needs to be a list or a string')
        
        # Test first geometry
        try:
            vec = wkt2vector(self.__geometries[0], srs=4326)
        except RuntimeError as e:
            raise Exception('The first geometry is not valid! Error: %s' % e)
        finally:
            vec = None
    
    def write_results(self, file_type, filename, output=False):
        """Write results to disk in different kind of formats

        Args:
            file_type: the file format to use:
                - 'wget': download bash file with wget software
                - 'json': write the dictionary object
                - 'url': a file with downloadable URLs
                - 'asf': a Python script for download from ASF Vertex
            filename: Path to file
            output: If True the written file will also be send to stdout (Default: False)

        """
        if file_type == 'wget':
            self._write_download_wget(filename)
        elif file_type == 'json':
            self._write_json(filename)
        elif file_type == 'asf':
            self._write_download_asf(filename)
        else:
            self._write_download_urls(filename)
        
        if output:
            with open(filename, 'r') as infile:
                print(infile.read())
    
    def _filter_existing(self, scenes):
        """Filter scenes based on existing files in the define download directory and all further data directories

        Args:
            scenes: List of scenes to be filtered

        Returns:
            Filtered list of scenes

        """
        filtered = []
        dirs = self.__data_dirs + [self.__download_dir]
        for scene in scenes:
            exist = [os.path.isfile(os.path.join(dir, scene['title'] + '.zip')) for dir in dirs]
            if not any(exist):
                filtered.append(scene)
        return filtered
    
    @staticmethod
    def _filter_overlap(scenes, wkt_geometry, min_overlap=0.001):
        """Filter scenes based on the minimum overlap to the area of interest

        Args:
            scenes: List of scenes to filter
            wkt_geometry: Wkt Geometry representation of the area of interest
            min_overlap: Minimum overlap (0-1) in decimal format between scene geometry and area of interest

        Returns:
            Filtered list of scenes

        """
        filtered = []
        
        with wkt2vector(wkt_geometry, srs=4326) as vec1:
            site_area = vec1.getArea()
            for scene in scenes:
                with wkt2vector(scene['footprint'], srs=4326) as vec2:
                    footprint_area = vec2.getArea()
                    inter = intersect(vec1, vec2)
                    if inter is not None:
                        intersect_area = inter.getArea()
                        overlap = intersect_area / site_area
                        inter.close()
                    else:
                        intersect_area = 0
                        overlap = 0
                if overlap > min_overlap or (
                        site_area / footprint_area > 1 and intersect_area / footprint_area > min_overlap):
                    scene['_script_overlap'] = overlap * 100
                    filtered.append(scene)
            
            return filtered
    
    def _format_url(self, startindex, wkt_geometry, platform, date_filtering, **keywords):
        """Format the search URL based on the arguments

        Args:
            wkt_geometry: Geometry in Wkt representation
            platform: Satellite to search in
            dateFiltering: filter of dates
            **keywords: Further search parameters from ESA Data Hub

        Returns:
            url: String URL to search for this data

        """
        with wkt2vector(wkt_geometry, srs=4326) as vec:
            bbox = vec.bbox().convert2wkt()[0]
        
        query_area = ' AND (footprint:"Intersects(%s)")' % bbox
        filters = ''
        for kw in sorted(keywords.keys()):
            filters += ' AND (%s:%s)' % (kw, keywords[kw])
        
        url = os.path.join(self.__esa_api_url,
                           'search?format=json&rows=100&start=%s&q=%s%s%s%s' %
                           (startindex, platform, date_filtering, query_area, filters))
        return url
    
    @staticmethod
    def _is_valid(zipfile, minsize=1000000):
        """
        Test whether the downloaded zipfile is valid
        Args:
            zipfile: the file to be tested
            minsize: the minimum accepted file size

        Returns: True if the file is valid and False otherwise

        """
        if not os.path.getsize(zipfile) > minsize:
            print('The downloaded scene is too small: {}'.format(os.path.basename(zipfile)))
            return False
        try:
            archive = zf.ZipFile(zipfile, 'r')
            try:
                corrupt = True if archive.testzip() else False
            except zlib.error:
                corrupt = True
            archive.close()
        except zf.BadZipfile:
            corrupt = True
        if corrupt:
            print('The downloaded scene is corrupt: {}'.format(os.path.basename(zipfile)))
        else:
            print('file seems to be valid.')
        return not corrupt
    
    @staticmethod
    def _merge_scenes(scenes1, scenes2):
        """Merge scenes from two different lists using the 'id' keyword

        Args:
            scenes1: List of prior available scenes
            scenes2: List of new scenes

        Returns:
            Merged list of scenes

        """
        existing_ids = []
        for scene in scenes1:
            existing_ids.append(scene['id'])
        
        for scene in scenes2:
            if not scene['id'] in existing_ids:
                scenes1.append(scene)
        
        return scenes1
    
    @staticmethod
    def _parse_json(obj):
        """Parse the JSON result from ESA Data Hub and create a dictionary for each scene

        Args:
            obj: Dictionary (if 1 scene) or list of scenes

        Returns:
            List of scenes, each represented as a dictionary

        """
        if 'entry' not in obj['feed']:
            print('No results for this feed')
            return []
        
        scenes = obj['feed']['entry']
        if not isinstance(scenes, list):
            scenes = [scenes]
        scenes_dict = []
        for scene in scenes:
            item = {
                'id': scene['id'],
                'title': scene['title'],
                'url': scene['link'][0]['href']
            }
            
            for data in scene['str']:
                item[data['name']] = data['content']
            
            for data in scene['date']:
                item[data['name']] = data['content']
            
            for data in scene['int']:
                item[data['name']] = data['content']
            
            scenes_dict.append(item)
        
        return scenes_dict
    
    def _search_request(self, url):
        """Do the HTTP request to ESA Data Hub

        Args:
            url: HTTP URL to request

        Returns:
            List of scenes (result from _parseJSON method), empty list if an error occurred

        """
        try:
            content = requests.get(url, auth=(self.__esa_username, self.__esa_password), verify=True)
            if not content.status_code // 100 == 2:
                print('Error: API returned unexpected response {}:'.format(content.status_code))
                print(content.text)
                return []
            result = self._parse_json(content.json())
            for item in result:
                item['footprint'] = self.multipolygon2list(item['footprint'])[0]
            return result
        
        except requests.exceptions.RequestException as exc:
            print('Error: {}'.format(exc))
            return []
    
    def _write_download_asf(self, filename):
        template = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'asf_template.py')
        
        with open(template, 'r') as temp:
            content = temp.read()
            pattern = r'^(?P<sensor>S1[AB])_' \
                      r'(?P<beam>S1|S2|S3|S4|S5|S6|IW|EW|WV|EN|N1|N2|N3|N4|N5|N6|IM)_' \
                      r'(?P<product>SLC|GRD|OCN)' \
                      r'(?P<subproduct>[FHM_])'
            errormessage = '[ASF writer] unknown product: {}'
            targets = []
            for scene in self.__scenes:
                title = scene['title']
                match = re.search(pattern, title)
                if match:
                    meta = match.groupdict()
                    url = 'https://datapool.asf.alaska.edu'
                    if meta['product'] == 'SLC':
                        url += '/SLC'
                    elif meta['product'] == 'GRD':
                        url += '/GRD_{}D'.format(meta['subproduct'])
                    else:
                        raise RuntimeError(errormessage.format(title))
                    url += re.sub(r'(S)1([AB])', r'/\1\2/', meta['sensor'])
                    url += title + '.zip'
                    targets.append(url)
                else:
                    raise RuntimeError(errormessage.format(title))
            linebreak = '\n{}"'.format(' ' * 12)
            filestring = ('",' + linebreak).join(targets)
            replacement = linebreak + filestring + '"'
            content = content.replace("'placeholder_files'", replacement)
            content = content.replace("placeholder_targetdir", self.__download_dir)
            with open(filename, 'w') as out:
                out.write(content)
    
    def _write_download_urls(self, filename):
        """Write URLs of scenes to text file

        Args:
            filename: Path to file to write in

        """
        with open(filename, 'w') as outfile:
            for scene in self.__scenes:
                outfile.write(scene['url'] + '\n')
        return filename
    
    def _write_download_wget(self, filename):
        """Write bash file to download scene URLs based on wget software
        Please note: User authentication to ESA Data Hub (username, password) is being stored in plain text!

        Args:
            filename: Path to file to write in

        """
        with open(filename, 'w') as outfile:
            for scene in self.__scenes:
                out = 'wget -c -T120 --no-check-certificate --user="{}" --password="{}" -O {}.zip "{}"\n' \
                    .format(self.__esa_username, self.__esa_password,
                            os.path.join(self.__download_dir, scene['title']), scene['url'].replace('$', '\$'))
                
                outfile.write(out)
    
    def _write_json(self, filename):
        """Write JSON representation of scenes list to file

        Args:
            filename: Path to file to write in

        """
        with open(filename, 'w') as outfile:
            json.dump(self.__scenes, outfile)
        return True


###########################################################
# Example use of class
# Note: please set your own username and password of ESA Data Hub
###########################################################

def main(username, password):
    """Example use of class:
    Note: please set your own username and password of ESA Data Hub

    Args:
        username: Your username of ESA Data Hub
        password: Your password of ESA Data Hub

    api_hub options:
    'https://scihub.copernicus.eu/apihub/' for fast access to recently acquired imagery in the API HUB rolling archive
    'https://scihub.copernicus.eu/dhus/' for slower access to the full archive of all acquired imagery

    s1 = SentinelDownloader(username, password, api_url='https://scihub.copernicus.eu/apihub/')
    s1.set_geometries('POLYGON ((13.501756184061247 58.390759025092443,13.617310497771715 58.371827474899703,13.620921570075168 58.27891592167088,13.508978328668151 58.233319081414017,13.382590798047325 58.263723491583974,13.382590798047325 58.263723491583974,13.501756184061247 58.390759025092443))')
    s1.set_download_dir('./') # default is current directory
    s1.search('S1A*', 0.8, productType='GRD', sensoroperationalmode='IW')
    s1.write_results(type='wget', file='test.sh.neu')  # use wget, urls or json as type
    s1.download_all()

    """
    
    s1 = SentinelDownloader(username, password, api_url='https://scihub.copernicus.eu/apihub/')
    # s1.load_sites('wetlands_v8.shp')
    s1.set_geometries(
        'POLYGON ((13.501756184061247 58.390759025092443,13.617310497771715 58.371827474899703,13.620921570075168 58.27891592167088,13.508978328668151 58.233319081414017,13.382590798047325 58.263723491583974,13.382590798047325 58.263723491583974,13.501756184061247 58.390759025092443))')
    s1.set_download_dir('./')  # default is current directory
    
    # set additional directories which contain downloaded scenes.
    # A scene is only going to be downloaded if it does not yet exist in either of the data directories or the download directory.
    s1.set_data_dir('/path/to/datadir1')
    s1.set_data_dir('/path/to/datadir2')
    
    s1.search('S1A*', 0.8, productType='GRD', sensoroperationalmode='IW')
    s1.write_results(file_type='wget', filename='sentinel_api_download.sh')  # use wget, urls or json as type
    s1.download_all()
    
    return s1
