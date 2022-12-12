#!/usr/bin/env python3

import os
import sys
import time
import fcntl
from re import search
import pwd
import inspect
import argparse
from urllib.parse import urlparse
import requests
import urllib3
import shutil
import logging
import json
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from maap.maap import MAAP
from maap.Result import Result


urllib3.disable_warnings()


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(stream=sys.stdout, format=log_format, level=logging.INFO)
logger = logging.getLogger('stage_in')


class ArgcException(Exception):
	"""Raised when the number of provided arguments is less than expected."""
	def __init__(self, staging_type, received, expected):
		self.expected = expected
		self.message = '{t}: Not enough arguments (received {c}, expected {e})'
		self.message = self.message.format(t=staging_type, c=(received-1), e=expected)

class CacheException(Exception):
	"""Raised in various caching failure scenarios."""
	def __init__(self, staging_type, error_message):
		self.message = '{t}: Caching Error {e}'
		self.message = self.message.format(t=staging_type, e=(error_message))


class Util:
	@staticmethod
	def create_dest(dest: str = os.path.join(os.getcwd(), 'inputs')) -> str:
		"""Create the destination directory, if it does not already exist."""
		if not os.path.isdir(dest):
			os.makedirs(dest)
		return dest

	@staticmethod
	def is_s3_url(url: str) -> bool:
		"""Attempts to determine if a given URL refers to an S3 bucket.
		
		Returns true if 'url' follows the general format of an S3 URL.
		Returns false otherwise.
		"""
		split = url.split('.')
		# Virtual-hosted–style access (https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html)
		if len(split) >= 5 and split[1] == 's3' and split[3] == 'amazonaws' and split[4].startswith('com/'):
			return True
		# Path-style access
		if len(split) >= 4 and (split[0] == 'https://s3' or split[0] == 's3') and split[2] == 'amazonaws' and split[3].startswith('com/'):
			return True
		# Accessing a bucket through S3 access points
		if len(split) >= 5 and split[1] == 's3-accesspoint' and split[3] == 'amazonaws' and split[4].startswith('com'):
			return True
		# Accessing a bucket using S3://
		if url.startswith('S3://'):
			return True
		return False

	@staticmethod
	def to_list(var) -> list:
		"""If var is not a list, then places it in a list and returns."""
		return var if isinstance(var, list) else [var]

	@staticmethod
	def merge_dict(dict1, dict2):
		"""Merges two dictionaries and returns the copy, without modifying the originals."""
		res = {**dict1, **dict2}
		return res

	@staticmethod
	def rename_file(i: int, dl_path: str) -> str:
		"""Rename file to preserve the list order in future containers."""
		renamed = '{}_{}'.format(str(i), os.path.basename(dl_path))
		renamed = os.path.join(os.path.dirname(dl_path), renamed)
		os.rename(dl_path, renamed)
		return renamed

class StageIn:
	@staticmethod
	def stage_in_http(url: str, dest_file: str, headers: str = None) -> str:
		"""Stage in a file from a HTTP/HTTPS URL.
		Args:
			url (str): HTTP/HTTPS URL of input file.
			dest_file (str): Absolute file path to download towards.
		Returns:
			str: relative path to the staged-in input file
		"""

		# Create the parent directory if it does not exist
		Util.create_dest(os.path.dirname(dest_file))

		# download input file
		r = requests.get(url, headers=headers, stream=True, verify=False)
		r.raise_for_status()
		r.raw.decode_content = True
		with open(dest_file, 'wb') as f:
			shutil.copyfileobj(r.raw, f)

		return dest_file

	@staticmethod
	def stage_in_s3(url: str, dest_file: str, cred: dict = None) -> str:
		"""Stage in a file from an S3 URL.
		Args:
			url (str): S3 URL of input file.
			dest_file (str): Absolute file path to download towards.
			cred (dict): The credential dictionary to be passed into the boto3.client.
		Returns:
			str: relative path to the staged-in input file
		"""

		# Create the parent directory if it does not exist
		Util.create_dest(os.path.dirname(dest_file))

		if cred is None:
			s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client('s3', **cred)

		# download input file
		p = urlparse(url)
		s3.download_file(p.netloc, p.path[1:], dest_file)

		return dest_file

	@staticmethod
	def stage_in_maap(
		collection_concept_id: str,
		readable_granule_name: str,
		dest_file: str, 
		maap_pgt: str = None,
		maap_host: str = 'api.ops.maap-project.org',
	) -> str:
		"""Stage in a MAAP dataset granule.
		Args:
			collection_concept_id (str): the collection-concept-id of the dataset collection
			readable_granule_name (str): either the GranuleUR or producer granule ID
			maap_host (str): IP or FQDN of MAAP API host
		Returns:
			str: relative path to the staged-in input file
		"""

		# Create the parent directory if it does not exist
		Util.create_dest(os.path.dirname(dest_file))

		# Set the MAAP token if it is not None
		if maap_pgt is not None:
			os.environ['MAAP_PGT'] = maap_pgt
			os.environ['MAAP_CONF'] = '/home/jovyan'

		# instantiate maap object
		maap = MAAP(maap_host=maap_host)

		# get granule object
		granule = maap.searchGranule(
			collection_concept_id=collection_concept_id,
			readable_granule_name=readable_granule_name,
		)[0]

		# download input file
		granule._downloadname = os.path.basename(dest_file)
		granule.getData(destpath=os.path.dirname(dest_file))

		return dest_file

	@staticmethod
	def stage_in_maap_http(
		url: str,
		dest_file: str,
		maap_pgt: str = None,
		maap_host: str = 'api.ops.maap-project.org',
	) -> str:
		"""Downloads an HTTP URL from MAAP.
		
		Calls StageIn.stage_in_http after initialization of the MAAP headers.
		"""

		# Create the parent directory if it does not exist
		Util.create_dest(os.path.dirname(dest_file))

		# Set the MAAP token if it is not None
		if maap_pgt is not None:
			os.environ['MAAP_PGT'] = maap_pgt
			os.environ['MAAP_CONF'] = '/home/jovyan'

		# instantiate maap object
		maap = MAAP(maap_host=maap_host)
		r = Result()
		r._cmrFileUrl = f'https://{maap_host}/api/cmr/granules'
		r._apiHeader = maap._get_api_header()
		r._location = url
		r._downloadname = os.path.basename(dest_file)

		return r.getData(os.path.dirname(dest_file))


class Cache:
	@staticmethod
	def cacheable_path(url: str) -> str:

		""" 
		Determine if a given URL can be placed into the local cache by only referencing its path.
		Returns the cacheable path if 'url' references a plain path. This eliminates URI that contains any parameters,
		query, or fragments, since there is some uncertainty about how these might impact file contents.
		Ideally, I think we want a reversible mapping between URL<->URN where URN is the cacheable element
		Its also possible to return a hashed value of the URL as a lookup value.
		Returns cacheable path on success or otherwise null string "" so can test for success
		"""

		p = urlparse(url)
		if search('''[!*'"();:@&=+$,?]''', p.path) or p.params or p.query or p.fragment:
			logger.warning('s3 url contains characters, params, query, or fragment that prevent transparent caching')
			return ''
		else:
			# p.netloc for some urls may not be in a neat compact format, or have unnecessary region names.
			# we might want to strip some of that off. For example:

			# https://chaos42.s3.us-west-2.amazonaws.com/chaos42.txt vs s3://chaos42/chaos42.txt we really just want the bucket name
			# and not all of chaos42.s3.us-west-2.amazonaws.com
			return p.netloc+p.path

	@staticmethod
	def lock_cache(cache_dir: str, cache_file_path: str) -> bool: 

		"""
		RBC grab a lock associated with a cacheable file. various scheme of varying complexity are possible..
		The semantic we want is that if the cacheable file exists, its a full copy. So we lock a lock file (foo.LOCK)
		and download into a different file (foo.PART), then atomic rename to final (foo) once fully copied in.
		Of course these locks rely on POSIX advisory locking, but conceptually easily to port to other platforms.

		Returns full path on lock success, otherwise warn and return false to caller
		"""

		user_name=pwd.getpwuid(os.getuid())[0]

		full_cache_file_path= os.path.join(cache_dir, user_name, 'S3cache', cache_file_path)

		try:
			os.makedirs(os.path.dirname(full_cache_file_path), exist_ok=True)
			#os.makedirs(os.path.dirname(fullCacheableSysFilePath), exist_ok=True)
		except:
			logger.warning('Failed to create cache directory in local MAAP cache: '+ os.path.dirname(full_cache_file_path))
			return False


		# we will try for 10 minutes without any progress before giving up. Note that the stat might not correctly capture growth
		# once we give up, there are other options...

		Tries=tries=60
		sleep=10
		progress=0
		last_size=0
		try:
			fd = open(full_cache_file_path+'.lock', 'w+')
			while tries:

				try:
					fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
					return full_cache_file_path

				except IOError as e:

					# raise on unrelated IOErrors
					if e.errno != errno.EAGAIN:
						logger.warning('Failed to fcntl.flock file in MAAP cache: '+ full_cache_file_path+'.lock')
						return False
					else:
						if os.path.exists(full_cache_file_path+'.part'):
							part_size=os.path.getsize(full_cache_file_path+'.part')
							if (last_size < part_size): 
								tries = Tries
								last_size = part_size
						time.sleep(sleep)
						tries=tries-1

		except:
			logger.warning('Failed to obtain lock file in MAAP cache: '+ full_cache_file_path+'.lock')
			return False

		# timed out waiting. 10 minutes without the file growing.
		logger.warning('Waiting for locks timed out - could not obtain: '+ full_cache_file_path+'.lock')
		return False

	@staticmethod
	def cache_hit(
		cache_dir: str,
		unique_cacheable_file_path: str,
		restage_in: bool,
		integrity_func,
		params: dict
	) -> str:

		"""
		RBC try to find a copy of a cacheable file in either the system of the users cache locations
		Returns string containing file location or null ""
		"""

		try:
			user_name = pwd.getpwuid(os.getuid())[0]
				
			# look for path in the users private cache
			full_path = os.path.join(cache_dir, user_name, 'S3cache', unique_cacheable_file_path)
			if os.path.exists(full_path):
				if restage_in:
					logger.warning('removing file from cache and restaging in: ' + full_path)
					os.remove(full_path)
					return ''

				if integrity_func(**params):
					return full_path

				# failed integrity
				logger.warning('removing file that failed integrity check in user cache: ' + full_path)
				os.remove(full_path)
				return ''

		except KeyError as e:
			logger.error('Exception caught within Cache.cache_hit(): {}'.format(e.what()))

		# look for path in the systems public cache
		full_path = os.path.join(cache_dir, 'S3cache', unique_cacheable_file_path)
		if os.path.exists(full_path):
			if restage_in:
				logger.warning('ignoring file in system cache - can not restage: ' + full_path)
				#can't os.remove(full_path)
				return ''

			if integrity_func(**params):
				return full_path

			# failed integrity
			logger.warning('ignoring file that failed integrity check in system cache: ' + full_path)
			#can't os.remove(full_path)
			return ''

		return ''

	@staticmethod
	def s3_check_etag(url: str, dest_file: str = None, cred: dict = None) -> str:
		""" Check to see that a cached file matches its web based ETAG
		Args:
			url (str): S3 URL of input file
			unsigned (bool): send unsigned request
		Returns:
			bool: good or bad etag check
		"""

		logger.debug('XX ETAG CHECK not implemented yet but is called!')

		p = urlparse(url)

		if cred is None:
			s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client('s3', **cred)

		return True


def main(argc: int, argv: list) -> int:

	"""
	call: stage_in.py <input.json> <staging_type> [options]
			staging_type: HTTP | S3_unsigned | S3 | DAAC | MAAP | MAAP_HTTP | Role | Local

	stage_in.py is called in either a one or two pass mode. Because of various failures that can happen, we like to be resiliant to fails.

	One pass mode:
		stage_in.py is only called one time in the main workflow.cwl. If a cache is configured, it will be checked for the cached object. 
		If found, will be copied to /input. If not found, and has a named cacheable reference, it will be copied into the cache, and then into /input. 
		If it can't be cached, it will be directly copied into /input.

	Two pass mode:
		stage_in.py is called twice, once in prime cache mode where just the stage_in.cwl section is run first and then entire workflow.cwl
		is run sometime in the future. On a busy system, these two passes may occur some time apart since they are separate jobs. 

		Pass one of two pass:
			Same behavior as the single pass of One pass mode above, except that only cacheable objects are copied into cache and not into /input 
		Pass two of two pass:
			Same behavior as the single pass of One pass mode above. 

		Note: that workflow.cwl may possibly be constructed so that cwl-runner could run a stage_in step with --single-step or --target. This not 
		needed at this point because a seporate cache_workflow.cwl is being created based on the stage_in steps in workflow.cwl. This note just 
		to keep that thought around for future investigations with the capabilities of cwl.


	To facilitate caching, the main section of stage_in.py is broken into three steps:

		Step 1: If possible, convert the staging request arguments into a path, an MD5 hash handle, or other unique mapping that can 
			be used to locate the object that is cached. Ideally, this mapping is reversible, that is we can take this string and find its 
			internet location or its cache location. But at a minimum, the mapping from request name (e.g. url or MD5) to cache name is unique.
		Step 2: If not cached, copy the file into the cache, if caching enabled
		Step 3: Copy into main workflow from cache if cached or from remote if not


	 A limited number of command line arguments is supported mainly for debuging and not used during normal processing
	 [only the -r option currently functions]

	-r remove cached item from the cache and download again
	-d turn on debugging messages

	After processing the command line arguments, argc and argv are returned to how they are run during normal processing
	of stage_in.py argparse is good enough to even handle appended known args...

	The three different processing scenarios should be handled correctly:
			1) two pass processing: stage_in runs to prime the cache by explicitly calling stage_in just to prime the cache
			2) cached one pass processing: stage_in will fill the cache, but in-line with processing. Subsequent run will have a successful hit
			3) original one pass processing: no cache. stage_in is processed inline and subsequent runs will have to pull the file again
	"""

	parser = argparse.ArgumentParser(prog='stage_in.py',
		formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=100),
		description = 'stage_in data to local cache, into an actual cwl process step, or both',
		epilog = ' First implementation of 2-pass caching. Something to consider is a symlink being copied in instead of a file copy in.  \
			   This might reduce the use of tmpdir space (/tmp maps to /dev/shm on HECC), and the accompaniing memory usage. Another  \
			   approach would be to configure cwl to use an actual shared filesystem instead of the default /tmp, which may have unexpected \
			   memory consumption due to input/output file sizes.\n\n')

	parser.add_argument('-r', dest='restage_in', action='store_true', help='ignore any previously cached file')
	parser.add_argument('-d', dest='debug'     , action='store_true', help='ignore any previously cached file')

	args, nargv = parser.parse_known_args()

	nargv = [argv[0]] + nargv[:]
	nargc = len(nargv)
	argv=nargv
	argc=nargc

	# I'd like to keep the argument processing section in place for now. It makes for much easier testing than editing json input files. 
	# We may also want to retain the -r for flushing/repoplulate the cache. We can revisit this later

	restage_in = args.restage_in

	if args.debug:
		logger.setLevel(logging.DEBUG)


	# Verify the number of positional arguments is as expected
	expected_argc = 2
	if argc < expected_argc + 1:
		raise ArgcException('FATAL', argc, expected_argc)

	# The first input should always be JSON inputs filename
	inputs_json = argv[1]
	# The second input should always be the deduced download type
	staging_type = argv[2]

	# Convert inputs_json from a filename to a JSON dictionary represented in Python
	if os.path.exists(inputs_json):
		with open(inputs_json, 'r') as f:
			inputs_json = json.load(f)
	else:
		logger.error('{} does not exist, resulting in a fatal error.'.format(inputs_json))
		return 1

	# Extract the input parameters and supplementary flags
	input_path = inputs_json['input_path']
	cache_only = inputs_json.get('cache_only', False)
	cache_dir = inputs_json.get('cache_dir')['path'] if inputs_json.get('cache_dir') is not None else ''

	dest_dir = os.path.join(os.getcwd(), 'inputs') if cache_dir == '' else cache_dir

	# Use a dictionary to determine which execution branch to use
	staging_map = {
		'HTTP': [StageIn.stage_in_http, None],
		'S3_unsigned': [StageIn.stage_in_s3, Cache.s3_check_etag],
		'S3': [StageIn.stage_in_s3, Cache.s3_check_etag],
		'DAAC': [None, None],
		'MAAP': [StageIn.stage_in_maap, None],
		'MAAP_HTTP': [StageIn.stage_in_maap_http, None],
		'Role': [None, None],
		'Local': [None, None]
	}

	download_func = staging_map[staging_type][0]
	integrity_func = staging_map[staging_type][1]
	params = {}

	# Based on the staging type, walk through the input path JSON and create a list of targets
	extra_param_list = None
	try:
		if staging_type == 'HTTP':
			extra_param_list = [{'url': url} for url in Util.to_list(input_path['url'])]
		elif staging_type == 'S3_unsigned':
			extra_param_list = [{'url': url} for url in Util.to_list(input_path['s3_url'])]
			params['cred'] = None
		elif staging_type == 'S3':
			extra_param_list = [{'url': url} for url in Util.to_list(input_path.pop('s3_url'))]
			params['cred'] = input_path
		elif staging_type == 'DAAC':
			extra_param_list = [{}]
			params['url'] =  Util.to_list(input_path['url'])
			params['username'] = input_path['username']
			params['password'] = input_path['password']
		elif staging_type == 'MAAP':
			extra_param_list = [{
				'collection_concept_id': input_path['collection_concept_id'],
				'readable_granule_name': input_path['readable_granule_name']
			}]
			params['maap_pgt'] = input_path['maap_pgt']
		elif staging_type == 'MAAP_HTTP':
			extra_param_list = [{'url': url} for url in Util.to_list(input_path['url'])]
			params['maap_pgt'] = input_path['maap_pgt']
		elif staging_type == 'Role':
			extra_param_list = [{
				'role_arn': input_path['role_arn'],
				'source_profile': input_path['source_profile'],
			}]
		elif staging_type == 'Local':
			path_list = [x['path'] for x in Util.to_list(input_path['path'])]
			for i, path in enumerate(path_list):
				if os.path.exists(path):
					inputs_dir = Util.create_dest(os.path.join(dest_dir, i))
					path_dest = os.path.join(inputs_dir, os.path.basename(path))
					shutil.move(path, path_dest)
				else:
					logger.error('"{}" does not exist, now exiting...'.format(path))
					return 1
			return 0
		else:
			logger.error('Unsupported staging type: ' + staging_type)
			return 1
	except ArgcException as e:
		logger.error(e.message)
		return 1

	# create inputs directory
	dest_dir = Util.create_dest()

	# Loop over the list based parameters and merge them with the base credentials in params
	for i, extra_params in enumerate(extra_param_list):

		joined_params = Util.merge_dict(params, extra_params)
		stage_file_to_input = not cache_only

		try:
			# break out the path from the url
			possibly_cacheable_url = joined_params['url']
			p = urlparse(possibly_cacheable_url)
			staged_file = os.path.join(os.path.join(dest_dir, str(i)), os.path.basename(p.path))

			# check to see that we have a cache dir and a path that we can cache to
			unique_cacheable_file_path = Cache.cacheable_path(possibly_cacheable_url)
			if (cache_dir == '' or not unique_cacheable_file_path):
				# no caching directory or not a cacheable path
				if stage_file_to_input:
					dl_style = 'DownloadedToInput'
					joined_params['dest_file'] = staged_file
					dl_path = download_func(**joined_params)
					Util.rename_file(i, dl_path)
					logger.info('({}) {}: '.format(staging_type, dl_style) + dl_path)
					continue

			# we are caching and have a unique_cacheable_file_path so the download object can be stored in the cache
			full_cache_file_path = Cache.cache_hit(cache_dir, unique_cacheable_file_path, restage_in, integrity_func, joined_params)
			if not full_cache_file_path:

				# we have cache miss, so go get it into the cache
				full_cache_file_path = Cache.lock_cache(cache_dir, unique_cacheable_file_path)
				if not full_cache_file_path:

					# failed to lock the cache for file download - copy it to default input as a backup plan
					logger.warning('Failed to lock file for download: '+ unique_cacheable_file_path)
					if stage_file_to_input:
						dl_style='CacheFail:Download'
						joined_params['dest_file'] = staged_file
						dl_path = download_func(**joined_params)
						Util.rename_file(i, dl_path)
						logger.info('({}) {}: '.format(staging_type, dl_style) + dl_path)

					else:
						logger.warning(staging_type, 'Failed to lock cache file : ' + cache_dir + '/' + unique_cacheable_file_path)
					continue

				else:

					# I have the lock for the file and I'm going to cache it.
					# here is the point at which I could call out to a system specific caching function to put the file into a shared system area.
					# for example - the NAS #CLOUD cache, but for now, we always cache to the users private area. Some agreement on cache path
					# naming between MAAP and NAS #cloud caching is important so that MAAP can use NAS WebCache to reduce egress.

					# remove any failed pieces from previous tries
					os.remove(full_cache_file_path+'.part') if os.path.exists(full_cache_file_path+'.part') else None

					dl_style='DownloadToCache'
					joined_params['dest_file'] = full_cache_file_path+'.part'
					dl_path = download_func(**joined_params)
					os.rename(full_cache_file_path+'.part', full_cache_file_path)
					os.remove(full_cache_file_path+'.lock')

					# file is now in cache 
					if stage_file_to_input:
						shutil.copy(full_cache_file_path, staged_file)
						dl_style=dl_style+':CopyToInput'
						dl_path = staged_file
						Util.rename_file(i, dl_path)

			else:
				# file was found in cache 
				logger.debug('file found in cache: ' + full_cache_file_path)
				dl_style='CacheHit'
				dl_path=full_cache_file_path

				if stage_file_to_input:
					shutil.copy(full_cache_file_path, staged_file)
					dl_style=dl_style+':CopyToInput'
					dl_path = staged_file
					Util.rename_file(i, dl_path)

		except CacheException as e:
			print(e.message)
			return 1

		logger.info('({}) {}: '.format(staging_type, dl_style) + dl_path)


	return 0


if __name__ == '__main__':
	main(len(sys.argv), sys.argv)