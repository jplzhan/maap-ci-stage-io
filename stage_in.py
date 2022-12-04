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
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from maap.maap import MAAP


urllib3.disable_warnings()


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING)
logger = logging.getLogger('stage_in')


class ArgcException(Exception):
	"""Raised when the number of provided arguments is less than expected."""
	def __init__(self, staging_type, received, expected):
		self.expected = expected
		self.message = '{t}: Not enough arguments (received {c}, expected {e})'
		self.message = self.message.format(t=staging_type, c=(received-2), e=expected)

class CacheException(Exception):
	"""Raised in various caching failure scenarios."""
	def __init__(self, staging_type, error_message):
		self.message = '{t}: Caching Error {e}'
		self.message = self.message.format(t=staging_type, e=(error_message))


class Util:
	@staticmethod
	def create_dest_dir(inputs_dest_dir: str = os.path.join(os.getcwd(), 'inputs')) -> str:
		"""Create inputs directory."""

		if not os.path.isdir(inputs_dest_dir):
			os.makedirs(inputs_dest_dir)
		return inputs_dest_dir

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


class StageIn:
	@staticmethod
	def stage_in_http(url: str, get_url: dict = False, input_dest_file: str = None) -> str:
		"""Stage in a file from a HTTP/HTTPS URL.
		Args:
			url (str): HTTP/HTTPS URL of input file
		Returns:
			str: relative path to the staged-in input file
		"""

		# only interested in the potentially cacheable path name
		if (get_url):
			return url

		# download input file

		r = requests.get(url, stream=True, verify=False)
		r.raise_for_status()
		r.raw.decode_content = True
		with open(input_dest_file, "wb") as f:
			shutil.copyfileobj(r.raw, f)

		return input_dest_file

	@staticmethod
	def stage_in_s3(url: str, get_url: dict = False, input_dest_file: str = None, cred: dict = None) -> str:
		"""Stage in a file from an S3 URL.
		Args:
			url (str): S3 URL of input file
			unsigned (bool): send unsigned request
		Returns:
			str: relative path to the staged-in input file
		"""

		# only interested in the potentially cacheable path name
		if (get_url):
			return url

		p = urlparse(url)

		if cred is None:
			s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client('s3', **cred)

		# download input file
		s3.download_file(p.netloc, p.path[1:], input_dest_file)

		return input_dest_file

	@staticmethod
	def stage_in_maap(
		collection_concept_id: str,
		readable_granule_name: str,
		get_url: dict = False,
		input_dest_file: str = None, 
                # TODO: remove these commented parameters if there is no need for them
		# user_token: str,
		# application_token: str,
		maap_host: str = 'api.ops.maap-project.org',
	) -> str:
		"""Stage in a MAAP dataset granule.
		Args:
			collection_concept_id (str): the collection-concept-id of the dataset collection
			readable_granule_name (str): either the GranuleUR or producer granule ID
			user_token (str): MAAP user token (retrieved from https://auth.ops.maap-project.org/)
			application_token (str): MAAP application token
			maap_host (str): IP or FQDN of MAAP API host
		Returns:
			str: relative path to the staged-in input file
		"""
                
		# RBC. I'm set up to do caching for maap granules. BUT, I don't have an understanding of the maap.searchGranule or the maap.getDownloadUrl code.
		# in particular, do these calls have side effects or overheads where we want to avoid calling them more than once. What I want is a cacheable path
		# name that I derive from the url - and it looks like I can get that right here. But I may need to call searchGranule before getDownloadUrl
		# and only want to call searchGranule once, requiring some static data....
		# but I just return a fail here as uncacheable for now....

		if (get_url):
			return False

		# instantiate maap object
		maap = MAAP(maap_host=maap_host)

		# get granule object
		granule = maap.searchGranule(
			collection_concept_id=collection_concept_id,
			readable_granule_name=readable_granule_name,
		)[0]

		# parse url
		url = granule.getDownloadUrl()
		p = urlparse(url)

		# download input file
		input_dest_file = os.path.join(inputs_dest_dir, os.path.basename(p.path))
		granule.getData(destpath=os.path.dirname(input_dest_file))

		return input_dest_file


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
	def lock_cache(caching_directory: str, cache_file_path: str) -> bool: 

		"""
		RBC grab a lock associated with a cacheable file. various scheme of varying complexity are possible..
		The semantic we want is that if the cacheable file exists, its a full copy. So we lock a lock file (foo.LOCK)
		and download into a different file (foo.PART), then atomic rename to final (foo) once fully copied in.
		Of course these locks rely on POSIX advisory locking, but conceptually easily to port to other platforms.

		Returns full path on lock success, otherwise warn and return false to caller
		"""

		user_name=pwd.getpwuid(os.getuid())[0]

		full_cache_file_path= os.path.join(caching_directory, user_name, 'S3cache', cache_file_path)

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
	#def cache_hit(caching_directory: str, unique_cacheable_file_path: str) -> str:
	def cache_hit(caching_directory: str, unique_cacheable_file_path: str, restage_in: bool, integrity_func, params: dict) -> str:

		"""
		RBC try to find a copy of a cacheable file in either the system of the users cache locations
		Returns string containing file location or null ""
		"""

		user_name=pwd.getpwuid(os.getuid())[0]

		if os.path.exists(full_path:=os.path.join(caching_directory, user_name, 'S3cache', unique_cacheable_file_path)):
			if restage_in:
				logger.warning('removing file from cache and restaging in: '+ full_path)
				os.remove(full_path)
				return ''

			if integrity_func(**params):
				return   full_path

			else:
				# failed integrity
				logger.warning('removing file that failed integrity check in user cache: '+ full_path)
				os.remove(full_path)
				return ''

		if os.path.exists(full_path:=os.path.join(caching_directory, 'S3cache', unique_cacheable_file_path)):
			if restage_in:
				logger.warning('ignoring file in system cache - can not restage: '+ full_path)
				#can't os.remove(full_path)
				return ''

			if integrity_func(**params):
				return   full_path

			else:
				# failed integrity
				logger.warning('ignoring file that failed integrity check in system cache: '+ full_path)
				#can't os.remove(full_path)
				return ''


		return ''

	@staticmethod
	def s3_check_etag(url: str, get_url: dict = False, input_dest_file: str = None, cred: dict = None) -> str:
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

		# get the files etag and check it
		# s3.download_file(p.netloc, p.path[1:], input_dest_file)
		# not implemented yet

		return True


def main(argc: int, argv: list) -> int:

	"""
		
	add arguments to implement the transparent caching functionality.

	-s only prime cache. items will only be copied int the local cache is possible. Only used for pre-staging files

	-C <path> : location of the local cache. Stage_in will search the local cache for the request file and copy it 
				to the /input directory is found. If not found in the cache, then will be pulled in from the remote 
				location to the cache and /input. This might better be in a config file per ADES. 
				
	-r remove cached item from the cache and download again
	-d turn on debugging messages

	After processing the extra arguments, argc and argv are returned to how they would have been set in previous releases
	of stage_in.py to eliminate changes in how the arguments are processed below.  argparse is good enough to even handle 
	appended known args...

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

	parser.add_argument('-c', dest='caching_directory'  , metavar='<path>', action='store',      help='path to local cache directory')
	parser.add_argument('-s', dest='only_prime_cache',                   action='store_true', help='prime cache by only staging to cache')
	parser.add_argument('-r', dest='restage_in'      ,                   action='store_true', help='ignore any previously cached file')
	parser.add_argument('-d', dest='debug'           ,                   action='store_true', help='ignore any previously cached file')

	args , nargv = parser.parse_known_args()

	nargv = [argv[0]] + nargv[:]
	nargc = len(nargv)
	argv=nargv
	argc=nargc

	caching_directory = args.caching_directory
	stage_file_to_input = not args.only_prime_cache
	restage_in = args.restage_in

	if args.debug:
		logger.setLevel(10)


	"""
	RBC - stage_in.py is called in either a one or two pass mode. Because of various failures that can happen, we like to be resiliant to 
	One pass mode:
		stage_in.py is only called one time in the main workflow.cwl. If a cache is configured, it will be checked for the cached object. 
		If found, will be copied to /input. If not found, and has a named cacheable reference, it will be copied into the cache, and then into /input. 
		If it can't be cached, it will be directly copied into /input.

	Two pass mode:
		stage_in.py is called twice, once in prime cache mode where just the stage_in.cwl section is run first and then entire workflow.cwl
		is run sometime in the future. On a busy system, these two passes may occur some time apart since they are separate jobs. 

		workflow.cwl could be constructed so that cwl-runner could run a single stage_in step with --, that staged multiple files.

		Pass one of two pass:
			Same behavior as the single pass of One pass mode above, except that only cacheable objects are copied into cache and not into /input 
		Pass two of two pass:
			Same behavior as the single pass of One pass mode above. 

	To facilitate caching, we have to determine whether an objects reference (e.g. url) is cacheable. To do this, main section of stage_in.py is broken 
	into two steps:

		Step 1: If possible, convert the staging request arguments into a path, an MD5 hash handle, or other unique mapping that can 
			be used to locate the object that is cached. Ideally, this mapping is reversible, that is we can take this string and find its 
			internet location or its cache location. But at a minimum, the mapping from request name (e.g. url or MD5) to cache name is unique.
			This requires a call the staging functions to get the url, since the maap requires some calls to resolve the url.
		Step 2: If not cached, copy the file into the cache
		Step 3: 

	"""


	staging_type = argv[1]
	staging_map = {
		'HTTP': [StageIn.stage_in_http, {}, None],
		'S3_unsigned': [StageIn.stage_in_s3, {'cred': None}, Cache.s3_check_etag],
		'S3': [StageIn.stage_in_s3, {'cred': None}, Cache.s3_check_etag],
		'DAAC': [None, {}, None],
		'MAAP': [StageIn.stage_in_maap, {}, None],
		'Role': [None, {}, None],
		'Local': [None, {}, None]
	}

	download_func = staging_map[staging_type][0]
	params = staging_map[staging_type][1]
	integrity_func = staging_map[staging_type][2]

	try:
		if staging_type in ['HTTP', 'S3_unsigned']:
			expected_argc = 1
			if argc < expected_argc + 2:
				raise ArgcException(staging_type, argc, expected_argc)

			params['url'] = argv[2]
		elif staging_type == 'S3':
			expected_argc = 5
			if argc < expected_argc + 2:
				raise ArgcException(staging_type, argc, expected_argc)

			# Submit the URL as a parameter to the S3 function
			params['url'] = argv[2]

			aws_dir = os.path.join(os.path.expanduser('~'), '.aws')
			if not os.path.isdir(aws_dir):
				os.makedirs(aws_dir)

			staging_map['S3'][1]['cred'] = {
				'aws_access_key_id': argv[3],
				'aws_secret_access_key': argv[4],
				'aws_session_token': argv[5],
				'region_name': argv[6],
			}
		elif staging_type == 'DAAC':
			expected_argc = 3
			if argc < expected_argc + 2:
				raise ArgcException(staging_type, argc, expected_argc)

			params['url'] = argv[2]
			params['username'] = argv[3]
			params['password'] = argv[4]
		elif staging_type == 'MAAP':
			expected_argc = 2
			if argc < expected_argc + 2:
				raise ArgcException(staging_type, argc, expected_argc)

			params['collection_concept_id'] = argv[2]
			params['readable_granule_name'] = argv[3]
		elif staging_type == 'Role':
			expected_argc = 2
			if argc < expected_argc + 2:
				raise ArgcException(staging_type, argc, expected_argc)

			params['role_arn'] = argv[2]
			params['source_profile'] = argv[3]
		elif staging_type == 'Local':
			path = argv[2]
			if os.path.exists(path):
				inputs_dest_dir = Util.create_dest_dir()
				dst = os.path.join(inputs_dest_dir, os.path.basename(path))
				shutil.move(path, dst)
				return 0
			else:
				print('"{}" does not exist, now exiting...'.format(path))
				return 1
		else:
			print('Unsupported staging type: ' + staging_type)
			return 1
	except ArgcException as e:
		print(e.message)
		return 1

	# create inputs directory
	inputs_dest_dir = Util.create_dest_dir()

	try: 
		# set up the default file path for all downloads to be copied into
                # this requires consistant behavior from all download functions

		add_params = params.copy()
		add_params['get_url'] = True
		# only download functions that return a url will be considered for caching
		if possibly_cacheable_url := download_func(**add_params):
			p = urlparse(possibly_cacheable_url)
			staged_file = os.path.join(inputs_dest_dir, os.path.basename(p.path))

			if (caching_directory != ''):

        		# we have a local caching directory, try to get a unique_cacheable_file_path so the download object can be stored in the cache
				staged_file = os.path.join(inputs_dest_dir, os.path.basename(p.path))

				# the download function has returned a possible name that we can cache. (not all download functions do)
				# even if a download function supports cacheable paths, not all provided names may be cacheable
				if unique_cacheable_file_path := Cache.cacheable_path(possibly_cacheable_url):

					# we sucessfully created a unique name that can be cached.
					if not (full_cache_file_path := Cache.cache_hit(caching_directory, unique_cacheable_file_path, args.restage_in, integrity_func, params)):

						# we have a unique_cacheable_file_path and a cache miss, so go get it into the cache
						if not (full_cache_file_path := Cache.lock_cache(caching_directory, unique_cacheable_file_path)):

							# failed to lock the file for download - copy it to default input as a backup plan
							logger.warning('Failed to lock file for download: '+ unique_cacheable_file_path)
							if stage_file_to_input:
								add_params.clear()
								add_params = params.copy()
								add_params['input_dest_file'] = staged_file
								dl_path = download_func(**add_params)
								dl_style='CacheFail:Download'

							else:
								raise CacheException(staging_type, 'Failed to lock cache file : ' + unique_cacheable_file_path)

						else:

							# I have the lock for the file and I'm going to cache it.
							# here is the point at which I could call out to a system specific caching function to put the file into a shared system area.
							# for example - the NAS #CLOUD cache, but for now, we always cache to the users private area.
							# remove any failed pieces for 
							os.remove(full_cache_file_path+'.part') if os.path.exists(full_cache_file_path+'.part') else None

							add_params.clear()
							add_params = params.copy()
							add_params['input_dest_file'] = full_cache_file_path+'.part'
							dl_path = download_func(**add_params)
							dl_style='DownloadToCache'
							os.rename(full_cache_file_path+'.part', full_cache_file_path)
							os.remove(full_cache_file_path+'.lock')

							# file is now in cache 
							if stage_file_to_input:
								shutil.copy(full_cache_file_path, staged_file)
								dl_path = staged_file
								dl_style=dl_style+':CopyToInput'

					else:
						# file was found in cache 
						logger.debug('file found in cache: ' + full_cache_file_path)
						dl_path=full_cache_file_path
						dl_style='CacheHit'

						if stage_file_to_input:
							shutil.copy(full_cache_file_path, staged_file)
							dl_path = staged_file
							dl_style=dl_style+':CopyToInput'
				else:
					# can't produce a unique_cacheable_file_path 
					if stage_file_to_input:
						add_params.clear()
						add_params = params.copy()
						add_params['input_dest_file'] = staged_file
						dl_path = download_func(**add_params)
						dl_style='DownloadedToInput'

			else:
				# no caching directory
				if stage_file_to_input:
					add_params.clear()
					add_params = params.copy()
					add_params['input_dest_file'] = staged_file
					dl_path = download_func(**add_params)
					dl_style='DownloadedToInput'
		else:
			# The only non null function at this point is for maap
			# because I haven't implemented the Stage.maap for caching yet
			add_params.clear()
			add_params = params.copy()
			add_params['input_dest_file'] = staged_file
			dl_path = download_func(**add_params)
			dl_style='DownloadedToInput'

			# if I can get confirm maap behavior then this can be uncommented...
			## Currently, only download functions that don't to anything will land here
			#print('No download function for ({}): '.format(staging_type))
			#return 0
				
	except CacheException as e:
		print(e.message)
		return 1

	print('({}) {}: '.format(staging_type, dl_style) + dl_path)

	return 0


if __name__ == '__main__':
	main(len(sys.argv), sys.argv)
