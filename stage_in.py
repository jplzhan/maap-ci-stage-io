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


class Util:
	@staticmethod
	def create_inputs_dir(inputs_dir: str = os.path.join(os.getcwd(), 'inputs')) -> str:
		"""Create inputs directory."""

		if not os.path.isdir(inputs_dir):
			os.makedirs(inputs_dir)
		return inputs_dir

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



def cacheable_path(url: str) -> str:
	"""RBC Attempts to determine if a given URL can be placed into the local cache by only referencing its path.
		   Returns the cachable path if 'url' references a plain path. This eliminates URI that contains any parameters, 
		   query, or fragments, since there is some uncertainty about how these might impact file contents. 
		   Ideally, I think we want a reversable mapping between URL<->URN where URN is the cachable element
		   Its also possible to return a hashed value of the URL as a lookup value. 
		   Returns cachable path on sucess or otherwise null string "" so can test for success
	"""
	p = urlparse(url)
	if search('''[!*'"();:@&=+$,?]''', p.path) or p.params or p.query or p.fragment:
		logger.warning("s3 url contains characters, params, query, or fragment that prevent transparent caching")
		return ''
	else:
		# p.netloc for some urls may not be in a neat compact format, or have unecessary region names.
		# we might want to strip some of that off. For example:

		# https://chaos42.s3.us-west-2.amazonaws.com/chaos42.txt vs s3://chaos42/chaos42.txt we really just want the bucket name
		# and not all of chaos42.s3.us-west-2.amazonaws.com
		return p.netloc+p.path

def lock_cache(fullCacheableFilePath: str, fullCacheableSysFilePath: str) -> bool:
	"""RBC grab a lock associated with a cachable file. various scheme of varing complexity are possible..
	   The semantic we want is that if the cacheable file exists, its a full copy. So we lock a lock file (foo.LOCK)
	   and download into a different file (foo.PART), then atomic rename to final (foo) once fully copied in. 
	   Of course these locks rely on POSIX advisory locking, but conceptually easily to port to other platforms.

		   Returns true on lock success, otherwise warn and return false to caller
	"""

	try: 
		os.makedirs(os.path.dirname(fullCacheableFilePath), exist_ok=True)
		os.makedirs(os.path.dirname(fullCacheableSysFilePath), exist_ok=True)
	except:
		logger.warning("Failed to create cache directory in local MAAP cache", os.path.dirname(fullCacheableFilePath))
		return False


	# we will try for 10 minutes without any progress before giving up. Note that the stat might not correctly capture growth
	# once we give up, there are other options...
	Tries=tries=60
	sleep=10
	progress=0
	lastSize=0
	try:
		fd = open(fullCacheableFilePath+'.lock', 'w+')
		while tries:

			try:
				fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
				return fd

			except IOError as e:
				# raise on unrelated IOErrors
				if e.errno != errno.EAGAIN:
					logger.warning("Failed to fcntl.flock file in MAAP cache", fullCacheableFilePath+'.lock')
					#raise
					return False
				else:
					if os.path.exists(fullCacheableFilePath+'.part'):
						partSize=os.path.getsize(fullCacheableFilePath+'.part') 
						if (lastSize < partSize):
							tries = Tries
							lastSize = partSize
					time.sleep(sleep)
					tries=tries-1

	except:
		logger.warning("Failed to open lock file in MAAP cache", fullCacheableFilePath+'.lock')
		return False

	# I timed out waiting. 10 minutes without the file growing. 
	logger.warning("Waiting for locks timeed out - could not obtain: ", fullCacheableFilePath+'.lock')
	return False 
		
def cache_hit(urnCacheableFilePath: str) -> str:

	"""
	   RBC try to find a copy of a cachable file in either the users or systems cache locations
	   Returns string containing file location or null ""
	"""
	global localCachePath
	userName=pwd.getpwuid(os.getuid())[0]

	if os.path.exists(os.path.join(localCachePath, userName, "S3cache", urnCacheableFilePath)):
		return os.path.join(localCachePath, userName, "S3cache", urnCacheableFilePath)

	if os.path.exists(os.path.join(localCachePath, "S3cache", urnCacheableFilePath)):
		return os.path.join(localCachePath, "S3cache", urnCacheableFilePath)
  
	return ""

class StageIn:
	@staticmethod
	def stage_in_http(url: str) -> str:
		"""Stage in a file from a HTTP/HTTPS URL.
		Args:
			url (str): HTTP/HTTPS URL of input file
		Returns:
			str: relative path to the staged-in input file
		"""

		# create inputs directory
		inputs_dir = Util.create_inputs_dir()

		# download input file
		p = urlparse(url)
		staged_file = os.path.join(inputs_dir, os.path.basename(p.path))
		r = requests.get(url, stream=True, verify=False)
		r.raise_for_status()
		r.raw.decode_content = True
		with open(staged_file, "wb") as f:
			shutil.copyfileobj(r.raw, f)

		return staged_file

	@staticmethod
	def stage_in_s3(url: str, cred: dict = None) -> str:
		"""Stage in a file from an S3 URL.
		Args:
			url (str): S3 URL of input file
			unsigned (bool): send unsigned request
		Returns:
			str: relative path to the staged-in input file
		"""
		global localCachePath
		global only_prime_cache
		global restage_in

		# parse and handle various cases for local caching
		p = urlparse(url)
		userName=pwd.getpwuid(os.getuid())[0]
		urnCacheableFilePath=cacheable_path(url);

		if (urnCacheableFilePath[1:] == '/'):					  
			urnCacheableFilePath = urnCacheableFilePath[1:]

		if localCachePath:
			fullCacheableFilePath	= os.path.join(localCachePath, userName, "S3cache", urnCacheableFilePath)
			fullCacheableSysFilePath = os.path.join(localCachePath,		   "S3cache", urnCacheableFilePath)
   
		""" Set the location where we want to file to be downloaded
		"""

		inputs_dir = Util.create_inputs_dir() # create inputs directory
		staged_file = os.path.join(inputs_dir, os.path.basename(p.path))			

		# set up S3 client
		if cred is None:
			s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client('s3', **cred)

		if not localCachePath or not urnCacheableFilePath:															 
			# no local cache configured or can't turn path into a usable path or handle
			# default behavior, copy in the file to /input, just as done in the past
			print('urnCacheableFilePath', urnCacheableFilePath)
			print('ONE', p.netloc, p.path, staged_file)
			s3.download_file(p.netloc, p.path[1:], staged_file)
			return staged_file

		if not cache_hit(urnCacheableFilePath): 
			# has a path thats cacheable, but cache miss
			# We download into cache and potentially copy to /input
			
			if not lock_cache(fullCacheableFilePath, fullCacheableSysFilePath):
			   # we failed getting lock file, possibility that the file was successfully copied in by another process
			   # while we were trying to get a lock on the file
			   # so we look to see if the file exists
			   if cache_hit(urnCacheableFilePath) and only_prime_cache:
				   # nothing to do
				   return cache_hit(urnCacheableFilePath)
			   if cache_hit(urnCacheableFilePath):
				   # this is the primary path for a cache hit for full stage_in/process step
				   shutil.copy2(cache_hit(urnCacheableFilePath), staged_file, follow_symlinks=True) 
				   return staged_file 

			   # not in cache and unable to cache for unknown reason, so just pull the file as if cache didn't exist
			   print('TWO', p.netloc, p.path, staged_file)
			   s3.download_file(p.netloc, p.path[1:], staged_file)
			   return staged_file

			else:
				# I have the lockfile, so I'll delete any partial file (should be result of a failed transfer), from the cache and begin the download
				try:
					os.remove(fullCacheableFilePath+'.part')
				except: 
					True
				print('THREE', p.netloc, p.path, fullCacheableFilePath+'.part')
				s3.download_file(p.netloc, p.path[1:], fullCacheableFilePath+'.part')
				os.rename(fullCacheableFilePath+'.part', fullCacheableFilePath)
				# This is another area for specific local system customization. Depending on the file, we may not want some permissions controls
				os.symlink(fullCacheableFilePath, fullCacheableSysFilePath)
				os.remove(fullCacheableFilePath+'.lock')
				if only_prime_cache:
					return fullCacheableFilePath
				else:
					shutil.copy2(fullCacheableFilePath, staged_file, follow_symlinks=True) 
					return staged_file 

		if cache_hit(urnCacheableFilePath) and only_prime_cache:
			# nothing to do
			return cache_hit(urnCacheableFilePath)

		if cache_hit(urnCacheableFilePath):
			# this is the primary path for a cache hit for full stage_in/process step
			shutil.copy2(cache_hit(urnCacheableFilePath), staged_file, follow_symlinks=True) 
			return staged_file 

		# not in cache and unable to cache for unknown reason, just pull the file as if cache didn't exist
		print(p.netloc, p.path, staged_file)
		s3.download_file(p.netloc, p.path[1:], staged_file)
		return staged_file

	@staticmethod
	def stage_in_maap(
		collection_concept_id: str,
		readable_granule_name: str,
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

		# create inputs directory
		inputs_dir = Util.create_inputs_dir()

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
		staged_file = os.path.join(inputs_dir, os.path.basename(p.path))
		granule.getData(destpath=inputs_dir)

		return staged_file

	   
localCachePath = ""
only_prime_cache = False
restage_in = False

def main(argc: int, argv: list) -> int:

	"""
	   add arguments to implement the transparent caching functionality.
	   -s will only bring in remote cachable items. It will ignore items that are localally acessible and have litte cost.
		  The implication is those items will not have copies in the local cache.
		  
	   -C <path> : tells stage_in where the system cache is. This might better be in a config file per ADES. If stage_in is 
				   run without -s, then stage_in will look here for the requested item, if found, will then copy it from the 
				   cache into the /inputs directory. if not found in the cache, then will be pulled in from the remote location.
				   in either case, the item will be put into the local cache. 

	   After processing the extra arguments, argc and argv are returned to how they would have been set in previous releases 
	   of stage_in.py to eliminate changes in how the arguments are processed below.
	   argparse is good enough to even handle appended known args...

	   The three different processing scenarios should be handled correctly:
	   1) two pass processing: stage_in runs to prime the cache by explicitly calling stage_in just to prime the cache
	   2) cached one pass processing: stage_in will fill the cache, but in-line with processing. Subsequent run will have a sucessful hit
	   3) original one pass processing: no cache. stage_in is processed inline and subsequent runs will have to pull the file again
	"""

	# OK - this is lazy for now. Belongs in the staging_map Func calls
	global localCachePath
	global only_prime_cache
	global restage_in

	parser = argparse.ArgumentParser(prog='stage_in.py',
	  formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=100),
	  description = 'stage_in data to local cache, into an actual cwl process step, or both',
	  epilog = ' First implementation of 2-pass caching. Something to consider is a symlink being copied in instead of a file copy in.  \
				 This might reduce the use of tmpdir space (/tmp maps to /dev/shm on HECC), and the accompaniing memory usage. Another  \
				 approach would be to configure cwl to use an actual shared filesystem instead of the default /tmp, which may have unexpected \
				 memory consumption due to input/output file sizes.\n\n')

	parser.add_argument('-c', dest='localCachePath'  , metavar='<path>', action='store'	 , help='path to local cache directory') 
	parser.add_argument('-s', dest='only_prime_cache',				   action='store_true', help='prime cache by only staging to cache')
	parser.add_argument('-r', dest='restage_in'	  ,				   action='store_true', help='ignore any previously cached file')

	args , nargv = parser.parse_known_args()

	nargv = [argv[0]] + nargv[:]
	nargc = len(nargv)
	argv=nargv
	argc=nargc

	if only_prime_cache := args.only_prime_cache:
		print('only stage to cache')

	if ((localCachePath := args.localCachePath) != ""):
		print('path to local Cache: ', localCachePath)

	staging_type = argv[1]

	staging_map = {
		'HTTP': [StageIn.stage_in_http, {}],
		'S3_unsigned': [StageIn.stage_in_s3, {'cred': None}],
		'S3': [StageIn.stage_in_s3, {'cred': None}],
		'DAAC': [None, {}],
		'MAAP': [StageIn.stage_in_maap, {}],
		'Role': [None, {}],
		'Local': [None, {}]
	}

	func = staging_map[staging_type][0]
	params = staging_map[staging_type][1]

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
				inputs_dir = Util.create_inputs_dir()
				dst = os.path.join(inputs_dir, os.path.basename(path))
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

	dl_path = func(**params)
	print('Downloaded ({}): '.format(staging_type) + dl_path)

	return 0


if __name__ == '__main__':
	main(len(sys.argv), sys.argv)
