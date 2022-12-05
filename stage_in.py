import os
import sys
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


class StageIn:
	@staticmethod
	def stage_in_http(url: str, dest: str) -> str:
		"""Stage in a file from a HTTP/HTTPS URL.
		Args:
			url (str): HTTP/HTTPS URL of input file
		Returns:
			str: relative path to the staged-in input file
		"""

		# create inputs directory
		inputs_dir = Util.create_dest(dest)

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
	def stage_in_s3(url: str, dest: str, cred: dict = None) -> str:
		"""Stage in a file from an S3 URL.
		Args:
			url (str): S3 URL of input file
			unsigned (bool): send unsigned request
		Returns:
			str: relative path to the staged-in input file
		"""

		# create inputs directory
		inputs_dir = Util.create_dest(dest)

		# download input file
		p = urlparse(url)
		staged_file = os.path.join(inputs_dir, os.path.basename(p.path))
		if cred is None:
			s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client('s3', **cred)
		s3.download_file(p.netloc, p.path[1:], staged_file)

		return staged_file

	@staticmethod
	def stage_in_maap(
		collection_concept_id: str,
		readable_granule_name: str,
		dest: str,
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
		inputs_dir = Util.create_dest(dest)

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


def main(argc: int, argv: list) -> int:
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
	cache_dir = inputs_json.get('cache_dir')
	dest_dir = os.path.join(os.getcwd(), 'inputs') if cache_dir is None else cache_dir['path']

	# Use a dictionary to determine which execution branch to use
	staging_map = {
		'HTTP': StageIn.stage_in_http,
		'S3_unsigned': StageIn.stage_in_s3,
		'S3': StageIn.stage_in_s3,
		'DAAC': None,
		'MAAP': StageIn.stage_in_maap,
		'Role': None,
		'Local': None,
	}

	func = staging_map[staging_type]
	params = {'dest': dest_dir}

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
			params['url'] =  Util.to_list(input_path['url'])
			params['username'] = input_path['username']
			params['password'] = input_path['password']
			extra_param_list = [{}]
		elif staging_type == 'MAAP':
			params['collection_concept_id'] = input_path['collection_concept_id']
			params['readable_granule_name'] = input_path['readable_granule_name']
			extra_param_list = [{}]
		elif staging_type == 'Role':
			params['role_arn'] = input_path['role_arn']
			params['source_profile'] = input_path['source_profile']
			extra_param_list = [{}]
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

	# Loop over the list based parameters and merge them with the base credentials in params
	for i, extra_params in enumerate(extra_param_list):
		# Download the file to the initial destination directory
		params['dest'] = os.path.join(dest_dir, str(i))
		joined_params = Util.merge_dict(params, extra_params)
		dl_path = func(**joined_params)

		# Rename the file to preserve the list order in future containers
		renamed = '{}_{}'.format(str(i), os.path.basename(dl_path))
		renamed = os.path.join(os.path.dirname(dl_path), renamed)
		os.rename(dl_path, renamed)
		logger.info('Downloaded ({}): '.format(staging_type) + renamed)

	return 0


if __name__ == '__main__':
	main(len(sys.argv), sys.argv)
