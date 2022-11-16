import os
import sys
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
		self.message = '{t}: Not enough arguments (recieved {c}, expected {e})'
		self.message = message.format(t=staging_type, c=(received-1), e=expected)


class Util:
	@staticmethod
	def create_inputs_dir(inputs_dir: str = os.path.join(os.getcwd(), "inputs")) -> str:
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
	def stage_in_s3(url: str, unsigned: bool = False) -> str:
		"""Stage in a file from an S3 URL.
		Args:
			url (str): S3 URL of input file
			unsigned (bool): send unsigned request
		Returns:
			str: relative path to the staged-in input file
		"""

		# create inputs directory
		inputs_dir = Util.create_inputs_dir()

		# download input file
		p = urlparse(url)
		staged_file = os.path.join(inputs_dir, os.path.basename(p.path))
		if unsigned:
			s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
		else:
			s3 = boto3.client("s3")
		s3.download_file(p.netloc, p.path[1:], staged_file)

		return staged_file

	@staticmethod
	def stage_in_maap(
		collection_concept_id: str,
		readable_granule_name: str,
		# TODO: remove these commented parameters if there is no need for them
		# user_token: str,
		# application_token: str,
		maap_host: str = "api.ops.maap-project.org",
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


def main(argc: int, argv: list) -> int:
	staging_type = argv[1]

	staging_map = {
		'HTTP': [StageIn.stage_in_http, {}],
		'S3_unsigned': [StageIn.stage_in_s3, {'unsigned': True}],
		'S3': [StageIn.stage_in_s3, {'unsigned': False}],
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

			# Create the AWS credential file with the necessary secrets
			aws_prefix = os.path.join(os.path.expanduser('~'), '.aws')
			with open(os.path.join(aws_prefix, 'credentials'), 'w') as f:
				content = '[default]'
				content += '\naws_access_key_id = ' + argv[3]
				content += '\naws_secret_access_key = ' + argv[4]
				content += '\naws_session_token = ' + argv[5]
				f.write(content)
			# Append the region to the AWS config
			with open(os.path.join(aws_prefix, 'config'), 'w') as f:
				content = '[default]'
				content += '\nregion = ' + argv[6]
				f.write(content)
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
