import logging
import boto3
from botocore.exceptions import ClientError
import os
import sys
from urllib.parse import urlparse


class AWS:
	def __init__(self, key: str, secret: str, token: str, region: str):
		self.session = boto3.Session(
			aws_access_key_id=key,
			aws_secret_access_key=secret,
			aws_session_token=token,
			region_name=region
		)
		self.client = self.session.client('s3')

	def upload_file(self, file_name: str, bucket: str, object_name=None):
		"""Upload a file to an S3 bucket

		:param file_name: File to upload
		:param bucket: Bucket to upload to
		:param object_name: S3 object name. If not specified then file_name is used
		:return: True if file was uploaded, else False
		"""

		# If S3 object_name was not specified, use file_name
		if object_name is None:
			object_name = os.path.basename(file_name)

		# Upload the file
		try:
			response = self.client.upload_file(file_name, bucket, object_name)
		except ClientError as e:
			logging.error(e)
			return False
		return True

	def upload_dir(self, dirname: str, bucket: str, path: str):
		"""Recursively uploads a directory to an S3 bucket."""
		if not os.path.isdir(dirname):
			print('{} is not a directory!'.format(dirname))

		if not dirname.endswith('/'):
			dirname += '/'
		prefix_len = len(dirname)
		for parent, dirs, filenames in os.walk(dirname):
			for fname in filenames:
				obj_name = os.path.join(path, fname)
				if len(parent) > prefix_len:
					obj_name = os.path.join(parent[prefix_len:], obj_name)
				obj_name = obj_name.lstrip('/')
				self.upload_file(os.path.join(parent, fname), bucket, obj_name)


def main(argc, argv):
	expected_args = 8
	if argc != expected_args:
		print('Stage out script is being used incorrectly, {} arguments expected.'.format(expected_args))
		return 1

	if not os.path.exists(argv[7]):
		print('Output notebook is missing as the last argument!')
		return 1

	s3_url = argv[1]
	key = argv[2]
	secret = argv[3]
	token = argv[4]
	region = argv[5]

	parsed_url = urlparse(s3_url)
	bucket = parsed_url.netloc
	path = parsed_url.path


	uploader = AWS(key, secret, token, region)
	uploader.upload_dir(argv[6], bucket, path)
	uploader.upload_file(argv[7], bucket, os.path.join(path, os.path.basename(argv[7])).lstrip('/'))

	return 0


if __name__ == '__main__':
	main(len(sys.argv), sys.argv)
