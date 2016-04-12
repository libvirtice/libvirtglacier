import datetime
import hashlib
import logging
import os
from itertools import zip_longest
from socket import gethostname

import boto3
from streams import BZ2Stream, AESEncryptor

# from config import ACCESS_KEY_ID, SECRET_ACCESS_KEY, PASSPHRASE, AWS_REGION, COMPRESSION_LEVEL, LOG_FILE
# logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(filename=LOG_FILE, level=logging.INFO)


class AmazonChunkReader:
    chunk_size = 1024 * 1024  # Amazon specific (don't change)
    chunks_per_part = 32  # Must be a power of 2
    # TODO: automatic part_size choosing (log2 etc)
    part_size = chunk_size * chunks_per_part
    # max_size = part_size * 10000

    def __init__(self, stream):
        self.stream = stream
        self.digests = []

    def get_chunks(self):
        for chunk in iter(lambda: self.stream.read(self.chunk_size), b''):
            yield chunk

    def get_parts(self):
        """
        :param chunks_per_part: power of two integer, defining the part size in chunks (megabytes)
        :return: yields tuple of checksum, part
        """
        for chunks in zip_longest(*[iter(self.get_chunks())] * self.chunks_per_part, fillvalue=b''):
            tree_hash = self._calculate_hash_tree([hashlib.sha256(chunk).digest() for chunk in chunks if chunk])
            self.digests.append(tree_hash)
            yield tree_hash.hex(), b''.join(chunks)

    @staticmethod
    def _calculate_hash_tree(chunks):
        if not chunks:
            return hashlib.sha256(b'').digest()
        else:
            while len(chunks) > 1:
                chunks = [hashlib.sha256(a + b).digest() if b else a for a, b in zip_longest(*[iter(chunks)] * 2)]
            return chunks[0]

    def calculate_hash_tree(self):
        return self._calculate_hash_tree(self.digests).hex()


class Glacier:
    def __init__(self, access_key_id, secret_access_key, region_name=None):
        if region_name is None:
            region_name = 'us-east-1'

        self.client = boto3.client('glacier', region_name=region_name, aws_access_key_id=access_key_id,
                                   aws_secret_access_key=secret_access_key, use_ssl=False)

    def create_vault(self, name=None):
        if name is None:
            name = "%s_%s" % (datetime.date.today() - datetime.timedelta(days=1), gethostname())
        vault = self.client.create_vault(vaultName=name)
        logging.info("Vault created: %s", vault["location"])
        return name

    def upload(self, path, vault=None, tag=None, passphrase=None, compression_level=None):
        # TODO: multithreading?
        if vault is None:
            vault = self.create_vault()

        stream = open(path, mode='rb')

        tags = [os.path.basename(path)]

        if tag:
            tags.append(tag)

        if compression_level:
            stream = BZ2Stream(stream, compression_level=compression_level)
            tags.append(stream.tag)

        if passphrase:
            stream = AESEncryptor(stream, passphrase)
            tags.append(stream.tag)

        description = '|'.join(reversed(tags))

        stream = AmazonChunkReader(stream)

        upload = self.client.initiate_multipart_upload(vaultName=vault, archiveDescription=description,
                                                       partSize=str(AmazonChunkReader.part_size))
        logging.info("Initiating multipart upload (%s) for %s: %s", upload['uploadId'], path, upload['location'])
        logging.info(description)
        pos = 0
        for checksum, part in stream.get_parts():
            length = len(part)
            range = "bytes %d-%d/*" % (pos, pos + length - 1)
            attempts = 3
            while attempts:
                try:
                    logging.info("Uploading part: %s", range)
                    response = self.client.upload_multipart_part(vaultName=vault, uploadId=upload['uploadId'],
                                                                 range=range, body=part, checksum=checksum)
                    break
                except Exception as e:
                    attempts -= 1
                    logging.error(str(e))
            else:
                logging.critical("Giving up on %s", path)
                return False

            pos += length

        response = self.client.complete_multipart_upload(vaultName=vault, uploadId=upload['uploadId'], archiveSize=str(pos),
                                                     checksum=stream.calculate_hash_tree())
        logging.info(response)
        return response


# if __name__ == '__main__':
#     glacier = Glacier(access_key_id=ACCESS_KEY_ID, secret_access_key=SECRET_ACCESS_KEY, region_name=AWS_REGION)
#
#     # glacier.upload(path)
#     # glacier.upload(path, compression_level=COMPRESSION_LEVEL)
#     # glacier.upload(path, passphrase=PASSPHRASE)
#     # glacier.upload(path, compression_level=COMPRESSION_LEVEL, passphrase=PASSPHRASE)
#
#     result = glacier.upload(path, compression_level=COMPRESSION_LEVEL, passphrase=PASSPHRASE)
#     logging.debug(result)
