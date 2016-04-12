import bz2
import os
from io import RawIOBase

import cryptography.hazmat
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import algorithms, modes, Cipher
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class BZ2Stream(RawIOBase):
    # TODO: subclass BufferedReader?
    tag = 'bz2'

    def __init__(self, stream, compression_level=9):
        assert isinstance(compression_level, int)
        assert 1 <= compression_level <= 9
        self.stream = stream
        self.EOF = False
        self.compressor = bz2.BZ2Compressor(compression_level)
        self.buffer = b''  # TODO: bytearray would be more efficient?
        self.pos = 0

    def read(self, *args, **kwargs):
        if args:
            (length,) = args
        else:
            length = False
        while not self.EOF and length and len(self.buffer) < length:
            buffer = self.stream.read(*args, **kwargs)
            if buffer == b'':
                self.EOF = True
                self.buffer = self.compressor.flush()
            else:
                self.buffer = self.compressor.compress(buffer)

        if length:
            output = self.buffer[:length]
            self.buffer = self.buffer[length:]
        else:
            output = self.buffer
            self.buffer = b''
        self.pos += len(output)
        return output

    def tell(self, *args, **kwargs):
        return self.pos


class AESEncryptor(RawIOBase):
    key_size = algorithms.AES.block_size * 2
    mode = modes.CBC

    def __init__(self, stream, passphrase):
        assert isinstance(passphrase, bytes)
        self.stream = stream
        self.iv = os.urandom(algorithms.AES.block_size // 8)  # 128 bit / 16 bytes
        self.tag = 'aes:' + self.iv.hex()
        self.key = PBKDF2HMAC(algorithm=hashes.SHA256(),
                              length=self.key_size // 8,  # 256 bit / 32 bytes
                              salt=self.iv,
                              iterations=1337,
                              backend=cryptography.hazmat.backends.default_backend()).derive(passphrase)
        self.encryptor = Cipher(algorithms.AES(self.key), self.mode(self.iv),
                                backend=cryptography.hazmat.backends.default_backend()).encryptor()
        self.padder = padding.PKCS7(algorithms.AES.block_size).padder()
        self.buffer = b''
        self.EOF = False

    def read(self, *args, **kwargs):
        if args:
            (length,) = args
        else:
            length = False
        while not self.EOF and (not length or len(self.buffer) < length):
            plaintext = self.stream.read(*args, **kwargs)
            if plaintext == b'':
                self.EOF = True
                self.buffer += self.encryptor.update(self.padder.finalize())
                self.buffer += self.encryptor.finalize()  # this shouldn't do anything for AES...
            else:
                self.buffer += self.encryptor.update(self.padder.update(plaintext))

        if length:
            output = self.buffer[:length]
            self.buffer = self.buffer[length:]
        else:
            output = self.buffer
            self.buffer = b''
        return output

    def seek(self, offset, *args, **kwargs):
        raise Exception('Seek not supported with CBC mode...')