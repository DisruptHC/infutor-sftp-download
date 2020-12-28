import os
import logging
import typing
from os.path import join, dirname

from stat import S_ISDIR

from paramiko import SFTPClient, SFTPFile, Message, SFTPError, Transport
from paramiko.sftp import CMD_STATUS, CMD_READ, CMD_DATA

class _SFTPFileDownloader:
    """
    Helper class to download large file with paramiko sftp client with limited number of concurrent requests.
    """
    _DOWNLOAD_MAX_REQUESTS = 48
    _DOWNLOAD_MAX_CHUNK_SIZE = 0x8000

    def __init__(self, f_in: SFTPFile, f_out: typing.BinaryIO, callback=None):
        self.f_in = f_in
        self.f_out = f_out
        self.callback = callback

        self.requested_chunks = {}
        self.received_chunks = {}
        self.saved_exception = None

    def download(self):
        file_size = self.f_in.stat().st_size
        requested_size = 0
        received_size = 0

        while True:
            # send read requests
            while len(self.requested_chunks) + len(self.received_chunks) < self._DOWNLOAD_MAX_REQUESTS and \
                    requested_size < file_size:
                chunk_size = min(self._DOWNLOAD_MAX_CHUNK_SIZE, file_size - requested_size)
                request_id = self._sftp_async_read_request(
                    fileobj=self,
                    file_handle=self.f_in.handle,
                    offset=requested_size,
                    size=chunk_size
                )
                self.requested_chunks[request_id] = (requested_size, chunk_size)
                requested_size += chunk_size

            # receive blocks if they are available
            # note: the _async_response is invoked
            self.f_in.sftp._read_response()
            self._check_exception()

            # write received data to output stream
            while True:
                chunk = self.received_chunks.pop(received_size, None)
                if chunk is None:
                    break
                _, chunk_size, chunk_data = chunk
                self.f_out.write(chunk_data)
                if self.callback is not None:
                    self.callback(chunk_data)

                received_size += chunk_size

            # check transfer status
            if received_size >= file_size:
                break

            # check chunks queues
            if not self.requested_chunks and len(self.received_chunks) >= self._DOWNLOAD_MAX_REQUESTS:
                raise ValueError("SFTP communication error. The queue with requested file chunks is empty and"
                                 "the received chunks queue is full and cannot be consumed.")

        return received_size

    def _sftp_async_read_request(self, fileobj, file_handle, offset, size):
        sftp_client = self.f_in.sftp

        with sftp_client._lock:
            num = sftp_client.request_number

            msg = Message()
            msg.add_int(num)
            msg.add_string(file_handle)
            msg.add_int64(offset)
            msg.add_int(size)

            sftp_client._expecting[num] = fileobj
            sftp_client.request_number += 1

        sftp_client._send_packet(CMD_READ, msg)
        return num

    def _async_response(self, t, msg, num):
        if t == CMD_STATUS:
            # save exception and re-raise it on next file operation
            try:
                self.f_in.sftp._convert_status(msg)
            except Exception as e:
                self.saved_exception = e
            return
        if t != CMD_DATA:
            raise SFTPError("Expected data")
        data = msg.get_string()

        chunk_data = self.requested_chunks.pop(num, None)
        if chunk_data is None:
            return

        # save chunk
        offset, size = chunk_data

        if size != len(data):
            raise SFTPError(f"Invalid data block size. Expected {size} bytes, but it has {len(data)} size")
        self.received_chunks[offset] = (offset, size, data)

    def _check_exception(self):
        """if there's a saved exception, raise & clear it"""
        if self.saved_exception is not None:
            x = self.saved_exception
            self.saved_exception = None
            raise x

class SFTP:
    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
    def _list_files_r(self, sftp, root):
        files = []
        for path in sftp.listdir_attr(root):
            if path.filename.startswith("."):
                continue
            if S_ISDIR(path.st_mode):
                for filename in self._list_files_r(sftp, os.path.join(root, path.filename)):
                    files.append(path.filename + "/" + filename)
            else:
                files.append(path.filename)
        return files
    def list_files(self):
        transport = Transport((self.hostname, self.port))
        transport.set_keepalive(30)
        transport.connect(None, self.username, self.password)
        sftp = SFTPClient.from_transport(transport)
        return self._list_files_r(sftp, ".")
    def get(self, src, dst, callback=None):
        """
        Helper function to download remote file via sftp.
        It contains a fix for a bug that prevents a large file downloading with :meth:`paramiko.SFTPClient.get`
        """
        transport = Transport((self.hostname, self.port))
        transport.set_keepalive(30)
        transport.connect(None, self.username, self.password)
        sftp = SFTPClient.from_transport(transport)

        remote_file_size = sftp.stat(src).st_size

        try:
            sftp.get(src,dst)
        except:
            with sftp.open(src, 'rb') as f_in, open(dst, 'wb') as f_out:
                logging.info(f"Getting './{src}' to '{dst}'...")
                _SFTPFileDownloader(
                    f_in=f_in,
                    f_out=f_out,
                    callback=callback
                ).download()

        local_file_size = os.path.getsize(dst)
        if remote_file_size != local_file_size:
            raise IOError(f"file size mismatch: {remote_file_size} != {local_file_size}")
