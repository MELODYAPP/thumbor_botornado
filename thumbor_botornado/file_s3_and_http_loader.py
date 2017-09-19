import os
from tornado.concurrent import return_future
import thumbor_botornado.s3_loader as S3Loader
import thumbor.loaders.http_loader as HttpLoader
import thumbor.loaders.file_loader as FileLoader
import re

HTTP_RE = re.compile(r'\Ahttps?:', re.IGNORECASE)
S3_RE = re.compile(r'\Ahttps?://(?P<bucket>.*-melody).s3.amazonaws.com/(?P<path>.*)', re.IGNORECASE)


@return_future
def load(context, url, callback):
    match = S3_RE.match(url)

    def callback_wrapper(result):
        if result.successful:
            callback(result)
        else:
            # If not on efs, try s3
            S3Loader.load(context,
                          os.path.join(match.group('bucket'), match.group('path')),
                          callback)

    # If melody s3 file, first try to load from efs
    if match:
        FileLoader.load(context, match.group('path'), callback_wrapper)
    # else get from the internet
    else:
        HttpLoader.load(context, url, callback)
