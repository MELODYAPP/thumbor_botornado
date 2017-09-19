import os
from tornado.concurrent import return_future
from thumbor.utils import logger
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
            logger.warn('efs success: ' + match.group('path'))
            callback(result)
        else:
            logger.warn('efs failed: ' + os.path.join(match.group('bucket').rstrip('/'), match.group('path').lstrip('/')))

            # If not on efs, try s3
            S3Loader.load(context,
                          os.path.join(match.group('bucket').rstrip('/'),
                                       match.group('path').lstrip('/')),
                          callback)

    # If melody s3 file, first try to load from efs
    if match:
        logger.warn('melody s3 file, first try to load from efs: ' + match.group('path'))
        FileLoader.load(context, match.group('path'), callback_wrapper)
    # else get from the internet
    else:
        logger.warn('http:' + url)
        HttpLoader.load(context, url, callback)
