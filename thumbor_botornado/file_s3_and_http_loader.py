import os
import re

from tornado.concurrent import return_future

from thumbor.utils import logger

import thumbor_botornado.s3_loader as S3Loader
import thumbor.loaders.http_loader as HttpLoader
import thumbor.loaders.file_loader as FileLoader
from thumbor.loaders.http_loader import quote_url
from thumbor.loaders import LoaderResult

HTTP_RE = re.compile(r'\Ahttps?:', re.IGNORECASE)
S3_RE = re.compile(r'\Ahttps?://(?P<bucket>.*-melody).s3.amazonaws.com/(?P<path>.*)', re.IGNORECASE)


@return_future
def load(context, url, callback):
    url = quote_url(url)
    match = S3_RE.match(url)

    def callback_wrapper(result):
        if result.successful:
            # logger.warn('efs success: ' + match.group('path'))
            logger.warn('efs success: ' + url)
            callback(result)
        else:
            logger.warn('efs failed, try s3 with: ' + os.path.join(match.group('bucket').rstrip('/'), match.group('path').lstrip('/')))

            # If not on efs, try s3
            S3Loader.load(context,
                          os.path.join(match.group('bucket').rstrip('/'),
                                       match.group('path').lstrip('/')),
                          callback)

    # If melody s3 file, first try to load from efs
    if match:
        logger.warn('melody s3 file, first try to load from efs: ' + match.group('path'))
        FileLoader.load(context, match.group('path'), callback_wrapper)
        # logger.warn('first try to load from efs: ' + url)
        # FileLoader.load(context, url, callback_wrapper)
    # else get from the internet
    elif HTTP_RE.match(url):
        logger.warn('regular web url:' + url)
        HttpLoader.load(context, url, callback)
    else:
        logger.warn('not a url')
        result = LoaderResult()
        result.successful = False
        result.error = LoaderResult.ERROR_NOT_FOUND
        callback(result)
