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
            callback(result)
        else:
            logger.debug('s3 {0}'.format(
                os.path.join(match.group('bucket').rstrip('/'), match.group('path').lstrip('/')))
            )

            # If not on efs, try s3
            S3Loader.load(context,
                          os.path.join(match.group('bucket').rstrip('/'),
                                       match.group('path').lstrip('/')),
                          callback)

    # If melody s3 file, first try to load from efs
    if match:
        logger.debug('efs {0}'.format(match.group('path')))

        # TEMP try s3 direct
        S3Loader.load(context,
                      os.path.join(match.group('bucket').rstrip('/'),
                                   match.group('path').lstrip('/')),
                      callback)

        # FileLoader.load(context, match.group('path'), callback_wrapper)
    # else get from the internet
    elif HTTP_RE.match(url):
        logger.debug('web {0}'.format(url))
        HttpLoader.load(context, url, callback)
    else:
        logger.debug('not a url')
        result = LoaderResult()
        result.successful = False
        result.error = LoaderResult.ERROR_NOT_FOUND
        callback(result)
