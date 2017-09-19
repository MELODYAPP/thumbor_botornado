import os
from tornado.concurrent import return_future
import thumbor_botornado.s3_loader
import thumbor.loaders.http_loader
import thumbor.loaders.file_loader
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
            s3_loader.load(context,
                           os.path.join(match.group('bucket'), match.group('path')),
                           callback)

    # If melody s3 file, first try to load from efs
    if match:
        file_loader.load(context, match.group('path'), callback_wrapper)
    # else get from the internet
    else:
        http_loader.load(context, url, callback)
