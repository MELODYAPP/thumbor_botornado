from tornado.concurrent import return_future
import thumbor_botornado.s3_loader as S3Loader
import thumbor.loaders.http_loader as HttpLoader
import re

HTTP_RE = re.compile(r'\Ahttps?:', re.IGNORECASE)

@return_future
def load(context, url, callback):
    if HTTP_RE.match(url):
        HttpLoader.load(context, url, callback)
    else:
        S3Loader.load(context, url, callback)
