from __future__ import print_function

import mimetypes
import random
import string

_BOUNDARY_CHARS = string.digits + string.ascii_letters


def encode_multipart(fields, files, boundary=None, console=False):
    r"""
    Encode dict of form fields and dict of files as multipart/form-data. This recipe is derived from Ben Hoyt's function
    found here: http://code.activestate.com/recipes/578668-encode-multipart-form-data-for-uploading-files-via/

    Args:
        fields (dict): The keys represent the fields and the values are the values of the fields::

            fields = {'name': 'John Doe', 'age': 26}

        files (dict): Each value of the files dict is a dictionary with required keys 'filename' and 'content' and an
                      optional key of 'mimetype'. If mimetype is not specified, it will try to guess the mime type or it
                      will use 'application/octet-stream'::

            files = {'file1': {'filename': 'gages.txt', 'content': 'GAGE DATA HERE'},
                     'file2': {'filename': 'stations.txt', 'content': 'STATION DATA HERE'}}

        boundary (string, optional): The string to use as the boundary delimiter of the multipart form data. Defaults to
                                     a string of randomly selected characters.

    Returns:
        tuple: (body_string, headers_dict)

    Example::

        >>> body, headers = encode_multipart({'FIELD': 'VALUE'},
        ...                                  {'FILE': {'filename': 'F.TXT', 'content': 'CONTENT'}},
        ...                                  boundary='BOUNDARY')
        >>> print('\n'.join(repr(l) for l in body.split('\r\n')))
        '--BOUNDARY'
        'Content-Disposition: form-data; name="FIELD"'
        ''
        'VALUE'
        '--BOUNDARY'
        'Content-Disposition: form-data; name="FILE"; filename="F.TXT"'
        'Content-Type: text/plain'
        ''
        'CONTENT'
        '--BOUNDARY--'
        ''
        >>> print(sorted(headers.items()))
        [('Content-Length', '193'), ('Content-Type', 'multipart/form-data; boundary=BOUNDARY')]
        >>> len(body)
        193
    """
    def escape_quote(s):
        return s.replace('"', '\\"')

    if boundary is None:
        boundary = ''.join(random.choice(_BOUNDARY_CHARS) for i in range(30))
    lines = []

    for name, value in fields.items():
        lines.extend((
            '--{0}'.format(boundary),
            'Content-Disposition: form-data; name="{0}"'.format(escape_quote(name)),
            '',
            str(value),
        ))

    for name, value in files.items():
        filename = value['filename']
        if 'mimetype' in value:
            mimetype = value['mimetype']
        else:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        lines.extend((
            '--{0}'.format(boundary),
            'Content-Disposition: form-data; name="{0}"; filename="{1}"'.format(
                    escape_quote(name), escape_quote(filename)),
            'Content-Type: {0}'.format(mimetype),
            '',
            value['content'],
        ))

    lines.extend((
        '--{0}--'.format(boundary),
        '',
    ))

    if console:
        print('\n'.join(repr(line) for line in lines))

    body = '\r\n'.join(lines)

    headers = {
        'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary),
        'Content-Length': str(len(body)),
    }

    return body, headers
