import os
import urllib
import urllib2
import json
import pprint
import requests

from ..base import DatasetEngineABC
from ..utilities import encode_multipart


class CKANDatasetEngine(DatasetEngineABC):
    """
    Definition for CKAN Dataset Engine objects.
    """

    @property
    def type(self):
        """
        CKAN type
        """
        return 'CKAN'

    # api_key = '8cd924a8-48d3-452a-becf-d5ec19e9801b' ##TODO: Stop hardcoding API key
    api_key = '003654e6-cd89-46a6-9035-28e4037b44d6'

    def _execute_api_request(self, method, data, headers=None, error=None, console=False):
        """
        Execute the REST API method on the API endpoint.

        Args:
          method (string): Name of the REST API method to execute (e.g.: 'resource_show')
          data (dict): Dictionary of key-value arguments to the method.
          error (dict, optional): Override the default error messages. (e.g. {'500': 'Custom 500 message'})
          console (bool, optional): Print resulting JSON to the console for debugging. Defaults to False.

        Returns:
          dict: Response JSON object parsed into a dictionary.
        """
        # Construct the method url
        method_url = '{0}/{1}'.format(self.api_endpoint, method)

        # Execute the method
        request = urllib2.Request(method_url)
        # request.add_header('Authorization', '003654e6-cd89-46a6-9035-28e4037b44d6')
        request.add_header('Authorization', '8cd924a8-48d3-452a-becf-d5ec19e9801b')
        request.add_header('X-CKAN-API-Key', '8cd924a8-48d3-452a-becf-d5ec19e9801b')

        # Add other headers
        if headers and isinstance(headers, dict):
            for name, value in headers.iteritems():
                request.add_header(name, value)

        # Create the data string from dictionary passed
        if isinstance(data, dict):
            # In the case of JSON parameters provided via dictionary
            data_string = urllib.quote(json.dumps(data))
        else:
            # In the case of multipart form data
            data_string = urllib.quote(data)

        request.add_data(data_string)

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            if e.code == 400:
                if error and error['400']:
                    print(error['400'])
                else:
                    print(e)
                return None

            elif e.code == 404:
                if error and error['404']:
                    print(error['404'])
                else:
                    print('HTTP ERROR 404: The dataset service could not be found at {0}. Please check the API endpoint '
                          'and try again.'.format(self.api_endpoint))
                return None

            elif e.code == 409:
                if error and error['409']:
                    print(error['409'])
                else:
                    print(e)
                return None

            elif e.code == 500:
                if error and error['500']:
                    print(error['500'])
                else:
                    print('HTTP ERROR 500: The dataset service experienced an internal error. Ensure that the service is'
                          'functioning properly, and try again.')
                return None

            else:
                print(e)
                return None

        if response.code != 200:
            print('WARNING: Server responded with code {0}. Check that the dataset service is running and try ' \
                  'again.'.format(response.code))
            return None

        # Convert CKAN response JSON into dictionary
        response_dict = json.loads(response.read())

        if console:
            pprint.pprint(response_dict)

        return response_dict

    def _prepare_request(self, method, data_dict=None, api_key=None, files=None):
        """
        Preprocess the parameters for CKAN API call. This is derived from CKAN's api client which can be found here:
        https://github.com/ckan/ckanapi/tree/master/ckanapi/common.py

        Args:
            method (string):
            data_dict (dict, optional):
            api_key (string, optional):
            files (dict)

        Returns:
            tuple: url, data_dict, headers
        """
        if not data_dict:
            data_dict = {}

        headers = {}

        if files:
            data_dict = dict((k.encode('utf-8'), v.encode('utf-8'))
                for (k, v) in data_dict.items())
        else:
            data_dict = json.dumps(data_dict).encode('ascii')
            headers['Content-Type'] = 'application/json'

        if api_key:
            api_key = str(api_key)
        else:
            api_key = str(self.api_key)

        headers['X-CKAN-API-Key'] = api_key
        headers['Authorization'] = api_key

        url = '/'.join((self.api_endpoint.rstrip('/'), method))

        return url, data_dict, headers

    @staticmethod
    def _execute_request(url, data, headers, files=None):
        """
        Execute the request using the requests module. See: https://github.com/ckan/ckanapi/tree/master/ckanapi/common.py

        Args:
          url (string):
          data (dict):
          headers (dict):
          files (dict):

        Returns:
          tuple: status_code, response
        """
        r = requests.post(url, data=data, headers=headers, files=files)
        return r.status_code, r.text

    @staticmethod
    def _parse_response(url, status, response, console=False):
        """
        Parse the response and check for errors.

        Args:
          url (string):
          status (string):
          response (string):
          console (bool, optional):

        Returns:
          dict: response parsed into a dictionary or raises appropriate error.
        """
        try:
            parsed = json.loads(response)
            if console:
                if hasattr(parsed, 'get'):
                    if parsed.get('success'):
                        pprint.pprint(parsed)
                    else:
                        print('ERROR: {0}'.format(parsed['error']['message']))
            return parsed
        except:
            print('Status Code {0}: {1}'.format(status, response))
            return None

    def search_datasets(self, query, console=False, **kwargs):
        """
        Search CKAN datasets that match a query.

        Wrapper for the CKAN search_datasets API method. See the CKAN API docs for this methods to see applicable
        options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs

        # Assemble the query parameters
        query_terms = []

        if len(query.keys()) > 1:
            for key, value in query.iteritems():
                query_terms.append('{0}:{1}'.format(key, value))
        else:
            for key, value in query.iteritems():
                query_terms = '{0}:{1}'.format(key, value)

        data['q'] = query_terms

        # Special error
        error_409 = 'HTTP ERROR 409: Ensure query fields are valid and try again.'

        # Execute
        url, data, headers = self._prepare_request(method='package_search', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def search_resources(self, query, console=False, **kwargs):
        """
        Search CKAN resources that match a query.

        Wrapper for the CKAN search_resources API method. See the CKAN API docs for this methods to see applicable
        options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          query (dict): Key value pairs representing field and values to search for.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs

        # Assemble the query parameters
        query_terms = []
        if len(query.keys()) > 1:
            for key, value in query.iteritems():
                query_terms.append('{0}:{1}'.format(key, value))
        else:
            for key, value in query.iteritems():
                query_terms = '{0}:{1}'.format(key, value)

        data['query'] = query_terms

        # Special error
        error_409 = 'HTTP ERROR 409: Ensure query fields are valid and try again.'

        # Execute
        url, data, headers = self._prepare_request(method='resource_search', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def list_datasets(self, console=False, with_resources=False, **kwargs):
        """
        List CKAN datasets.

        Wrapper for the CKAN package_list and current_package_list_with_resources API methods. See the CKAN API docs for
        these methods to see applicable options (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          with_resources (bool, optional): Return a list of dataset dictionaries. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          list: A list of dataset names or a list of dataset dictionaries if with_resources is true.
        """
        # Execute API Method
        if not with_resources:
            url, data, headers = self._prepare_request(method='package_list', data_dict=kwargs)
            status, response = self._execute_request(url=url, data=data, headers=headers)

        else:
            url, data, headers = self._prepare_request(method='current_package_list_with_resources', data_dict=kwargs)
            status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def get_dataset(self, dataset_id, console=False, **kwargs):
        """
        Retrieve CKAN dataset

        Wrapper for the CKAN package_show API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Error message
        error_404 = 'HTTP ERROR 404: The dataset could not be found. Check that the id or name provided is valid ' \
                    'and that the dataset service at {0} is running properly, then try again.'.format(self.api_endpoint)

        # Execute
        url, data, headers = self._prepare_request(method='package_show', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def get_resource(self, resource_id, console=False, **kwargs):
        """
        Retrieve CKAN resource

        Wrapper for the CKAN resource_show API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble data dictionary
        data = kwargs
        data['id'] = resource_id

        # Error message
        error_404 = 'HTTP ERROR 404: The resource could not be found. Check that the id provided is valid ' \
                    'and that the dataset service at {0} is running properly, then try again.'.format(self.api_endpoint)

        # Execute
        url, data, headers = self._prepare_request(method='resource_show', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def create_dataset(self, name, console=False, **kwargs):
        """
        Create CKAN dataset

        Wrapper for the CKAN package_create API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          name (string): The id or name of the resource to retrieve.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['name'] = name

        # Execute
        url, data, headers = self._prepare_request(method='package_create', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def create_resource(self, dataset_id, path='', console=False, **kwargs):
        """
        Create CKAN resource

        Wrapper for the CKAN resource_create API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to to which the resource will be added.
          path (string, optional): Absolute path to a file to upload for the resource.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data_dict = kwargs
        data_dict['package_id'] = dataset_id

        # Prepare file
        files = None

        if path and os.path.isfile(path):
            files = {'file': (os.path.basename(path), open(path, 'rb'))}

        url, data, headers = self._prepare_request(method='resource_create',
                                                   data_dict=data_dict,
                                                   files=files)

        status, response = self._execute_request(url=url, data=data, headers=headers, files=files)

        print(status, response)

    def update_dataset(self, dataset_id, console=False, **kwargs):
        """
        Update CKAN dataset

        Wrapper for the CKAN package_update API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to update.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Preserve the resources and tags if not included in parameters
        """
        Note: The default behavior of 'package_update' is to replace the resources and tags attributes with empty
              lists if they are not included in the parameter list... This is suboptimal, because the resources become
              disassociated with the dataset and float off into the ether. This behavior is modified in this method so
              that these properties are retained by default, unless included in the parameters that are being updated.
        """
        result = self._execute_api_request(method='package_show',
                                           data=data)
        if result['success']:
            original_dataset = result['result']

            if 'resources' not in data:
                data['resources'] = original_dataset['resources']

            if 'tags' not in data:
                data['tags'] = original_dataset['tags']


        # Execute
        url, data, headers = self._prepare_request(method='package_update', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def update_resource(self, resource_id, console=False, **kwargs):
        """
        Update CKAN resource

        Wrapper for the CKAN resource_update API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource to update.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = resource_id

        # Execute
        result = self._execute_api_request(method='resource_update',
                                           data=data,
                                           console=console)

        # TODO: The resource_update method only returns a 409 error... address this after resource_create is working. Possible cause is that other parameters may be required...

        return result

    def delete_dataset(self, dataset_id, console=False, **kwargs):
        """
        Delete CKAN dataset

        Wrapper for the CKAN package_delete API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          dataset_id (string): The id or name of the dataset to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = dataset_id

        # Execute
        url, data, headers = self._prepare_request(method='package_delete', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)

    def delete_resource(self, resource_id, console=False, **kwargs):
        """
        Delete CKAN resource

        Wrapper for the CKAN resource_delete API method. See the CKAN API docs for this method to see applicable options
        (http://docs.ckan.org/en/ckan-2.2/api.html).

        Args:
          resource_id (string): The id of the resource to delete.
          console (bool, optional): Pretty print the result to the console for debugging. Defaults to False.
          **kwargs: Any number of optional keyword arguments for the method (see CKAN docs).

        Returns:
          The response dictionary or None if an error occurs.
        """
        # Assemble the data dictionary
        data = kwargs
        data['id'] = resource_id

        # Execute
        url, data, headers = self._prepare_request(method='resource_delete', data_dict=data)
        status, response = self._execute_request(url=url, data=data, headers=headers)

        return self._parse_response(url, status, response, console)
