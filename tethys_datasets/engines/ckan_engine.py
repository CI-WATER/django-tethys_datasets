import urllib
import urllib2
import json
import pprint

from ..base import DatasetEngineABC


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

    def _execute_api_request(self, method, data_dictionary, console=False, error=None):
        """
        Will return the result dictionary or None if an error occurs.
        """
        # Create the data string from dictionary passed
        data_string = urllib.quote(json.dumps(data_dictionary))

        # Construct the method url
        method_url = '{0}/{1}'.format(self.api_endpoint, method)

        # Execute the method
        request = urllib2.Request(method_url)
        request.add_header('Authorization', '003654e6-cd89-46a6-9035-28e4037b44d6')

        try:
            response = urllib2.urlopen(request, data_string)
        except urllib2.HTTPError as e:
            if e.code == 404:
                if error and error['404']:
                    print(error['404'])
                else:
                    print('HTTP ERROR 404: The dataset service could not be found at {0}. Please check the API endpoint '
                          'and try again.'.format(self.api_endpoint))
                return None

            elif e.code == 500:
                if error and error['500']:
                    print(error['500'])
                else:
                    print('HTTP ERROR 500: The dataset service experienced an internal error. Ensure that the service is'
                          'functioning properly, and try again.')
                return None

            elif e.code == 409:
                if error and error['409']:
                    print(error['409'])
                else:
                    print(e)
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
        result = self._execute_api_request(method='package_search',
                                           data_dictionary=data,
                                           console=console,
                                           error={'409': error_409})
        return result

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
        result = self._execute_api_request(method='resource_search',
                                           data_dictionary=data,
                                           console=console,
                                           error={'409': error_409})

        return result


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
          The response dictionary or None if an error occurs.
        """
        # Execute API Method
        if not with_resources:
            result = self._execute_api_request(method='package_list',
                                               data_dictionary=kwargs,
                                               console=console)
        else:
            result = self._execute_api_request(method='current_package_list_with_resources',
                                               data_dictionary=kwargs,
                                               console=console)
        return result

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
        result = self._execute_api_request(method='package_show',
                                           data_dictionary=data,
                                           console=console,
                                           error={'404': error_404})
        return result

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
        result = self._execute_api_request(method='resource_show',
                                           data_dictionary=data,
                                           console=console,
                                           error={'404': error_404})
        return result

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
        result = self._execute_api_request(method='package_create',
                                           data_dictionary=data,
                                           console=console,)
        return result

    def create_resource(self, dataset, url, console=False, *kwargs):
        """
        Create CKAN resource
        """
        # TODO: Implement this method fully to allow file uploads.

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
        Note: The default behavior of the package_update is to replace the resources and tags attributes with empty
              lists if they are not included in the parameter list... This is suboptimal, because the resources become
              disassociated with the dataset and float off into the ether. This behavior is modified in this method so
              that these properties are retained by default, unless included in the parameters that are being updated.
        """
        result = self._execute_api_request(method='package_show',
                                           data_dictionary=data)
        if result['success']:
            original_dataset = result['result']

            if 'resources' not in data:
                data['resources'] = original_dataset['resources']

            if 'tags' not in data:
                data['tags'] = original_dataset['tags']


        # Execute
        result = self._execute_api_request(method='package_update',
                                           data_dictionary=data,
                                           console=console)

        return result

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
                                           data_dictionary=data,
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
        result = self._execute_api_request(method='package_delete',
                                           data_dictionary=data,
                                           console=console)
        return result

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
        result = self._execute_api_request(method='resource_delete',
                                           data_dictionary=data,
                                           console=console)
        return result
