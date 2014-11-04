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

    def _execute_api_request(self, method, data_dictionary, console=False, error_404=None):
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
                if error_404:
                    print(error_404)
                else:
                    print('HTTP ERROR 404: The dataset service could not be found at {0}. Please check the API endpoint '
                          'and try again.'.format(self.api_endpoint))
                return None

            if e.code == 500:
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

    def search(self, console=False, **kwargs):
        """
        Search CKAN datasets given query.
        :return:
        """

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
          id (string): The id or name of the dataset to retrieve.
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
                                           error_404=error_404)

        return result

    def get_resource(self, console=False, **kwargs):
        """
        Retrieve CKAN resource
        :return:
        """

    def create_dataset(self, console=False, **kwargs):
        """
        Create CKAN dataset
        :return:
        """

    def create_resource(self, console=False, *kwargs):
        """
        Create CKAN resource
        :return:
        """

    def update_dataset(self, console=False, **kwargs):
        """
        Update CKAN dataset
        :return:
        """

    def update_resource(self, console=False, **kwargs):
        """
        Update CKAN resource
        :return:
        """

    def delete_dataset(self, console=False, **kwargs):
        """
        Delete CKAN dataset
        :return:
        """

    def delete_resource(self, console=False, **kwargs):
        """
        Delete CKAN resource
        :return:
        """
