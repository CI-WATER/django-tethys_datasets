from abc import ABCMeta, abstractmethod, abstractproperty


class DatasetEngineABC:
    __metaclass__ = ABCMeta

    @property
    def api_endpoint(self):
        """
        API Endpoint (e.g.: www.example.com/api)
        """
        return self._api_endpoint

    @property
    def apikey(self):
        """
        API Endpoint (e.g.: www.example.com/api)
        """
        return self._apikey

    @property
    def username(self):
        """
        API Endpoint (e.g.: www.example.com/api)
        """
        return self._username

    @property
    def password(self):
        """
        API Endpoint (e.g.: www.example.com/api)
        """
        return self._password

    @abstractproperty
    def type(self):
        """
        Returns the type of dataset engine.
        """
        return NotImplemented

    def __init__(self, api_endpoint, apikey=None, username=None, password=None):
        """
        Constructor for Dataset Engines.
        """
        self._api_endpoint = api_endpoint

        if apikey:
            self._apikey = apikey

        if username:
            self._username = username

        if password:
            self._password = password

    def __repr__(self):
        """
        Representation of object for debugging purposes.
        :return:
        """
        return '<DatasetEngine type={0} endpoint={1}>'.format(self.type, self.api_endpoint)

    @abstractmethod
    def search_datasets(self, query, **kwargs):
        """
        This method is used to search for datasets that match a set of query terms.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def search_resources(self, query, **kwargs):
        """
        This method is used to search for resources that match a set of query terms.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def list_datasets(self, **kwargs):
        """
        This method is used to return a list names of datasets. Optionally, this method can be used to return a list of
        dataset objects.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def get_dataset(self, **kwargs):
        """
        This method is used to retrieve a dataset object for the given id.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def get_resource(self, **kwargs):
        """
        This method is used to retrieve a resource object for the given id and dataset.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def create_dataset(self, **kwargs):
        """
        This method is used to create a new dataset.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def create_resource(self, **kwargs):
        """
        This method is used to create/add a new resource to an existing dataset.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def update_dataset(self, **kwargs):
        """
        This method is used to update existing dataset with the given id.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def update_resource(self, **kwargs):
        """
        This method is used to update existing resource with the given id.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def delete_dataset(self, **kwargs):
        """
        This method is used to delete the dataset with the given id.
        :return:
        """
        return NotImplemented

    @abstractmethod
    def delete_resource(self, **kwargs):
        """
        This method is used to delete the resource with the given id and dataset.
        :return:
        """
        return NotImplemented