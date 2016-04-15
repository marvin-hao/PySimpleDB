__author__ = 'Marvin'


class Index:
    """
    This interface contains methods to traverse an index.
    """
    def before_first(self, search_key):
        """
        Positions the index before the first record
        having the specified search key.
        :param search_key: the search key value.
        """
        raise NotImplementedError()

    def next(self):
        """
        Moves the index to the next record having the
        search key specified in the beforeFirst method.
        Returns false if there are no more such index records.
        :return: false if no other index records have the search key.
        """
        raise NotImplementedError()

    def get_data_rid(self):
        """
        Returns the dataRID value stored in the current index record.
        :return: the dataRID stored in the current index record.
        """
        raise NotImplementedError()

    def insert(self, data_val, data_rid):
        """
        Inserts an index record having the specified
        data_val and data_rid values.
        :param data_val: the data_val in the new index record.
        :param data_rid: the data_rid in the new index record.
        """
        raise NotImplementedError()

    def delete(self, data_val, data_rid):
        """
        Deletes the index record having the specified
        data_val and data_rid values.
        :param data_val: the data_val of the deleted index record
        :param data_rid: the data_rid of the deleted index record
        """
        raise NotImplementedError()

    def close(self):
        """
        Closes the index.
        """
        raise NotImplementedError()