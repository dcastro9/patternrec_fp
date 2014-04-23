# Copyright 2014, MIT License.
# Author: Daniel Castro <dcastro9@gatech.edu>

from random import shuffle
import numpy as np

class WTAHasher(object):
    """ Processes a dataset, and converts it to a rank correlation dataset
    based on a given k, and a random set of permutations.

    Attributes:
        k: Subset of the original vector.
        num_permutations: Number of permutations (this determines the dimensions
       					 of your new data vector).
		data: An array of data (each value has equal dimensions)
    """

    def __init__(self, k, num_permutations, data):
        self._k_value = k
        self._num_perm = num_permutations
        self._dataset = data
        self._permutations = []
        for val in range(num_permutations):
            indices = range(len(self._dataset[0]) - 1)
            shuffle(indices)
            self._permutations.append(indices[:k])

            

    def hashDataset(self):
        hashedArray = []
        for data_point in self._dataset:
            generated_hash = []
            for perm in self._permutations:
                generated_hash.append(self.__getHashCode(data_point[:-1], perm))
            generated_hash.append(data_point[-1])

            hashedArray.append(generated_hash)
        return hashedArray

    def __getHashCode(self, data_point, permutation):
        temp_array = []
        for index in permutation:
            temp_array.append(data_point[index])
        return np.argmax(temp_array)