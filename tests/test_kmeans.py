""" K-Means algorith test.
"""
import json
import os

import pickle
import unittest2
import findspark
import cloudpickle
from pysparkling import Context
import pyspark  # pylint: disable=import-error

import kmeans as kmeans
import settings as settings


class AlgorithmProvider(object):
    """Class to provide all the needed algorithms to clustering algorithm.
    """
    count = -1

    @staticmethod
    def random_sampling(data, number_of_clusters):
        """Takes a sample of the objects to define initial centroids.
        Args:
            data (rdd): the rdd containing all the objects.
            number_of_clusters (int): number of needed clusters.

        Returns:
            (list): list with number_of_clusters centroids.
        """
        return data.takeSample(number_of_clusters)

    @staticmethod
    def random_distance(item, centroid):
        """Returns a random distance item-centroid.
        Args:
            item (rdd): an object.
            centroid (int): a centroid.

        Returns:
            (int): the distance between the item and the centroid.
        """
        from random import randint
        return randint(0, 2)

    @staticmethod
    def random_cluster(item, centroids):
        """Returns a cluster id being sure to always return all the possible
            cluster id.
        Args:
            item (rdd): an object.
            centroids (list): a list of centroids.

        Returns:
            (int): the cluster id.
        """
        if AlgorithmProvider.count == 1:
            AlgorithmProvider.count = -1
        AlgorithmProvider.count += 1
        return AlgorithmProvider.count

    @staticmethod
    def dummy_update(items):
        """Dummy centroid update function.
        Args:
            items: a list of objects.

        Returns:
            (centroid): the generated centroid.
        """
        return 1


class KmeansTest(unittest2.TestCase):
    """Test class for the kmeans algorith.
    """

    def setUp(self):
        """Initializes a spark context. Findspark using the SPARK_HOME env
            variable.
        """
        findspark.init()
        self.context = pyspark.SparkContext(appName='TestKmeans')

    def tearDown(self):
        """Stops the Spark Context
        """
        self.context.stop()

    def test_algorith_excution(self):
        """Tests the algorithm execution with basic parameters
        """
        folder_path = os.path.dirname(os.path.realpath(__file__))
        json_path = os.path.join(folder_path, 'data', 'objects.json')

        dataset = Context(
            serializer=cloudpickle.dumps,
            deserializer=pickle.loads,
        ).textFile(json_path).map(json.loads)

        dataset.persist()

        number_of_cluster = 2

        algorithm_settings = settings.ClusteringSetting(
            number_of_cluster, 2, 0.01,
            AlgorithmProvider.random_distance
        )

        labeled, centroids = kmeans.compute_cluster(
            dataset, algorithm_settings,
            AlgorithmProvider.random_sampling,
            AlgorithmProvider.random_cluster,
            AlgorithmProvider.dummy_update
        )

        labeled.collect()
        self.assertEqual(10, labeled.count())

        first_item = labeled.first()
        self.assertTrue(0 <= first_item[0] < 2)
        self.assertEqual(number_of_cluster, len(centroids))

