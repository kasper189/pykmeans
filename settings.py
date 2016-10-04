""" Common Clustering settings.
"""


class ClusteringSetting(object):
    """ClusteringSetting settings container class

    Attributes:
        number_of_clusters (int): the number of clusters.
        max_iterations (int): the maximum number of iterations allowed.
        stop_variance (float): the minimum distance between centroids
          computed during two consecutive iterations. If the loop_variance
          is smaller then this value, then the algorithm stops.
        distance_function (function): function to assign a cluster to a traveler
    """
    def __init__(self, number_of_clusters, max_iterations,
                 stop_variance, distance_function):
        self.number_of_clusters = number_of_clusters
        self.max_iterations = max_iterations
        self.stop_variance = stop_variance
        self.distance_function = distance_function
