""" K-Means implementation using Spark.
"""

def compute_cluster(data, settings, init_function,
                    centroid_matcher, update_function, merge_function):
    """Computes k clusters applying k-means algorithm.

    Args:
        data (rdd): the RDD containing the objects.
        settings (ClusteringSetting): object containing algorithm settings
        init_function (function): initialization function to determine the
            initial centroids.
        centroid_matcher (function): function assign an object to the
            nearest cluster
        update_function (function): function to rebuild a centroid based on
            objects belonging to it.
        merge_function (function): function merge two lists.

    Returns:
        labeled_data (rdd): the rdd containing a map <cluster_id, object>.
        centroids (list): list of built centroids.
    """
    centroids = init_function(data, settings.number_of_clusters)
    iterations = 0
    loop_variance = 0

    while (iterations < settings.max_iterations and
           loop_variance < settings.stop_variance):
        labeled_data = data.map(
            lambda item: (centroid_matcher(item, centroids),
                              item)
        )

        computed_centroids = (
            labeled_data.aggregateByKey(
                [],
                (lambda a, b: merge_function(a, b)),
                (lambda a, b: merge_function(a, b)))
            .mapValues(
                lambda items: update_function(items))
            .collectAsMap()
        )

        computed_centroids = computed_centroids.values()

        loop_variance = _compare_centroids(settings.number_of_clusters,
                                           centroids,
                                           computed_centroids,
                                           settings.distance_function)

        centroids = computed_centroids
        iterations += 1

    return labeled_data, centroids


def _compare_centroids(number_of_clusters, last_centroids,
                       previous_centroids, distance_function):
    """Computes k clusters applying k-means algorithm.

    Args:
        number_of_clusters (int): the number of clusters.
        last_centroids (rdd): the RDD containing the last iteration centroids.
        previous_centroids (rdd): the RDD containing the previous
            iteration centroids.
        distance_function (function): function to assign a cluster to an item.

    Returns:
        distance (float): the distance between the centroids
    """
    distance = 0
    if len(last_centroids) == len(previous_centroids):
        for i in xrange(number_of_clusters):
            distance += distance_function(last_centroids[i],
                                          previous_centroids[i])

    return distance
