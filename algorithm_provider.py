""" Generic functions to used as parameters for the kmeans algorithm.
"""
import uuid


def random_sampling(data, number_of_clusters):
    """Takes a sample of the objects to define initial centroids.
    Args:
        data (rdd): the rdd containing all the objects.
        number_of_clusters (int): number of needed clusters.

    Returns:
        (list): list with number_of_clusters centroids.
    """
    centroids = data.takeSample(False, number_of_clusters)

    for centroid in centroids:
        centroid.unique_id = uuid.uuid4()
    return centroids


def match_object_to_centroid(item, centroids, compute_distance):
    """ Match an object to its closest centroid
    Args:
        item (object): the item that needs to be match to a centroid
        centroids (List(object)): the set of centroids to match to the item
        compute_distance (function): the function used to compute the distance
    Returns:
        (str): The Id of the centroid which is closest to the traveler
    """

    distance_list = [(compute_distance(traveler, centroid),
                      centroid.unique_id)
                     for centroid in centroids]
    distance_list.sort(key=lambda tup: tup[0])

    return distance_list[0][1]
