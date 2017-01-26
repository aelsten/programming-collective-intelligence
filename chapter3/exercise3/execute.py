import samples.clusters as clusters

def euclidean_distance(v1, v2):
    sum_of_squares = sum([pow(v1[i] - v2[i], 2) for i in range(len(v1))])
    return 1 - (1 / (1 + sum_of_squares))

blog_names, words, data = clusters.read_file('samples/blogdata.txt')
clust = clusters.h_cluster(data, distance=euclidean_distance)
clusters.draw_dendrogram(clust, blog_names, jpeg='blog_clust.jpg')
