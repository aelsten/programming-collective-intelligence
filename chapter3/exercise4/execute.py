import samples.clusters as clusters

def manhattan_distance(v1, v2):
    sum_of_abs_differences = sum([abs(v1[i] - v2[i]) for i in range(len(v1))])
    return 1 - (1 / sum_of_abs_differences)

wants, people, data = clusters.read_file('samples/zebo.txt')
clust = clusters.h_cluster(data, distance=manhattan_distance)
clusters.draw_dendrogram(clust, wants)
