import samples.clusters as clusters
import delicious

users, bookmarks, data = delicious.get_data()
users = users[:100]
data = data[:100]
print('retrieved users=', len(users), ', bookmarks=', len(bookmarks), ', and data=', len(data))
clust = clusters.h_cluster(data, distance=clusters.tanimoto)
print('calculated the clusters')
clusters.draw_dendrogram(clust, users, jpeg='user_cluster.jpg')
