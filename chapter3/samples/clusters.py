from PIL import Image, ImageDraw
from math import sqrt
import random


def read_file(filename):
    lines = [line for line in file(filename)]

    # First line is the column titles
    colnames = lines[0].strip().split('\t')[1:]
    rownames = []
    data = []
    for line in lines[1:]:
        p = line.strip().split('\t')
        # First column in each row is the rowname
        rownames.append(p[0])
        # The data for this row is the remainder of the row
        data.append([float(x) for x in p[1:]])
    return rownames, colnames, data


def pearson(v1, v2):
    # Simple sums
    sum1 = sum(v1)
    sum2 = sum(v2)

    # Sums of the squares
    sum1Sq = sum([pow(v, 2) for v in v1])
    sum2Sq = sum([pow(v, 2) for v in v2])

    # Sum of the products
    pSum = sum([v1[i] * v2[i] for i in range(len(v1))])

    # Calculate r (Pearson score)
    num = pSum - (sum1 * sum2 / len(v1))
    den = sqrt((sum1Sq - pow(sum1, 2) / len(v1)) * (sum2Sq - pow(sum2, 2) / len(v1)))
    if den == 0:
        return 0

    return 1.0 - num / den


class BiCluster:

    def __init__(self, vec, left=None, right=None, distance=0.0, id=None):
        self.left = left
        self.right = right
        self.vec = vec
        self.id = id
        self.distance = distance


def h_cluster(rows, distance=pearson):
    distances = {}
    current_cluster_id = -1

    # Clusters are initially just the rows
    cluster = [BiCluster(rows[i], id=i) for i in range(len(rows))]

    while len(cluster) > 1:
        lowest_pair = (0, 1)
        closest = distance(cluster[0].vec, cluster[1].vec)

        # loop through every pair looking for the smallest distance
        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                # distances is the cache of distance calculations
                if (cluster[i].id, cluster[j].id) not in distances:
                    distances[(cluster[i].id, cluster[j].id)] = distance(
                        cluster[i].vec, cluster[j].vec)

                d = distances[(cluster[i].id, cluster[j].id)]

                if d < closest:
                    closest = d
                    lowest_pair = (i, j)

        # calculate the average of the two clusters
        mergevec = [
            (cluster[lowest_pair[0]].vec[i] + cluster[lowest_pair[1]].vec[i]) / 2.0
            for i in range(len(cluster[0].vec))]

        # create the new cluster
        newcluster = BiCluster(mergevec, left=cluster[lowest_pair[0]],
                               right=cluster[lowest_pair[1]],
                               distance=closest, id=current_cluster_id)

        # cluster ids that weren't in the original set are negative
        current_cluster_id -= 1
        del cluster[lowest_pair[1]]
        del cluster[lowest_pair[0]]
        cluster.append(newcluster)

    return cluster[0]


def print_cluster(cluster, labels=None, n=0):
    # indent to make a hierarchy layout
    for i in range(n):
        print(' ')
    if cluster.id < 0:
        # negative id means that this is branch
        print('-')
    else:
        # positive id means that this is an endpoint
        if labels is None:
            print(cluster.id)
        else:
            print(labels[cluster.id])

    # now print the right and left branches
    if cluster.left is not None:
        print_cluster(cluster.left, labels=labels, n=n + 1)
    if cluster.right is not None:
        print_cluster(cluster.right, labels=labels, n=n + 1)


def get_height(clust):
    # Is this an endpoint? Then the height is just 1
    if clust.left is None and clust.right is None:
        return 1

    # Otherwise the height is the same of the heights of
    # each branch
    return get_height(clust.left) + get_height(clust.right)


def get_depth(clust):
    # The distance of an endpoint is 0.0
    if clust.left is None and clust.right is None:
        return 0

    # The distance of a branch is the greater of its two sides
    # plus its own distance
    return max(get_depth(clust.left), get_depth(clust.right)) + clust.distance


def draw_dendrogram(clust, labels, jpeg='clusters.jpg'):
    # height and width
    h = get_height(clust) * 20
    w = 1200
    depth = get_depth(clust)

    # width is fixed, so scale distances accordingly
    scaling = float(w - 150) / depth

    # Create a new image with a white background
    img = Image.new('RGB', (w, h), (255, 255, 255))
    draw = ImageDraw.Draw(img)

    draw.line((0, h / 2, 10, h / 2), fill=(255, 0, 0))

    # Draw the first node
    draw_node(draw, clust, 10, (h / 2), scaling, labels)
    img.save(jpeg, 'JPEG')


def draw_node(draw, clust, x, y, scaling, labels):
    if clust.id < 0:
        h1 = get_height(clust.left) * 20
        h2 = get_height(clust.right) * 20
        top = y - (h1 + h2) / 2
        bottom = y + (h1 + h2) / 2
        # Line length
        ll = clust.distance * scaling
        # Vertical line from this cluster to children
        draw.line((x, top + h1 / 2, x, bottom - h2 / 2), fill=(255, 0, 0))

        # Horizontal line to left item
        draw.line((x, top + h1 / 2, x + ll, top + h1 / 2), fill=(255, 0, 0))

        # Horizontal line to right item
        draw.line((x, bottom - h2 / 2, x + ll,
                   bottom - h2 / 2), fill=(255, 0, 0))

        # Call the function to draw the left and right nodes
        draw_node(draw, clust.left, x + ll, top + h1 / 2, scaling, labels)
        draw_node(draw, clust.right, x + ll, bottom - h2 / 2, scaling, labels)
    else:
        # If this is an endpoint, draw the item label
        draw.text((x + 5, y - 7), labels[clust.id], (0, 0, 0))


def rotate_matrix(data):
    newdata = []
    for i in range(len(data[0])):
        newrow = [data[j][i] for j in range(len(data))]
        newdata.append(newrow)
    return newdata


def k_cluster(rows, distance=pearson, k=4):
    # Determine the minimum and maximum values for each point
    ranges = [(min([row[i] for row in rows]), max([row[i] for row in rows]))
              for i in range(len(rows[0]))]

    # Create k randomly placed centroids
    clusters = [[random.random() * (ranges[i][1] - ranges[i][0]) + ranges[i][0]
                 for i in range(len(rows[0]))] for j in range(k)]

    lastmatches = None
    for t in range(100):
        print('Iteration %d' % t)
        bestmatches = [[] for i in range(k)]

        # Find which centroid is the closest for each row
        for j in range(len(rows)):
            row = rows[j]
            bestmatch = 0
            for i in range(k):
                d = distance(clusters[i], row)
                if d < distance(clusters[bestmatch], row):
                    bestmatch = i
            bestmatches[bestmatch].append(j)

        # If the results are the same as last time, this is complete
        if bestmatches == lastmatches:
            break
        lastmatches = bestmatches

        # Move the centroids to the average of their members
        for i in range(k):
            avgs = [0.0] * len(rows[0])
            if len(bestmatches[i]) > 0:
                for rowid in bestmatches[i]:
                    for m in range(len(rows[rowid])):
                        avgs[m] += rows[rowid][m]
                for j in range(len(avgs)):
                    avgs[j] /= len(bestmatches[i])
                clusters[i] = avgs

    return bestmatches


def tanimoto(v1, v2):
    c1, c2, shr = 0, 0, 0

    for i in range(len(v1)):
        if v1[i] != 0:
            c1 += 1  # in v1
        if v2[i] != 0:
            c2 += 1  # in v2
        if v1[i] != 0 and v2[i] != 0:
            shr += 1  # in both

    return 1.0 - (float(shr) / (c1 + c2 - shr))


def scale_down(data, distance=pearson, rate=0.01):
    n = len(data)

    # The real distances between every pair of items
    realdist = [[distance(data[i], data[j]) for j in range(n)]
                for i in range(0, n)]

    # Randomly initialize the starting points of the locations in 2D
    loc = [[random.random(), random.random()] for i in range(n)]
    fakedist = [[0.0 for j in range(n)] for i in range(n)]

    lasterror = None
    for m in range(0, 1000):
        # Find projected distances
        for i in range(n):
            for j in range(n):
                fakedist[i][j] = sqrt(sum([pow(loc[i][x] - loc[j][x], 2)
                                           for x in range(len(loc[i]))]))

        # Move points
        grad = [[0.0, 0.0] for i in range(n)]

        totalerror = 0
        for k in range(n):
            for j in range(n):
                if j == k:
                    continue
                # The error is percent difference between the distances
                errorterm = (fakedist[j][k] - realdist[j][k]) / realdist[j][k]

                # Each point needs to be moved away from or towards the other
                # point in proportion to how much error it has
                grad[k][0] += ((loc[k][0] - loc[j][0]) /
                               fakedist[j][k]) * errorterm
                grad[k][1] += ((loc[k][1] - loc[j][1]) /
                               fakedist[j][k]) * errorterm

                # Keep track of the total error
                totalerror += abs(errorterm)
        print(totalerror)

        # If the answer got worse by moving the points, we are done
        if lasterror and lasterror < totalerror:
            break
        lasterror = totalerror

        # Move each of the points by the learning rate times the gradient
        for k in range(n):
            loc[k][0] -= rate * grad[k][0]
            loc[k][1] -= rate * grad[k][1]

    return loc


def draw_2d(data, labels, jpeg='mds2d.jpg'):
    img = Image.new('RGB', (2000, 2000), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    for i in range(len(data)):
        x = (data[i][0] + 0.5) * 1000
        y = (data[i][1] + 0.5) * 1000
        draw.text((x, y), labels[i], (0, 0, 0))
    img.save(jpeg, 'JPEG')
