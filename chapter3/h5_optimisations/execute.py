import csv
import h5py
from math import sqrt
import scipy
from scipy import cluster

class BookmarksContainer:
    def __init__(self):
        self.bookmarks = []
        self.bookmark_to_col_dictionary = {}
        col = 0
        for bookmark in BookmarksContainer.get_data_from_file('bookmarks.dat'):
            self.bookmarks.append(bookmark[3])
            self.bookmark_to_col_dictionary[bookmark[0]] = col
            col += 1

    def get_bookmarks(self):
        return self.bookmarks

    def get_col_id_for_bookmark(self, bookmark):
        return self.bookmark_to_col_dictionary[bookmark]

    @staticmethod
    def get_data_from_file(filename):
        with open('data/hetrec2011-delicious-2k/' + filename, encoding = "ISO-8859-1") as f:
            reader = csv.reader(f, delimiter="\t")
            data = list(reader)
        return data[1:]

def write_users_with_bookmarks_to_hdf5():
    bookmarks_container = BookmarksContainer()
    h5_file = h5py.File("delicious.hdf5", "w")
    dataset = h5_file.create_dataset("users", (2000, len(bookmarks_container.get_bookmarks())), dtype="uint8", fillvalue=0)
    users = []
    current_user = None
    current_user_index = -1
    with open('data/hetrec2011-delicious-2k/user_taggedbookmarks.dat', encoding = "ISO-8859-1") as f:
        is_first_line = True
        for line in f:
            if is_first_line:
                is_first_line = False
            else:
                values = line.split("\t")
                if values[0] != current_user:
                    current_user = values[0]
                    current_user_index += 1
                    users.append(current_user)
                dataset[current_user_index, bookmarks_container.get_col_id_for_bookmark(values[1])] = 1
    dataset.attrs["users"] = [a.encode("utf8") for a in users]

def read_from_users_with_bookmarks_from_hdf5():
    delicious_file = h5py.File("delicious.hdf5", "r")
    users_with_bookmarks = delicious_file["."]["users"]
    return scipy.cluster.hierarchy.linkage(users_with_bookmarks)


def pearson(v1, v2):
    return 1 - scipy.stats.pearsonr(v1, v2)[0]
