import csv


def get_data_from_file(filename):
    with open('data/hetrec2011-delicious-2k/' + filename, encoding = "ISO-8859-1") as file:
        reader = csv.reader(file, delimiter="\t")
        data = list(reader)
    return data[1:]


def get_bookmarks():
    return get_data_from_file('bookmarks.dat')

def get_data():
    bookmarks_per_user = get_data_from_file('user_taggedbookmarks.dat')

    col_names = []
    col_id_dictionary = {}
    col = 0
    for bookmark in get_bookmarks():
        col_names.append(bookmark[3])
        col_id_dictionary[bookmark[0]] = col
        col += 1
    len_col_names = len(col_names)

    data = []
    row_names = []
    row = -1
    current_user = None
    for user_bookmark in bookmarks_per_user:
        if not user_bookmark[0] == current_user:
            row_names.append(user_bookmark[0])
            data.append([float(0) for x in range(len_col_names)])
            current_user = user_bookmark[0]
            row += 1
        data[row][col_id_dictionary[user_bookmark[1]]] = 1

    return row_names, col_names, data
