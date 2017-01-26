import csv


def get_data_from_file(filename):
    with open('data/hetrec2011-delicious-2k/' + filename) as file:
        reader = csv.reader(file, delimiter="\t")
        data = list(reader)
    return data[1:]


def get_bookmarks():
    return get_data_from_file('bookmarks.dat')


def get_user_tagged_bookmarks():
    return get_data_from_file('user_taggedbookmarks.dat')


def get_tags():
    return get_data_from_file('tags.dat')


def get_tags_as_dictionary():
    return {x[0]: x[1] for x in get_tags()}


def get_tag_id_for_tag(tag):
    for tag_row in get_tags():
        if tag_row[1] == tag:
            return tag_row[0]
    raise ValueError('Tag does not exist')


def get_bookmark_urls_as_dictionary():
    return {x[0]: x[3] for x in get_bookmarks()}


def get_bookmark_ids_for_popular_bookmarks(tag, count=5):
    tag_id = get_tag_id_for_tag(tag)
    user_bookmarks_for_tag = filter(lambda x: x[2] == tag_id, get_user_tagged_bookmarks())
    bookmarks_with_counts = {x[1]: 0 for x in user_bookmarks_for_tag}
    for bookmark in user_bookmarks_for_tag:
        bookmarks_with_counts[bookmark[1]] += 1
    sorted_bookmarks_with_counts = []
    for b in sorted(bookmarks_with_counts, key=bookmarks_with_counts.get, reverse=True)[0:count]:
        sorted_bookmarks_with_counts.append(b)
    return sorted_bookmarks_with_counts


def get_user_dict_from_popular(tag, count=5):
    bookmark_ids = get_bookmark_ids_for_popular_bookmarks(tag, count=count)
    bookmark_urls = get_bookmark_urls_as_dictionary()
    bookmarks = filter(lambda x: x[1] in bookmark_ids, get_user_tagged_bookmarks())
    user_dict = {x[0]: {} for x in bookmarks}
    all_items = {}
    for user in user_dict:
        for post in filter(lambda x: x[0] == user, bookmarks):
            url = bookmark_urls[post[1]]
            user_dict[user][url] = 1.0
            all_items[url] = 1

    # Fill in missing items with 0
    for ratings in user_dict.values():
        for item in all_items:
            if item not in ratings:
                ratings[item] = 0.0

    return user_dict


def get_bookmark_tags():
    return get_data_from_file('bookmark_tags.dat')


def get_tags_with_bookmarks():
    bookmark_tags = get_bookmark_tags()
    bookmark_urls = get_bookmark_urls_as_dictionary()
    tags = get_tags_as_dictionary()
    tags_dict = {}
    all_items = {}
    for bookmark_tag in bookmark_tags:
        tag = tags[bookmark_tag[1]]
        if tag not in tags_dict:
            tags_dict[tag] = {}
        url = bookmark_urls[bookmark_tag[0]]
        tags_dict[tag][url] = 1.0
        all_items[url] = 1

    return tags_dict
