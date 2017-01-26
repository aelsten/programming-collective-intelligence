import urllib.request
from bs4 import *
from urllib.parse import urljoin
from sqlite3 import dbapi2 as sqlite
import nn
import re
mynet = nn.searchnet('nn.db')

# Create a list of words to ignore
ignorewords = {'the': 1, 'of': 1, 'to': 1,
               'and': 1, 'a': 1, 'in': 1, 'is': 1, 'it': 1}


class crawler:
    # Initialize the crawler with the name of database
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    # Auxilliary function for getting an entry id and adding
    # it if it's not present
    def get_entry_id(self, table, field, value, createnew=True):
        cur = self.con.execute(
            "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    # Index an individual page
    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return
        print('Indexing ' + url)

        # Get the individual words
        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # Get the URL id
        urlid = self.get_entry_id('urllist', 'url', url)

        # Link each word to this url
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.get_entry_id('wordlist', 'word', word)
            self.con.execute(
                "insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid, wordid, i))

    # Extract the text from an HTML page (no tags)
    def get_text_only(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.get_text_only(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    # Seperate the words by any non-whitespace character
    def separate_words(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # Return true if this url is already indexed
    def is_indexed(self, url):
        u=self.con.execute \
            ("select rowid from urllist where url='%s'" % url).fetchone()
        if u!=None:
            # Check if it has actually been crawled
            v=self.con.execute( 'select * from wordlocation where urlid=%d' % u[0]).fetchone()
            if v!=None:
                return True
        return False

    # Add a link between two pages
    def add_link_ref(self, urlFrom, urlTo, linkText):
        words = self.separate_words(linkText)
        fromid = self.get_entry_id('urllist', 'url', urlFrom)
        toid = self.get_entry_id('urllist', 'url', urlTo)
        if fromid == toid:
            return
        cur = self.con.execute(
            "insert into link(fromid,toid) values (%d,%d)" % (fromid, toid))
        linkid = cur.lastrowid
        for word in words:
            if word in ignorewords:
                continue
            wordid = self.get_entry_id('wordlist', 'word', word)
            self.con.execute(
                "insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid, wordid))

    # Starting with a list of pages, do a breadth
    # first search to the given depth, indexing pages
    # as we go
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = {}
            for page in pages:
                try:
                    c = urllib.request.urlopen(page)
                except:
                    print("Could not open %s" % page)
                    continue
                try:
                    soup = BeautifulSoup(c.read(), "html.parser")
                    self.add_to_index(page, soup)

                    links = soup('a')
                    for link in links:
                        if ('href' in dict(link.attrs)):
                            url = urljoin(page, link['href'])
                            if url.find("'") != -1:
                                continue
                            url = url.split('#')[0]  # remove location portion
                            if url[0:4] == 'http' and not self.is_indexed(url):
                                newpages[url] = 1
                            linkText = self.get_text_only(link)
                            self.add_link_ref(page, url, linkText)

                    self.db_commit()
                except:
                    print("Could not parse page %s" % page)

            pages = newpages

    # Create the database tables
    def create_index_tables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.db_commit()

    def calculate_page_rank(self, iterations=20):
        # clear out the current page rank tables
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key,score)')

        # initialize every url with a page rank of 1
        for (urlid,) in self.con.execute('select rowid from urllist'):
            self.con.execute(
                'insert into pagerank(urlid,score) values (%d,1.0)' % urlid)
        self.db_commit()

        for i in range(iterations):
            print("Iteration %d" % (i))
            for (urlid,) in self.con.execute('select rowid from urllist'):
                pr = 0.15

                # Loop through all the pages that link to this one
                for (linker,) in self.con.execute(
                        'select distinct fromid from link where toid=%d' % urlid):
                    # Get the page rank of the linker
                    linkingpr = self.con.execute(
                        'select score from pagerank where urlid=%d' % linker).fetchone()[0]

                    # Get the total number of links from the linker
                    linkingcount = self.con.execute(
                        'select count(*) from link where fromid=%d' % linker).fetchone()[0]
                    pr += 0.85 * (linkingpr / linkingcount)
                self.con.execute(
                    'update pagerank set score=%f where urlid=%d' % (pr, urlid))
            self.db_commit()


class searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def get_match_rows(self, q):
        # Strings to build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # Split the words by spaces
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # Get the word ID
            wordrow = self.con.execute(
                "select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (
                        tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

        # Create the query from the separate parts
        fullquery = 'select %s from %s where %s' % (
            fieldlist, tablelist, clauselist)
        print(fullquery)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids

    def get_scored_list(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])

        # This is where we'll put our scoring functions
        weights = [(1.0, self.location_score(rows)),
                   (1.0, self.frequency_score(rows)),
                   (1.0, self.page_rank_score(rows)),
                   (1.0, self.link_text_score(rows, wordids)),
                   (5.0, self.nn_score(rows, wordids))]
        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]

        return totalscores

    def get_url_name(self, id):
        return self.con.execute(
            "select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.get_match_rows(q)
        scores = self.get_scored_list(rows, wordids)
        rankedscores = [(score, url) for (url, score) in scores.items()]
        rankedscores.sort()
        rankedscores.reverse()
        for (score, urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score, self.get_url_name(urlid)))
        return wordids, [r[1] for r in rankedscores[0:10]]

    def normalize_scores(self, scores, smallIsBetter=0):
        vsmall = 0.00001  # Avoid division by zero errors
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore) / max(vsmall, l)) for (u, l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0:
                maxscore = vsmall
            return dict([(u, float(c) / maxscore) for (u, c) in scores.items()])

    def frequency_score(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc

        return self.normalize_scores(locations, smallIsBetter=1)

    def distance_score(self, rows):
        # If there's only one word, everyone wins!
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])

        # Initialize the dictionary with large values
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i] - row[i - 1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]:
                mindistance[row[0]] = dist
        return self.normalize_scores(mindistance, smallIsBetter=1)

    def inbound_link_score(self, rows):
        uniqueurls = dict([(row[0], 1) for row in rows])
        inboundcount = dict([(u, self.con.execute(
            'select count(*) from link where toid=%d' % u).fetchone()[0]) for u in uniqueurls])
        return self.normalize_scores(inboundcount)

    def link_text_score(self, rows, wordids):
        linkscores = dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            cur = self.con.execute(
                'select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid' % wordid)
            for (fromid, toid) in cur:
                if toid in linkscores:
                    pr = self.con.execute(
                        'select score from pagerank where urlid=%d' % fromid).fetchone()[0]
                    linkscores[toid] += pr
        maxscore = max(linkscores.values())
        normalizedscores = dict([(u, float(l) / maxscore)
                                 for (u, l) in linkscores.items()])
        return normalizedscores

    def page_rank_score(self, rows):
        pageranks = dict([(row[0], self.con.execute(
            'select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores = dict([(u, float(l) / maxrank)
                                 for (u, l) in pageranks.items()])
        return normalizedscores

    def nn_score(self, rows, wordids):
        # Get unique URL IDs as an ordered list
        urlids = [urlid for urlid in dict([(row[0], 1) for row in rows])]
        nnres = mynet.getresult(wordids, urlids)
        scores = dict([(urlids[i], nnres[i]) for i in range(len(urlids))])
        return self.normalize_scores(scores)
