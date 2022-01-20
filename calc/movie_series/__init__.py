import requests
from bs4 import BeautifulSoup
import re
from helper import MyTask
import luigi
import collections
from calc.basic import Movies


class MovieSeriesRaw(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("movie_series_raw"))

    def run(self):
        def parse_movie_entry(entry):
            regex = r"([^()]+)\s*(?:\(([^()]+)\))?\s*\(\s*([0-9]{4})\)"
            match = re.search(regex, entry.replace(" film)", ")").replace("(Serial, ", "("))
            if match is None:
                return None
            title, trans, year = match.groups()
            if trans is not None:
                title = trans.strip(" ")
            else:
                title = title.strip(" ")
            year = year.strip(" ")
            return title, int(year)

        def get_series(url):
            r = requests.get(url)
            html_content = r.text
            soup = BeautifulSoup(html_content, "html.parser")
            ols = soup.find_all("ol")
            res = []
            for ol in ols:
                if ol.get("class") is None:
                    entries = []
                    lis = ol.find_all("li")
                    for li in lis:
                        p = parse_movie_entry(li.text)
                        if p is None:
                            print(url)
                        else:
                            entries.append(p)
                    res.append(entries)
            return res

        def get_urls():
            res = []
            url = "https://en.wikipedia.org/wiki/Lists_of_feature_film_series"
            r = requests.get(url)
            html_content = r.text
            soup = BeautifulSoup(html_content, "html.parser")
            ul = soup.select_one(".mw-parser-output > ul:nth-child(7)")
            for li in ul.find_all("li"):
                a = li.find("a")
                res.append("https://en.wikipedia.org" + a["href"])

            return res

        def get_res():
            urls = get_urls()
            series = []
            for url in urls:
                series.extend(get_series(url))
            return series

        self.output_(get_res())


class MovieSeries(MyTask):
    def requires(self):
        return [Movies(), MovieSeriesRaw()]

    def output(self):
        return luigi.LocalTarget(self.p_path("movie_series"))

    def run(self):
        movies, wiki_series = self.input_()
        title_lower = movies["title"].str.lower()

        def find_in_wiki_movies(title, year):
            for series in wiki_series:
                for movie in series:
                    if movie is None:
                        continue
                    t, y = movie
                    if t.lower() == title.lower() and y == year:
                        return series
            return []

        def find_movie_id(title, year):
            df = movies[(title_lower == title.lower()) & (movies["year"] == year)]
            if len(df) == 0:
                return None
            return df["id"].values[0]

        def get_series(title, year):
            series = find_in_wiki_movies(title, year)
            res = []
            for movie in series:
                if movie is None:
                    continue
                movie2 = find_movie_id(*movie)
                if movie2 is not None:
                    res.append(movie2)
            return res

        def get_movies_in_series():
            result = set()
            for _, movie in movies.iterrows():
                t = get_series(movie["title"], movie["year"])
                if len(t) >= 2:
                    result.add(tuple(sorted(t)))

            movies_in_series = collections.defaultdict(list)
            for series in result:
                for movie in series:
                    movies_in_series[movie] = list(series)
            return movies_in_series

        self.output_(get_movies_in_series())
