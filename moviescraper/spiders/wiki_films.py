import scrapy
import re


class WikiFilmsSpider(scrapy.Spider):
    name = "wiki_films"
    allowed_domains = ["ru.wikipedia.org", "omdbapi.com"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['title', 'genre', 'director', 'country', 'year', 'imdb_rating'],
        'DOWNLOAD_DELAY': 1,
    }
    API_KEY = "3fac53bc"

    def parse(self, response):
        subcategories = response.css("#mw-subcategories a::attr(href)").getall()
        for subcat in subcategories:
            yield response.follow(subcat, callback=self.parse_category)

        if not subcategories:
            yield from self.parse_category(response)

    def parse_category(self, response):
        films = response.css("#mw-pages li a::attr(href)").getall()
        for film_url in films:
            yield response.follow(film_url, callback=self.parse_film)

        next_page = response.css("a:contains('Следующая страница')::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_category)

    def parse_film(self, response):
        infobox = response.css("table.infobox")
        if not infobox:
            return

        title = response.css("h1#firstHeading::text").get()

        item = {
            'title': title,
            'genre': self.get_infobox_value(infobox, ['Жанр', 'Жанры']),
            'director': self.get_infobox_value(infobox, ['Режиссёр', 'Режиссёры']),
            'country': self.get_infobox_value(infobox, ['Страна', 'Страны']),
            'year': self.get_year(infobox),
            'imdb_rating': None
        }

        if item['year'] and item['title']:
            clean_title = re.sub(r"\(.*?\)", "", item['title']).strip()
            url = f"http://www.omdbapi.com/?apikey={self.API_KEY}&t={clean_title}&y={item['year']}"
            yield scrapy.Request(
                url,
                callback=self.parse_imdb,
                meta={'item': item},
                dont_filter=True
            )
        else:
            yield item

    def parse_imdb(self, response):
        item = response.meta['item']
        data = response.json()

        if data.get('Response') == 'True':
            item['imdb_rating'] = data.get('imdbRating')
        else:
            item['imdb_rating'] = "Not Found"

        yield item

    def get_infobox_value(self, infobox, keys):
        for key in keys:
            row = infobox.xpath(f".//tr[th[contains(text(), '{key}')]]")
            if row:
                text = " ".join(row.xpath(".//td//text()").getall())
                clean_text = re.sub(r'\[.*?\]', '', text).strip()
                return clean_text.replace('\xa0', ' ')
        return None

    def get_year(self, infobox):
        text = self.get_infobox_value(infobox, ['Год', 'Дата выхода'])
        if text:
            match = re.search(r'\d{4}', text)
            return match.group(0) if match else None
        return None
