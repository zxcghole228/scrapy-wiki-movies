import scrapy
import re


class WikiFilmsSpider(scrapy.Spider):
    name = "wiki_films"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]

    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['title', 'genre', 'director', 'country', 'year'],
        'DOWNLOAD_DELAY': 1,
    }

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
        }
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
