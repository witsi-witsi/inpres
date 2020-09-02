from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field

import witsi
import scrapy
import pandas as pd
from pandas.errors import EmptyDataError
from pydantic import BaseModel, Field

# import pipeline

class InpresItem(BaseModel):
    id: int
    fecha: datetime
    lat: float
    lng: float
    sentido: bool
    profundidad: int = Field(unit='Km')
    magnitud: float = Field(unit='Richter')
    intensidad: str
    provincia: str
    url: str


class InpresSpider(scrapy.Spider):
    name = 'inpres'
    base_url = 'http://contenidos.inpres.gob.ar'
    item_class = InpresItem

    custom_settings = {
        'ITEM_PIPELINES': {
            'witsi.pipeline.CsvPipeline': 300,
            'witsi.pipeline.DataPackagePipeline': 500,
            # 'witsi.pipeline.InfluxDbPipeline': 900,
        },

        # witsi.pipeline config
        'CSV': {
            'SORT_BY': ['fecha']
        },
        'DATA_PACKAGE': {
            'NAME': 'inpres',
            'TITLE': 'Some nice text',
            'DESCRIPTION': '',
            'RESOURCE_NAME': ''
        }
    }

    def start_requests(self):
        yield scrapy.Request(url=f'{self.base_url}/buscar_sismo', callback=self.fill_form)

    def fill_form(self, response):
        try:
            df = pd.read_csv('inpres/inpres.csv', parse_dates=['fecha'])
            date_from = df.fecha.max().strftime('%d/%m/%Y')
        except EmptyDataError:
            date_from = '29/07/1998'

        data = {
            'datepicker': date_from,
            'datepicker2': datetime.strftime(datetime.now(), '%d/%m/%Y'),
            'tilde1': 'checkbox'
        }

        yield scrapy.FormRequest.from_response(response,
            formdata=data, callback=self.get_each_page)

    def get_each_page(self, response):
        links = response.xpath("//td[@class='Estilo68']//a")[:-1]
        links = [{
            'page': ''.join(a.xpath('text()').get().split()),
            'href': a.xpath('@href').get()
        } for a in links]

        # Parse the current page
        self.parse(response)

        # Iter over the rest of the pages
        for link in links:
            yield scrapy.Request(url=f"{self.base_url}/{link['href']}", callback=self.parse)

    def parse(self, response):
        rows = response.xpath("//table[@id='sismos']//tr[@class='Estilo68']")[1:]

        for row in rows:
            tds = row.xpath('td')
            cells = [td.xpath('string()').get() for td in tds[1:-1]]

            date = datetime.strptime(f'{cells[0]} {cells[1]}', '%d/%m/%Y %H:%M:%S')
            link = tds[-1].xpath('a/@href').get()
            is_red = tds[1].xpath('div/font[@color="#FF0000"]').get(False)
            if is_red:
                is_red = True

            profundidad = cells[4].split()[0]
            profundidad = ''.join(profundidad.split())

            item = InpresItem(id=link.split('/')[1], fecha=date,
                lat=cells[2], lng=cells[3], profundidad=profundidad,
                magnitud=cells[5], intensidad=cells[6], provincia=cells[7],
                sentido=is_red, url=f'{self.base_url}/{link}')

            yield item.dict()
