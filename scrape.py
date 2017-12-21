import datetime
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from influxdb import InfluxDBClient


class Scraper:
    def __init__(self):
        self.s = requests.session()
        self.s.headers.update({
            'Accept-Language': 'en-US,en;q=0.5',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0'
        })
        self.profile_base_url = 'http://stats.comunio.de/profil.php?id={}'
        self.base_url = 'http://www.comstats.de'
        self.influx_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'machiavelli')
        print(self.s.headers)

    def scrape(self):

        entry_url = urljoin(self.base_url, 'squad')
        club_links = self.get_links(self.get_soup(entry_url).select('td.clubPics a'))
        for cl in club_links:
            player_links = self.get_links(self.get_soup(urljoin(self.base_url, cl)).select('td.playerCompare div a'))
            id_pattern = r'/csprofile/(\d+)-\w+'
            player_ids = [int(re.search(id_pattern, link).group(1)) for link in player_links]
            snapshots = []
            for pid in player_ids:
                snapshots.append(self.get_snapshot(pid))
            self.write_snapshots(snapshots)

    def get_snapshot(self, player_id):
        soup = self.get_soup(self.profile_base_url.format(player_id))
        club_id, club_name = self.get_club_info(soup)

        return {
            'id': player_id,
            'name': self.get_player_info_elem(soup, 'Name').text,
            'market_value': int(self.get_player_info_elem(soup, 'Market value').text.replace('.', '')),
            'points': self.get_points(soup),
            'club_id': club_id,
            'club_name': club_name,
            'trend': self.get_trend(soup)
        }

    def get_points(self, soup):
        try:
            return int(self.get_player_info_elem(soup, 'Points').text.replace('.', ''))
        except ValueError:
            return 0

    def get_trend(self, soup):
        pattern = r'icon-trend_([-]?\d+)'
        for c in self.get_player_info_elem(soup, 'Trend').find('img').get('class'):
            match = re.search(pattern, c)
            if match:
                return int(match.group(1))

    def get_player_info_elem(self, soup, info_name):
        return soup.find('td', string=info_name).next_sibling

    def get_club_info(self, soup):
        club_data = self.get_player_info_elem(soup, 'Club')
        id = int(re.search(r'/squad/(\d+)-\w+', club_data.find('a').get('href')).group(1))
        name = club_data.find('img').get('title')
        return id, name

    def get_links(self, elems):
        return [elem.get('href') for elem in elems]

    def get_soup(self, url):
        r = self.s.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup

    def write_snapshots(self, player_snapshots):
        json_body = [
            {
                'measurement': 'player',
                'tags': {
                    'player_id': snapshot['id'],
                    'name': snapshot['name'],
                    'club_id': snapshot['club_id'],
                    'club_name': snapshot['club_name']
                },
                'time': datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time()),
                'fields': {
                    'value': snapshot['market_value'],
                    'points': snapshot['points'],
                    'trend': snapshot['trend']
                }
            }
            for snapshot in player_snapshots
        ]
        print(json_body)
        self.influx_client.write_points(json_body)

def main():
    s = Scraper()
    s.scrape()






if __name__ == '__main__':
    main()