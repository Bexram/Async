import time
import psycopg2
from requests_html import HTMLSession
import cfscrape




def create_session(url):
    session = HTMLSession()
    session.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0'
    }
    scraper_sess = cfscrape.create_scraper(session)
    r = scraper_sess.get(url)
    session.close()
    return r


def main():
    conn = psycopg2.connect(dbname='bexram', user='bexram',
                            password='Stalker_159753', host='localhost')
    cursor = conn.cursor()
    cursor.execute('select href from f2a')
    start=time.time()
    for i in cursor:
        create_session(i[0])
    print(time.time()-start)




if __name__ == '__main__':
    main()
