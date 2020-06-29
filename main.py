import asyncio
import time
import aiohttp
import asyncpg
from bs4 import BeautifulSoup


async def corutine_download(session, url):
    async with session.get(url) as response:
        return await response.text()


async def dbsel(sql):
    conn = await asyncpg.connect(user='bexram', password='Stalker_159753',
                                 database='bexram', host='127.0.0.1')
    values = await conn.fetch(sql)
    await conn.close()
    return values


async def dbtrans(sql):
    connection = await asyncpg.connect(user='bexram', password='Stalker_159753',
                                       database='bexram', host='127.0.0.1')

    async with connection.transaction():
        await connection.execute(sql)
    await connection.close()


def use_bs(p_cont1, p_cont2, p_items1, p_items2, html_file):
    soup = BeautifulSoup(html_file)
    page_cont = soup.find(p_cont1, p_cont2)
    if page_cont is not None:
        items = page_cont.find_all(p_items1, p_items2)
        return items


async def parse_f1():
    # загрузка списка форумов 1-го уровня
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await corutine_download(session, 'https://deep.dublikat.shop')
        items_f1 = use_bs('div', {'class': 'p-body-content'}, 'div', {'class': 'block-container'}, results)
        for item in items_f1:
            href_f1 = item.find('h2', {'class': 'block-header'}).find('a').get('href')
            descr_f1 = item.find('h2', {'class': 'block-header'}).find('a').text
            sql = 'insert into f1a (href,name) values (\'%s\',\'%s\')' % (
                'https://deep.dublikat.shop' + href_f1, descr_f1)
            # await dbtrans(sql)


async def parse_f2():
    # разбор 2 уровня
    cursor = await dbsel('select  href, idf1 from f1a')

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [corutine_download(session, row[0]) for row in cursor]
        results = await asyncio.gather(*tasks)
        cnt = 0
        for i in results:
            items_f2 = use_bs('div', {'class': 'block-body'}, 'div', {'class': 'node-body'}, i)
            if items_f2 is not None:
                for item in items_f2:
                    namef2 = item.find('h3', {'class': 'node-title'}).find('a').text
                    if '\'' in namef2:
                        namef2 = namef2.replace('\'', '')
                    href = item.find('h3', {'class': 'node-title'}).find('a').get('href')
                    descr = item.find('div', {'class': 'node-description'})
                    if descr is not None:
                        if '\'' in descr:
                            descr = descr.replace('\'', '')
                        descr = item.find('div', {'class': 'node-description'}).text
                    else:
                        descr = ''
                    # await dbtrans(('insert into f2a (idf1,name,descr,href) values (' + str(cursor[cnt][1]) + ',\'' + namef2 + '\',\'' + descr + '\',\'' + 'https://my.dublikat.shop'+href + '\')'))
            cnt = cnt + 1


async def parse_themes():
    # разбор тем в форумах
    cursor = await dbsel('select  href, idf2 from f2a')
    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [corutine_download(session, row[0]) for row in cursor]
        results = await asyncio.gather(*tasks)
        cnt = 0
        cnt1 = 0
        for i in results:
            a = 2
            while 1:
                items_t = use_bs('div', {'class': 'block-body'}, 'div',
                                 {'class': 'structItem-cell structItem-cell--main'}, i)
                for item in items_t:
                    namet = item.find('div', {'class': 'structItem-title'}).find('a', {'class': ''}).text
                    name2t = item.find('div', {'class': 'structItem-title'}).find('a', {'class': ''}).text
                    hreft = item.find('div', {'class': 'structItem-title'}).find('a', {'class': ''}).get('href')
                    create_usert = item.find('span', {'class': 'username'})
                    if create_usert is not None:
                        create_usert = item.find('span', {'class': 'username'}).text
                    else:
                        create_usert = ''
                    create_datet = item.find('time', {'class': 'u-dt'})
                    if create_usert is not None:
                        create_datet = item.find('time', {'class': 'u-dt'}).text
                    else:
                        create_datet = ''
                    idf2 = cursor[cnt1][1]
                    if '\'' in namet:
                        namet = namet.replace('\'', '')
                    if '\'' in create_usert:
                        create_usert = create_usert.replace('\'', '')
                    # print('https://my.dublikat.shop'+hreft)
                    sql = 'insert into themes_a (idf2,name,href,create_user,create_date) values (' + str(
                        idf2) + ',\'' + namet + '\',\'' + 'https://my.dublikat.shop' + hreft + '\',\'' + create_usert + '\',\'' + create_datet + '\')'

                    # await dbtrans(sql)
                # проверка след страницы
                url = cursor[cnt][0]
                url = url + 'page-' + str(a)

                async with aiohttp.ClientSession(loop=loop) as session:
                    tasks = [corutine_download(session, url)]
                    results1 = await   asyncio.gather(*tasks)

                if results1 is None:
                    cnt = cnt + 1
                    break
                soup3 = BeautifulSoup(results1[0])
                page_cont3 = soup3.find('div', {'class': 'block-body'})
                items2t = page_cont3.find_all('div', {'class': 'structItem-cell structItem-cell--main'})
                items_empty = page_cont3.find_all('div', {'class': 'structItem-cell js-emptyThreadList'})
                if 'Запрашиваемая страница' in page_cont3.text:
                    cnt = cnt + 1
                    break
                if 'нет ни одной темы' in page_cont3.text:
                    cnt = cnt + 1
                    break
                for item in items2t:
                    name2t = item.find('div', {'class': 'structItem-title'}).find('a', {'class': ''}).text
                    break
                for item in items_t:
                    # items2 = page_cont2.find_all('div', {'class': 'structItem-cell structItem-cell--main'})
                    namet = item.find('div', {'class': 'structItem-title'}).find('a', {'class': ''}).text
                    break
                if name2t == namet:
                    cnt = cnt + 1
                    break
                a = a + 1
                i = results1[0]
            cnt1 = cnt1 + 1


async def parse_messages():
    # разбор сообщений на странице
    cursor = await dbsel('select  href, idt from themes_a')
    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [corutine_download(session, row[0]) for row in cursor]
        results = await  asyncio.gather(*tasks)
        cnt = 0
        cnt1 = 0
    for i in results:
        b = 2
        while 1:
            items_m = use_bs('div', {'class': 'p-body-main'}, 'div',
                             {'class': 'message-inner'}, i)
            for item in items_m:
                # print(item)

                if item.find('h4', {'class': 'message-name'}).find('span') is not None:
                    message_user = item.find('h4', {'class': 'message-name'}).find('span').text
                else:
                    message_user = item.find('h4', {'class': 'message-name'}).find('a').text

                message_date = item.find('time', {'class': 'u-dt'}).text
                text_message = item.find('div', {'class': 'bbWrapper'}).text
                if 'class="username' in text_message:
                    ans_user = item.find('a', {'class': 'username'}).text
                    text_message = text_message['</a>':]
                    text_message = ans_user + ', ' + text_message['</a>':]

                if 'blockquote' in text_message:
                    ans_user = item.find('a', {'class': 'bbCodeBlock-sourceJump'}).text
                    ans_text = item.find('div', {'class': 'bbCodeBlock-expandContent '}).text
                    text_message = ans_user + ': ' + ans_text + ' - ' + text_message[
                                                                        '</blockquote>':]

                # images_link = ''
                # img_items=item.find_all('a', {'class': 'link link--external'})
                # if img_items is not None:
                # for item in img_items:
                # images_link=images_link+' '+item.find('img', {'class': 'bbImage'}).data-url
                if '\'' in text_message:
                    text_message = text_message.replace('\'', '')
                if '\'' in message_user:
                    message_user = message_user.replace('\'', '')
                idt = cursor[cnt1][1]
                sql = 'insert into messages_a (idt,date_message,user_message,text) values (\'' + str(
                    idt) + '\',\'' + message_date + '\',\'' + message_user + '\',\'' + text_message + '\')'
                await  dbtrans(sql)

            # проверка наличия следующей страницы в сообщениях
            url = cursor[cnt][0]
            url = url + 'page-' + str(b)
            async with aiohttp.ClientSession(loop=loop) as session:
                tasks = [corutine_download(session, url)]
                results1 = await  asyncio.gather(*tasks)
            if results1 is None:
                break
                cnt = cnt + 1
            soup5 = BeautifulSoup(results1[0])
            page_cont5 = soup5.find('div', {'class': 'p-body-main'})
            items5 = page_cont5.find_all('div', {'class': 'message-inner'})
            if 'Запрашиваемая страница' in page_cont5.text:
                cnt = cnt + 1
                break
            for item in items5:
                textmesst = item.find('div', {'class': 'bbWrapper'}).text
                break
            for item in items_m:
                textmess = item.find('div', {'class': 'bbWrapper'}).text
                break
            if textmess == textmesst:
                break
                cnt = cnt + 1
            b = b + 1
            i = results1[0]
        cnt1 = cnt1 + 1


async def main(loop):
    start = time.time()
    # await parse_f1()
    # await parse_f2()
    # await parse_themes()
    await parse_messages()
    print(time.time() - start)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
