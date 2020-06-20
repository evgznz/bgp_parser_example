#/usr/bin/env python3

import requests
from lxml import html,etree 
from io import StringIO
import datetime
from datetime import timedelta
import urllib.request
import os
import time
from clint.textui import progress
import bz2
import json
from bgpdumpy import BGPDump, TableDumpV2

now = datetime.datetime.now()
server_url = "http://routeviews.org"
as_number = "197695"
DEBUG_FILE = False 
r = requests.get(server_url)
if not r.status_code == 200:

    print(r.status_code, "Ошибка подключения к серверу :",server_url)

links = []


html_page = r.content.decode('utf-8')

out = html.fromstring(html_page).xpath('//a/@href')

def json_out(networks,filename,as_number):
    if DEBUG_FILE:
        print("JSON_OUT: AS{} \n {}\n".format(as_number,networks))
    data = {}
    data['AS'] = []
    data['AS'].append({
        'as_name':   str(as_number),
        'sum' :   len(networks),
        'networks':  str(networks)})
    filename_json = "AS{}_{}.json".format(as_number,filename)
    with open(filename_json,'w') as f_json:
        json.dump(data,f_json)

    
def item_id(item):
     mas = item.split('.')

     date_string = "{}-{}-{} {}:{}:00".format(mas[1][0:4],mas[1][4:6],mas[1][6:8],mas[2][0:2],mas[2][2:4])
     time_id = time.mktime(datetime.datetime.strptime(date_string,"%Y-%m-%d %H:%M:%S").timetuple())

     return time_id       

def chek_archive(item):
        return item[-3:] == 'bz2'
def http_status_code(_link):
    return requests.get(_link).status_code
def bgp_as_search(file_bgp,as_number):
    networks_as_number = list()
    with BGPDump(file_bgp) as bgp:
        for entry in bgp:
            if not isinstance(entry.body, TableDumpV2):
                continue  

            prefix = '%s/%d' % (entry.body.prefix, entry.body.prefixLength)

            originatingASs = set([
                 route.attr.asPath.split()[-1]
                 for route
                 in entry.body.routeEntries])

            if '/'.join(originatingASs) == as_number:
                if DEBUG_FILE:
                    print('Найденные сети: %s для AS%s' % (prefix, '/'.join(originatingASs)))
                networks_as_number.append(prefix)

    if DEBUG_FILE:
        print("Найденные подсети" , networks_as_number)
        print("Количество:" ,len(networks_as_number))
    return networks_as_number
            
def links_downloads(_link):
    if http_status_code(_link) == 200:
            array_links = dict()
            rr = requests.get(_link)
            link_html = rr.content.decode('utf-8')
            link_html_lists = html.fromstring(link_html).xpath('//a/@href')
            [ array_links.update({ item_id(item): _link + item})  for item in link_html_lists if chek_archive(item) ]
            max_id_key = (max(array_links,key=float))
            print("Скачивание URL ссылки :",array_links[max_id_key])
            return array_links[max_id_key] , _link[7:] 

def download(_dir,url_item): 
        full_filename_bgp = ''
        full_filename_bz2 = ''
        dir_files = str(_dir)
        item = url_item[7:].split('/') 
        if not os.path.exists(dir_files):
               os.makedirs(dir_files)
        req_file = requests.get(url_item,stream = True)
        full_filename_bz2 = dir_files + item[4]

        filename_bgp = item[len(item) - 1][:-3]  + "bgp"
        filename_bz2 = item[len(item) - 1]
        full_filename_bgp = dir_files + filename_bgp
        if DEBUG_FILE:
            print(item)
            print("full_filename_bz2:", full_filename_bz2)
            print("filename_bz2:", filename_bz2)
            print("filename_bgp:", filename_bgp)
            print("full_filename_bgp:", full_filename_bgp)

        with open(full_filename_bz2, 'wb') as f:
                   total_length = int(req_file.headers.get('content-length'))
                   for chunk in  progress.bar(req_file.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1,label="Загружаем файл {} ".format(filename_bz2)):
                       if chunk:
                           f.write(chunk)
                           f.flush()
        
        f_bz2_lenth = int(os.path.getsize(full_filename_bz2))
        print("Скачали файл: {:5} размером {:12.3f} Мб".format(filename_bz2, f_bz2_lenth/(1024*1024)))
        print("Подождите немного , идет распаковка bz2 архива  ")
        with open(full_filename_bz2,'rb') as f_bz2, open(full_filename_bgp,'wb') as f_bgp:
            f_bgp.write(bz2.decompress(f_bz2.read()))
        print("Поиск AS",as_number)
        return bgp_as_search(full_filename_bgp,as_number)

        

networks = set()
for i in out:
    
    if i[0:4] == 'http':
        links.append(i)
    elif i[0:6] == 'telnet':
        pass
    else:
        if (server_url + i).find('@') == -1:
            
            links.append(server_url + i)

for link in links:
    try:
        if http_status_code(link + now.strftime("/%Y.%m/")) == 200:

            if True:

                list_networks = list()
                link_ribs = link + now.strftime("/%Y.%m/") + 'RIBS/'
                link_ribs_file, dir_ribs = links_downloads(link_ribs)

                list_networks  = download(dir_ribs,link_ribs_file)
                if DEBUG_FILE:
                    print(list_networks)
                for item in list_networks:
                    if DEBUG_FILE:
                        print(item)
                    networks.add(item)


                print("RIBS Количество сетей :{:d} для AS{:5} в файле: {:5}".format(len(networks),as_number,link_ribs))
                if DEBUG_FILE:
                    json_out(networks,"RIB_BGP",as_number)
                    print("RIBS list:",list_networks)
                    print("RIBS Networks", networks)
                    print("RIBS Количество сетей :{:d} для AS{:5} в файле: {:5}".format(len(networks),as_number,link_ribs))

            if True:
                list_networks = list()
                link_updates = link + now.strftime("/%Y.%m/") + 'UPDATES/'
                link_updates_file, dir_update   =   links_downloads(link_updates)
                list_networks = download(dir_update,link_updates_file)        
                if DEBUG_FILE:
                    print(list_networks)
                for item in list_networks:
                    networks.add(item)

                print("RIBS Количество сетей :{:d} для AS{:5} в файле: {:5}".format(len(networks),as_number,link_updates))
                if DEBUG_FILE:

                    json_out(networks,"UPDATE_BGP",as_number)
                    print("UPDATES list:",len(list_networks))
                    print("UPDATES Networks", networks)
                    print("UPDATES Количество сетей :{:d} для AS{:5} в файле: {:5}".format(len(networks),as_number,link_updates))
    except:
        if DEBUG_FILE:
            print("Скачали все , кроме этой ссылки ",link)
        else:
            pass

print(" Загрузка завершена! :)")

print("Общее количество сетей :{:d} для AS{:5}".format(len(networks),as_number))
json_out(networks,"NET_BGP_ALL",as_number)
