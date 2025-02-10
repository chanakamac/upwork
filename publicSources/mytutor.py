import requests
from lxml import etree
import json, time, datetime, csv
import re
import html2text
import dateparser
import pycountry
from datetime import timedelta
import base64

class Handler():
    API_BASE_URL = ""
    base_url = "https://www.mytutor.lk/"
    NICK_NAME = "mytutor.lk"
    FETCH_TYPE = ""
    TAG_RE = re.compile(r'<[^>]+>')

    browser_header = {
        'User-Agent':
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36',
        'Accept':
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
    }

    session = requests.Session()
    socket_timeout = 60

    def Execute(self, searchquery, fetch_type, action, API_BASE_URL):
        self.FETCH_TYPE = fetch_type
        self.API_BASE_URL = API_BASE_URL

        if fetch_type is None or fetch_type == "":
            pages = self.getpages(searchquery)

            if pages is not None:
                data = self.parse_pages(pages)
            else:
                data = []
            dataset = {}
            arr = []
            arr.append(data)
            dataset = data
        else:
            data = self.fetchByField(searchquery)
            dataset = []
            dataset.append(data)
        return dataset


    def getpages(self, searchquery):
        print(searchquery)
        try:
            company = {}
            link = 'https://www.mytutor.lk/teachers_and_tuition_classes_in_sri_lanka.php?page=1&ipp=100&key_word=&fk_subjects=mps%3D&fk_district=&fk_city=&verify=Verified+Option&fk_category=&fk_tutortype=lg%3D%3D&fk_tuitiontype=&fk_medium=&fk_gender='
            tree = requests.get(link, headers=self.browser_header).content

            data = tree.xpath('/html/body/div[2]/div[1]/section/div[2]/div/div/div[1]/div/div[2]/div/h5/a/strong/text()')
            print(data)

            for x in data:
                name = x.xpath('./div[2]/div/h5/a/strong/text()')
                name = "".join(name).strip().encode("utf-8").decode("utf-8")

                company['vcard:organization-name'] = name

            print(company)
            return company

        except Exception as e:
            # print(e)
            raise
            return None

    def parse_pages(self, pages):
        rlist = []
        i = 0
        for link in pages:
            i = i + 1
            if i == 11:
                break
            res = self.parse(link)
            rlist.append(res)

        return rlist

    def getContent(self, link):
        r = requests.get(link, headers=self.browser_header).content
        return r

    def replace_html(self, content):
        str = content.replace("&amp;", "&").replace("&#160;", " ")
        return str.decode('utf8')

    def convertbytes2string(self, text):
        if not isinstance(text, str):
            return text.decode("utf-8")
        else:
            return text

    def get_content(self, url):
        try:
            r = self.session.get(url,
                                 headers=self.browser_header,
                                 timeout=self.socket_timeout)
            return r
        except Exception as e:
            # print(e, url)
            return False

    def parse(self, param):

        company = {}

        name = param['name']

        address = param['address']
        try:
            city = address.split(',')[-1].strip()
        except:
            city = ''

        try:
            streetAddress = address.split(city)[0].strip()
        except:
            streetAddress = ''

        tel = param['tel']

        fax = param['fax']

        website = param['website']

        other = param['old_name']
        other_li = []
        if other:
            other_li.append(other)

        list_date = param['date']

        if list_date and 'Date Licensed'.lower() not in list_date.lower():
            company['isIncorporatedIn'] = self.format_date(list_date)

        if 'commercial' in param['type'].lower():
            link = 'https://www.cbn.gov.ng/Supervision/fi.asp?name={}&institutetype=Commercial%20Bank'.format(param['name'])
        else:
            link = 'https://www.cbn.gov.ng/Supervision/fi.asp?name={}&institutetype=Bureau%20De%20Change'.format(param['name'])

        company['vcard:organization-name'] = name
        company['@source-id'] = "cbn.gov.ng"
        company['@is-manual'] = False
        # company['bst:registryURI'] = link

        company['mdaas:RegisteredAddress'] = {
            "fullAddress": address,
            "country": "Nigeria",
            "streetAddress": streetAddress,
            "city": city,
            "zip": ""
        }
        company['isDomiciledIn'] = "NG"
        company['tr-org:hasRegisteredPhoneNumber'] = tel
        company['hasRegisteredFaxNumber'] = fax
        company['hasURL'] = website
        company['RegulationStatus'] = "Authorized"
        if len(other_li) > 0:
            company['bst:aka'] = other_li

        edd = {}

        key_name = 'overview'
        edd[key_name] = company
        edd['@identifier'] = None
        # edd['bst:registryURI'] = link

        edd['_links'] = self.links(link)

        return edd

    def overviewparse(self, html_, link):

        company = {}

        myparser = etree.HTMLParser(encoding="utf-8")
        tree = etree.HTML(html_,parser=myparser)
        company = {}

        name = tree.xpath('//table[@class="othertables"]/tr[1]/td[3]/b/text()')
        name = "".join(name).strip().encode("utf-8").decode("utf-8")

        address = tree.xpath('//table[@class="othertables"]/tr[3]/td[2]/text()')
        address = "".join(address).strip().encode("utf-8").decode("utf-8")

        tel = tree.xpath('//table[@class="othertables"]/tr[5]/td[2]/text()')
        tel = "".join(tel).strip().encode("utf-8").decode("utf-8")

        fax = tree.xpath('//table[@class="othertables"]/tr[6]/td[2]/text()')
        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")

        website = tree.xpath('//table[@class="othertables"]/tr[7]/td[2]/a/@href')
        website = "".join(website).strip().encode("utf-8").decode("utf-8")

        address = tree.xpath('//table[@class="othertables"]/tr[3]/td[2]/text()')
        address = "".join(address[0]).strip().encode("utf-8").decode("utf-8")

        try:
            city = address.split(',')[-1].strip()
        except:
            city = ''

        try:
            streetAddress = address.split(city)[0].strip()
        except:
            streetAddress = ''

        other = tree.xpath('//table[@class="othertables"]/tr[1]/td[9]/b/text()')
        other = "".join(other).strip().encode("utf-8").decode("utf-8")
        other_li = []
        if other:
            other_li.append(other)
        Listing_DateT = tree.xpath('//table[@class="othertables"]/tr[2]/td[2]/span/font/text()')
        Listing_DateT = "".join(Listing_DateT).strip().encode("utf-8").decode("utf-8")

        list_dateG = re.search('(\d+\/\d+\/\d+)', Listing_DateT)
        if list_dateG:
            list_date = list_dateG.group(1)
        else:
            list_date = None

        if list_date:
            company['isIncorporatedIn'] = self.format_date(list_date)

        company['vcard:organization-name'] = name
        company['@source-id'] = "cbn.gov.ng"
        company['@is-manual'] = False
        # company['bst:registryURI'] = link

        company['mdaas:RegisteredAddress'] = {"fullAddress":address, "country":"Nigeria", "streetAddress":streetAddress, "city": city, "zip":""}
        company['isDomiciledIn'] = "NG"
        company['tr-org:hasRegisteredPhoneNumber'] = tel
        company['hasRegisteredFaxNumber'] = fax
        company['hasURL'] = website
        company['RegulationStatus'] = "Authorized"
        if len(other_li) > 0:
            company['bst:aka'] = other_li

        edd = {}

        key_name = 'overview'
        edd[key_name] = company
        edd['@identifier'] = None
        edd['bst:registryURI'] = link

        edd['_links'] = self.links(link)

        return edd


    def links(self, link):
        data = {}
        base_url = self.NICK_NAME
        link2 = base64.b64encode(link.encode('utf-8'))
        link2 = (link2.decode('utf-8'))
        #elif self.FETCH_TYPE =="overview":
        data['overview']= {"method":"GET","url":self.API_BASE_URL+"?source="+base_url+"&url="+link2+"&fields=overview"}
        return data

    def format_date(self, text):
        if not isinstance(text, str):
            text = text.decode("utf-8")
        try:
            dv = dateparser.parse(text)
            dv = dv.strftime("%Y-%m-%d")
            return dv
        except Exception as e:
            # print(e)
            pass

    def get_content(self, url):
        try:
            r = self.session.get(url,
                                 headers=self.browser_header,
                                 timeout=self.socket_timeout)
            return r
        except:
            return False

    def remove_tags(self, text):
        if not isinstance(text, str):
            text = text.decode("utf-8")
        return self.TAG_RE.sub('', text)

    def getValidActivityStatus(self, text):
        status = str('CLOSED').encode("utf-8").upper()
        text = str(text).encode("utf-8").upper()
        if status in text:
            status = 'dissolved'

        return status
