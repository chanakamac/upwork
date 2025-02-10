import requests
from lxml import etree
import urllib3
urllib3.disable_warnings()
from datetime import datetime
# from souecepkg.extract import GetPages
from souecepkg.extract import Extract


class Handler(Extract):
    start_time = datetime.now()
    base_url = "http://www.bnb.bg"
    sourceId ="bnb.bg"

    browser_header = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
    }

    urls = [
        'http://www.bnb.bg/BankSupervision/BSCreditInstitution/BSCIRegisters/BS_CI_REG_BANKSLIST_EN',
        'http://www.bnb.bg/BankSupervision/BSCreditInstitution/BSCIRegisters/BS_CI_REG_CRBORDACTIV_EN',
        'http://www.bnb.bg/BankSupervision/BSCreditInstitution/BSCIRegisters/BS_CI_REG_CUSTODIAN_EN'
    ]

    session =requests.Session()
    socket_timeout = 60
    buf = []
    # the public method for expose outside of the class
    def Execute(self):
        dataset = []

        pages = self.getallpages()
        if pages is not None:
            rlist = []
            for link in pages:
                res = self.parse(link)
                rlist.append(res)
            dataset = rlist

            self.jsonfilesave(dataset, self.sourceId)

        return dataset


    def parse(self, data):

        company = {}

        name = data['name']
        address = data['address']
        phone = data['phone']
        fax = data['fax']
        web = data['web']
        _link = data['link']
        link = "{}/{}".format(_link, name)
        company['bst:sourceLinks'] = [data['link'], link]

        try:
            streetaddress = address[0]
        except:
            streetaddress = address
        try:
            city = address[1].split(' ')[-1].strip()
            zipcode = address[1].split(' ')[0].strip()
        except:
            city = ''
            zipcode = ''

        fullAddress = ''.join(address)

        company['vcard:organization-name'] = name
        company['@source-id'] = "bnb.bg"
        company['@is-manual'] = False
        # company['bst:registryURI'] = link
        company['tr-org:hasRegisteredPhoneNumber'] = phone
        company['hasRegisteredFaxNumber'] = fax
        company['hasURL'] = web

        company['mdaas:RegisteredAddress'] = {"fullAddress":fullAddress, "country":"Bulgaria", "streetAddress":streetaddress, "city": city, "zip":zipcode}
        company['isDomiciledIn'] = "BG"
        company['RegulationStatus'] = "Authorized"

        edd = {}

        key_name = 'overview'
        endtime = datetime.now()
        elapsed_time = endtime - self.start_time
        edd['sourceId'] = "bnb.bg"
        edd['start_time'] = str(self.start_time)
        edd['end_time'] = str(endtime)
        edd['elapsed_time'] = str(elapsed_time)
        edd[key_name] = company

        return edd


    def getallpages(self):

        try:
            linkset = []
            for url in self.urls:
                r = self.get_content(url, method='GET', headers=self.browser_header)

                tree = etree.HTML(r.content)
                companies = tree.xpath('//table[@bordercolor="#f6e29e"]')

                for comp in companies:

                    chk = comp.xpath('./tr')

                    name = comp.xpath('./tr[2]/td[1]/p[1]/b/text()')
                    name = "".join(name).strip().encode("utf-8").decode("utf-8")

                    if name not in self.buf:
                        address = comp.xpath('./tr[3]/td[2]/p/text()')

                        phone = comp.xpath('./tr[4]/td[2]/p/text()')
                        phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                        fax = comp.xpath('./tr[5]/td[2]/p/text()')
                        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                        web = comp.xpath('./tr[6]/td[2]/p//a/text()')
                        web = "".join(web).strip().encode("utf-8").decode("utf-8")
                        linkset.append({
                            'name':name,
                            'address':address,
                            'phone':phone,
                            'fax':fax,
                            'web':web,
                            'link':url
                        })
                        self.buf.append(name)

                    name = comp.xpath('./tr[2]/td[2]/p[1]/b/text()')
                    name = "".join(name).strip().encode("utf-8").decode("utf-8")

                    if name not in self.buf:
                        address = comp.xpath('./tr[3]/td[4]/p/text()')

                        phone = comp.xpath('./tr[4]/td[4]/p/text()')
                        phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                        fax = comp.xpath('./tr[5]/td[4]/p/text()')
                        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                        web = comp.xpath('./tr[6]/td[4]/p//a/text()')
                        web = "".join(web).strip().encode("utf-8").decode("utf-8")
                        linkset.append({
                            'name':name,
                            'address':address,
                            'phone':phone,
                            'fax':fax,
                            'web':web,
                            'link':url
                        })
                        self.buf.append(name)

                    else:
                        name = comp.xpath('./tr[2]/td[1]/p[1]/b/text()')
                        name = "".join(name).strip().encode("utf-8").decode("utf-8")

                        if name not in self.buf:
                            address = comp.xpath('./tr[3]/td[2]/p/text()')

                            phone = comp.xpath('./tr[4]/td[2]/p/text()')
                            phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                            fax = comp.xpath('./tr[5]/td[2]/p/text()')
                            fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                            web = comp.xpath('./tr[6]/td[2]/p//a/text()')
                            web = "".join(web).strip().encode("utf-8").decode("utf-8")
                            linkset.append({
                                'name':name,
                                'address':address,
                                'phone':phone,
                                'fax':fax,
                                'web':web,
                                'link':url
                            })
                            self.buf.append(name)

                    name = comp.xpath('./tr[2]/td[2]/p[1]/b/text()')
                    name = "".join(name).strip().encode("utf-8").decode("utf-8")

                    if name not in self.buf:
                        address = comp.xpath('./tr[3]/td[4]/p/text()')

                        phone = comp.xpath('./tr[4]/td[4]/p/text()')
                        phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                        fax = comp.xpath('./tr[5]/td[4]/p/text()')
                        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                        web = comp.xpath('./tr[6]/td[4]/p//a/text()')
                        web = "".join(web).strip().encode("utf-8").decode("utf-8")
                        linkset.append({
                            'name':name,
                            'address':address,
                            'phone':phone,
                            'fax':fax,
                            'web':web,
                            'link':url
                        })
                        self.buf.append(name)

                    name = comp.xpath('./tr[7]/td[1]/p[1]/b/text()')
                    name = "".join(name).strip().encode("utf-8").decode("utf-8")

                    if name not in self.buf:
                        address = comp.xpath('./tr[8]/td[2]/p/text()')

                        phone = comp.xpath('./tr[9]/td[2]/p/text()')
                        phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                        fax = comp.xpath('./tr[10]/td[2]/p/text()')
                        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                        web = comp.xpath('./tr[11]/td[2]/p//a/text()')
                        web = "".join(web).strip().encode("utf-8").decode("utf-8")
                        linkset.append({
                            'name':name,
                            'address':address,
                            'phone':phone,
                            'fax':fax,
                            'web':web,
                            'link':url
                        })
                        self.buf.append(name)

                    name = comp.xpath('./tr[7]/td[2]/p[1]/b/text()')
                    name = "".join(name).strip().encode("utf-8").decode("utf-8")

                    if name not in self.buf:
                        address = comp.xpath('./tr[8]/td[4]/p/text()')

                        phone = comp.xpath('./tr[9]/td[4]/p/text()')
                        phone = "".join(phone).strip().encode("utf-8").decode("utf-8")
                        fax = comp.xpath('./tr[10]/td[4]/p/text()')
                        fax = "".join(fax).strip().encode("utf-8").decode("utf-8")
                        web = comp.xpath('./tr[11]/td[4]/p//a/text()')
                        web = "".join(web).strip().encode("utf-8").decode("utf-8")
                        linkset.append({
                            'name':name,
                            'address':address,
                            'phone':phone,
                            'fax':fax,
                            'web':web,
                            'link':url
                        })

                        self.buf.append(name)

            return linkset

        except:
            return None