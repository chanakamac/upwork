import base64
from datetime import datetime
import requests
import json
from lxml import etree
import urllib.request
from io import BytesIO
import tabula
import os
import boto3
import hashlib
import pycountry
from multiprocessing import Process

from souecepkg.extract import Extract


class Handler(Extract):
    start_time = datetime.now()
    base_url = "http://www.fsma.be/ "
    sourceId = "fsma.be"

    browser_header = {
        'User-Agent':
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36',
        'Accept':
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
    }

    session = requests.Session()
    socket_timeout = 60
    
    def Execute(self):
        data = self.getallpages()
        print('asdasd')
        print(data)

        return data
        
    def parse(self, link):

        try:
            edd = {}

            # tree = self.get_tree("https://www.fsma.be" + link)
            overview = self.get_overview(link)
            if overview != None:
                endtime = datetime.now()
                elapsed_time = endtime - self.start_time
                edd['sourceId'] = "fsma.be"
                edd['start_time'] = str(self.start_time)
                edd['end_time'] = str(endtime)
                edd['elapsed_time'] = str(elapsed_time)
                edd['overview'] = overview

            if self.get_documents(link) != None:
                edd['documents'] = self.get_documents(link)

            if self.get_shareholders(link) != None:
                edd['shareholders'] = self.get_shareholders(link)

            return edd

        except Exception as e:
            print('parse error :'+link)
            print(e)

            
    def get_overview(self, link):
        company = {}
        address = {}
        tree = self.get_tree("https://www.fsma.be" + link)
        try:
            company['vcard:organization-name'] = tree.xpath('//*[@id="main-content"]/div/h1/text()')[0]
            
            try:
                company['hasURL'] = tree.xpath('//div[contains(text(),"Link to website")]/following-sibling::div/div/a/text()')[0]
            except:
                pass
            
            try:
                if tree.xpath('//div[contains(text(),"Active")]/following-sibling::div/text()')[0] == 'Yes':
                    company['hasActivityStatus'] = 'Active'
                elif tree.xpath('//div[contains(text(),"Active")]/following-sibling::div/text()')[0] == 'No':
                    company['hasActivityStatus'] = 'Inactive'
            except:
                pass
            
            try:
                previous_names = []
                for tr in tree.xpath('//div[contains(text(),"Legal names")]/following-sibling::div/div/div/table/tbody/tr')[:-1]:
                    previous_name = {}
                    previous_name['name'] = tr.xpath('td[1]/div/text()')[0]
                    previous_name['valid_from'] = str(datetime.strptime(tr.xpath('td[2]/div/text()')[0], '%d/%m/%Y').date())
                    previous_name['valid_to'] = str(datetime.strptime(tr.xpath('td[3]/div/text()')[0], '%d/%m/%Y').date())
                    
                    previous_names.append(previous_name)
                    
                    if previous_names:
                        company['previous_names'] = previous_names
            except:
                pass
            
            try:
                try:
                    business_classifiers = []
                    for tr in tree.xpath('//div[contains(text(),"Status of entity")]/following-sibling::div/div/div/table/tbody/tr'):
                        business_classifier = {}
                        try:
                            if tr.xpath('td[1]/div')[0].text:
                                business_classifiers.append({
                                    'code': "",
                                    'description': tr.xpath('td[1]/div')[0].text,
                                    'label': ""
                                })
                        except:
                            pass
                        
                        try:
                            if tr.xpath('td[2]/div/div')[0].text:
                                business_classifiers.append({
                                    'code': "",
                                    'description': tr.xpath('td[2]/div/div')[0].text,
                                    'label': ""
                                })
                        except:
                            pass
                        
                        try:
                            if tr.xpath('td[3]/div')[0].text:
                                split_classifiers = tr.xpath('td[3]/div')[0].text.split(',')
                                if len(split_classifiers) > 1:
                                    for classifier in split_classifiers:
                                        business_classifiers.append({
                                            'code': "",
                                            'description': classifier.strip(),
                                            'label': ""
                                        })
                                else:
                                    business_classifiers.append({
                                        'code': "",
                                        'description': tr.xpath('td[3]/div')[0].text,
                                        'label': ""
                                    })
                        except:
                            pass
                        
                    if business_classifiers:
                        company['bst:businessClassifier'] = business_classifiers
                except Exception as e:
                    print('overview')
                    print(e)
                    pass
                
                try:
                    # address = {}
                    
                    trs = tree.xpath('//div[contains(text(),"Address")]/following-sibling::div/div/div/table/tbody/tr')
                    address_index = -1

                    if len(trs) > 1:
                        for i in range(0, ):
                            if trs[i].xpath('td[5]/div/text()')[0] == 'BE':
                                address_index = i

                        if trs[address_index].xpath('td[1]/div/text()')[0].strip() != "":
                            address['streetAddress'] = trs[address_index].xpath('td[1]/div/text()')[0].strip()

                        if trs[address_index].xpath('td[3]/div/text()')[0].strip() != "":
                            address['zip'] = trs[address_index].xpath('td[3]/div/text()')[0].strip()

                        if trs[address_index].xpath('td[4]/div/text()')[0].strip() != "":
                            address['city'] = trs[address_index].xpath('td[4]/div/text()')[0].strip()

                        if trs[address_index].xpath('td[5]/div/text()')[0].strip() != "":
                            address['country'] = pycountry.countries.get(alpha_2=trs[address_index].xpath('td[5]/div/text()')[0].strip()).name

                        if address:
                            company['mdaas:RegisteredAddress'] = address
                            company['mdaas:RegisteredAddress']['fullAddress'] = trs[address_index].xpath('td[2]/div/text()')[0] + " " + address['streetAddress'] + ", " + address['zip'] + ", " + address['city'] + ", " + address['country']
                except Exception as e:
                    pass
                
                try:
                    company['bst:businessClassifier'] = [{
                        'code': "",
                        'description': tree.xpath('//div[contains(text(),"Sector")]/following-sibling::div/text()')[0],
                        'label': ""
                    }]
                except:
                    pass
            except:
                pass
            
            if address:
                company['isDomiciledIn'] = pycountry.countries.get(name=address['country']).alpha_2
            else:
                company['isDomiciledIn'] = "BE"
            
            try:
                if link.rsplit('/')[2] == 'party':
                    company['regulator_name'] = "Financial Services and Markets Authority"
                    company['regulator_url'] = "http://www.fsma.be/"
                    company['RegulationStatus'] = "Authorised"
                    company['regulatorAddress'] = {
                        'fullAddress': "Rue du Congrès/Congresstraat 12-14, 1000 Brussels Belgium",
                        'city': "Brussels",
                        'country': "Belgium"
                    }
                    
                elif link.rsplit('/')[2] == 'prospectus':
                    company['regulator_name'] = tree.xpath('//div[contains(text(),"Authority")]/following-sibling::div/text()')[0]
                    
                elif link.rsplit('/')[2] == 'issuer':
                    company['regulator_name'] = tree.xpath('//div[contains(text(),"Supervision FSMA based upon")]/following-sibling::div/text()')[0]
            except:
                pass
            
            try:
                identifiers = {}
                
                try:
                    identifiers['trade_register_number'] = tree.xpath('//*[@id="main-content"]/div/article/div/div[2]/div[2]/a/text()')[0]
                except:
                    pass

                try:
                    identifiers['trade_register_number'] = tree.xpath('//div[contains(text(),"Company number")]/following-sibling::div/text()')[0]
                except:
                    pass
                try:
                    identifiers['swift_code'] = tree.xpath('//div[contains(text(),"Bank code")]/following-sibling::div/text()')[0]
                except:
                    pass
                try:
                    identifiers['legal_entity_identifier'] = tree.xpath('//div[contains(text(),"LEI")]/following-sibling::div/text()')[0]
                except:
                    pass
                
                if identifiers:
                    company['identifiers'] = identifiers
            except:
                pass
            
            try:
                company['legislationidentifier'] = tree.xpath('//div[contains(text(),"CBFA code")]/following-sibling::div/text()')[0]
            except:
                pass
            
            try:
                company['bst:stock_info'] = {
                    'main_exchange': tree.xpath('//div[contains(text(),"Market")]/following-sibling::div/text()')[0]
                }
            except:
                pass
            
            try:
                company['lei:legalForm'] = {
                    'code': "",
                    'label': tree.xpath('//div[contains(text(),"Legal form entity")]/following-sibling::div/div/div/table/tbody/tr[1]/td[1]/div/text()')[0]
                }
            except:
                pass
            
            company['bst:registryURI'] = "https://www.fsma.be" + link
            source_link="https://www.fsma.be" + link
            try:
                shareholder_link=tree.xpath('//div[contains(text(),"Shareholding")]/following-sibling::div/div/a')[0].attrib['href']
                if shareholder_link:
                    company['bst:sourceLinks']=[source_link,shareholder_link]
            except:
                company['bst:sourceLinks']=[source_link]
                
            
            try:
                company['sourceDate'] = str(datetime.strptime(tree.xpath('//div[contains(text(),"Date")]/following-sibling::div/text()')[0].strip(), '%d/%m/%Y').date())
            except:
                pass
            
            company['@source-id'] = self.sourceId

            if company["isDomiciledIn"] == "BE":
                return company
            else:
                company = {}
                return company

        except:
            print('overview empty :' + link)
            pass
        

        
    def get_shareholders(self, link):

        edd = {}
        shareholders={}
        sholdersl1 = {}
        tree = self.get_tree("https://www.fsma.be" + link)
        company_name = tree.xpath('//*[@id="main-content"]/div/h1/text()')[0]
        company_name_hash = hashlib.md5(company_name.encode('utf-8')).hexdigest()

        try:
            file = tree.xpath('//div[contains(text(),"Shareholding")]/following-sibling::div/div/a')[0].attrib['href']

            if file:
                fd = urllib.request.urlopen('https://www.fsma.be'+file)
                print("fd : "+fd)
                df = tabula.read_pdf(BytesIO(fd.read()), guess=False, lattice=False, stream=True, multiple_tables=True, pages="all")
                # print("df : "+df)
                rows = []
                for d in df:
                    rows += d.values.tolist()

                df = []
                for row in rows:
                    item = " ".join(row).rsplit(" ",2)
                    if len(item) == 3:
                        try:
                            if "%" in item[2]:
                                item[2] = item[2].replace(",",".").strip("% ")
                                if "TOTAL" not in item[0]:
                                    item[0] = item[0].strip(" 1")
                                    df.append(item)
                        except:
                            pass

                for row in df:

                    try:
                        shareholder_name = row[0]
                        shareholder_name_hash = hashlib.md5(shareholder_name.encode('utf-8')).hexdigest()

                        print(shareholder_name_hash)

                        shareholders[shareholder_name_hash] = {
                            "natureOfControl": "SHH",
                            "source": "Services and Markets Authority",
                            "votingPercentage": row[2]
                        }

                        basic_org = {
                            "vcard:organization-name": shareholder_name,
                        }

                        sholdersl1[shareholder_name_hash] = {
                            "entity_type": "C",
                            "basic": basic_org,
                            "shareholders": {}
                        }

                        company = self.get_overview(tree, link)

                        edd[company_name_hash] = {
                            "basic": company,
                            "entity_type": "C",
                            "shareholders": shareholders
                        }
                    except:
                        pass

                if edd != None:
                    return edd
            else:
                return []

        except Exception as e:
            print('shareholders empty :'+link)
            pass




    def get_documents(self, link):
        documents = []
        tree = self.get_tree("https://www.fsma.be" + link)
        try:
            file = tree.xpath('//div[contains(text(),"Files")]/following-sibling::div/div/a')[0].attrib['href']
            if file:
                documents.append({
                    'date': str(datetime.strptime(tree.xpath('//div[contains(text(),"Date")]/following-sibling::div/text()')[0].strip(), '%d/%m/%Y').date()),
                    'description': tree.xpath('//div[contains(text(),"Document")]/following-sibling::div/div/text()')[0],
                    'url': file
                })
            return documents
        except Exception as e:
            print('documents empty :' + link)
            pass



    def getallpages(self):

        p3 = Process(target=self.runActissuer)
        p3.start()
        p1 = Process(target=self.runActprospectus)
        p1.start()
        p2 = Process(target=self.runActparty)
        p2.start()

        p1.join()
        p2.join()
        p3.join()

        return 'Done'


    def runActprospectus(self):

        try:
            url = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actprospectus&page=0#data-portal-facets'

            tree = self.get_tree(url)

            pages = tree.xpath('//*[@id="main-content"]/div/div/header/div/span/text()')[0]
            pagination = pages.split(' of ')[-1]
            pagination = int(pagination.replace(' results', ''))

            nop = int(pagination / 10) + 1

            print("Section Actprospectus")

            for i in range(0, nop):
                status = str(i) + "/" + str(nop)
                url_string = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actprospectus&page={}#data-portal-facets'

                url2 = url_string.format(i)
                tree2 = self.get_tree(url2)

                errorpage = tree.xpath('//*[@id="page_header"]/h2/text()')

                if len(errorpage) == 0:

                    for link in tree2.xpath('//*[@id="main-content"]/div/div/div/ul/li/div/a'):

                        tree3 = self.get_tree('https://www.fsma.be' + link.attrib['href'])
                        errorpage = tree3.xpath('//*[@id="page_header"]/h2/text()')

                        if len(errorpage) == 0:

                            res = self.parse(link.attrib['href'])

                            ret = self.jsonfilesave(res, self.sourceId, status)
                            if ret == 'done':
                                continue
                else:
                    print(url)
                    continue

        except Exception as e:
            print('Failed Actprospectus')
            pass



    def runActparty(self):
        url = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actparty&page=0#data-portal-facets'

        try:

            tree = self.get_tree(url)

            pages = tree.xpath('//*[@id="main-content"]/div/div/header/div/span/text()')[0]
            pagination = pages.split(' of ')[-1]
            pagination = int(pagination.replace(' results', ''))

            nop = int(pagination / 10) + 1

            print("Section Actparty")

            for i in range(0, nop):
                status = str(i) + "/" + str(nop)

                url_string = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actparty&page={}#data-portal-facets'

                url2 = url_string.format(i)
                tree2 = self.get_tree(url2)

                errorpage = tree.xpath('//*[@id="page_header"]/h2/text()')

                if len(errorpage) == 0:

                    for link in tree2.xpath('//*[@id="main-content"]/div/div/div/ul/li/div/a'):

                        # link_list.append(link.attrib['href'])
                        tree3 = self.get_tree('https://www.fsma.be' + link.attrib['href'])
                        errorpage = tree3.xpath('//*[@id="page_header"]/h2/text()')

                        if len(errorpage) == 0:

                            res = self.parse(link.attrib['href'])

                            if len(res.keys()) > 0 :
                                ret = self.jsonfilesave(res, self.sourceId, status)

                                if ret == 'done':
                                    continue
                else:
                    print(url)
                    continue

        except Exception as e:
            error = "Failed Actparty, url : {}".format(url)
            print(error)
            pass



    def runActissuer(self):

        try:
            url = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actissuer&page=0#data-portal-facets'

            tree = self.get_tree(url)

            pages = tree.xpath('//*[@id="main-content"]/div/div/header/div/span/text()')[0]
            pagination = pages.split(' of ')[-1]
            pagination = int(pagination.replace(' results', ''))

            nop = int(pagination / 10) + 1

            for i in range(0, nop):
                status = str(i)+"/"+str(nop)
                print("Page Number :"+status)

                url_string = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3Actissuer&page={}#data-portal-facets'

                url2 = url_string.format(i)
                tree2 = self.get_tree(url2)

                errorpage = tree.xpath('//*[@id="page_header"]/h2/text()')

                if len(errorpage) == 0:

                    for link in tree2.xpath('//*[@id="main-content"]/div/div/div/ul/li/div/a'):

                        # link_list.append(link.attrib['href'])
                        tree3 = self.get_tree('https://www.fsma.be'+link.attrib['href'])
                        errorpage = tree3.xpath('//*[@id="page_header"]/h2/text()')

                        if len(errorpage) == 0:

                            res = self.parse(link.attrib['href'])

                            ret = self.jsonfilesave(res, self.sourceId, status)

                            if ret == 'done':
                                continue
                else:
                    print(url)
                    continue

        except Exception as e:
            print('Failed Actissuer')
            pass




    def numnerOfpages(self):

        content_types = ['Actprospectus', 'Actparty', 'Actissuer']
        url_string = 'https://www.fsma.be/en/data-portal?search_api_fulltext=&sort_by=field_ct_last_update&f%5B0%5D=fa_content_type%3{}&page={}#data-portal-facets'

        for content_type in content_types:
            # if len(link_list) > 1:
            url = url_string.format(content_type, 1)

            print(url)

            tree = self.get_tree(url)

            pages = tree.xpath('//*[@id="main-content"]/div/div/header/div/span/text()')[0]
            pagination = pages.split(' of ')[-1]
            pagination = int(pagination.replace(' results', ''))

            nop = int(pagination / 10) + 1

            print(nop)
