'''
Functions:
    -atomParser
    -unzipandparse


Procedures:
1.- Download rss notices files from web and save to IMPORTDIR
2.- File by file unzip to TEMPDIR and parse at the same time to optimize space
    -AS A DICT, if there is more than one notice by the same identifier, its grouped.
    -Our interest is: Awarded notices     not: general/desert notices
3.- JSON dump to file
     -#TODO maybe repeated cifs because diferents years contracts

'''
import bs4 as bs
import json
import logging
import os, re
import requests
import zipfile


logging.basicConfig(filename='debug.log', filemode='w+', format='%(asctime)s - %(levelname)s - %(message)s')
logging.warning('--------------------------New Execution--------------------------')

OUTPUDIR= 'output/'
IMPORTDIR = 'import/'
TEMPDIR = 'tmp/'

# file ='2012/licitacionesPerfilesContratanteCompleto3.atom'


def atomParser(file):
    '''Read atom file to extract WINNERS'''
    fileread = open(file)
    source = fileread.read()
    soup = bs.BeautifulSoup(source, 'xml')
    entry = soup.find_all('entry')

    dades = {}
    errors = {}
    notinterest = 0
    for x,i in enumerate(entry):
        print('processant: ' + str(x) + ' from:' + str(file), end="\r")

        # Has winner?
        if not (i.find('WinningParty')):
            notinterest += 1
            continue
        #check if has lots:
        if len(i.findAll('ProcurementProjectLot')) != 0:
            # there's maybe some companies to parse
            identity = i.find('ContractFolderStatus').find('ContractFolderID').string
            weblink = i.link['href']
            lots = i.findAll('TenderResult')
            for k in lots:
                try:
                    dict = {}
                    if k.find('ResultCode').string in ('3','4'):    ## void, not awarded, abandoned
                        notinterest += 1
                        continue

                    cif = (k.find('WinningParty').find('PartyIdentification').find('ID').string).strip()
                    name = k.find('WinningParty').find('PartyName').find('Name').string
                    try:  #### some hasnt got taxexclusive item
                        taxexclusive = k.find('AwardedTenderedProject').find('TaxExclusiveAmount').string
                    except:
                        pass

                    payableamount = k.find('AwardedTenderedProject').find('PayableAmount').string
                    content = { 'partyname': name  ,'sumary': i.summary.string, 'weblink' : weblink, 'taxexclusive': taxexclusive,
                                               'payableamount': payableamount  }
                    dict[cif] = [ content ]
                    if cif in dades:
                        dades[cif].append(content)
                    else:
                        dades.update(dict)

                except Exception as e:

                    errors.update({identity : [i.summary.string, e, x, 'lots']})
                    logging.warning( i.summary.string + 'error: ' + str(e)  +'entry num: '+  str(x) +
                                    ' lots | file --> ' +file)

        # No lots, only one company
        else:
            try:
                dict = {}
                if i.find('ResultCode').string in ('3','4'):    ## void, not awarded, abandoned
                    notinterest += 1
                    continue
                cif = (i.find('WinningParty').find('PartyIdentification').find('ID').string).strip()
                name = i.find('WinningParty').find('PartyName').find('Name').string
                weblink = i.link['href']
                try:     #### some hasnt got taxexclusive item
                    taxexclusive = i.find('AwardedTenderedProject').find('TaxExclusiveAmount').string
                except:
                    pass
                payableamount = i.find('AwardedTenderedProject').find('PayableAmount').string
                content = { 'partyname': name, 'sumary': i.summary.string, 'weblink': weblink, 'taxexclusive': taxexclusive,
                           'payableamount': payableamount}
                dict[cif] = [ content]
                if cif in dades:
                    dades[cif].append(content)
                else:
                    dades.update(dict)

            except Exception as e:
                errors.update({ i.find('ContractFolderID').string : [ i.summary.string , i.link['href'],  e, x]})
                # logging.warning({ i.find('ContractFolderID').string : [ i.summary.string , i.link['href'],  e, x]} + ' file --> ' + file)
                logging.warning(i.summary.string + 'error: ' + str(e) + 'entry num: ' + str(x) +
                                ' lots | file --> ' + file)


    print('Errors : ' +str(len(errors))+'|-|NotInterested: '+str(notinterest) +     '--file->' +file)
    fileread.close()
    return dades

def unzipandParse(file):
    '''Pass downloaded zip file for decompress and parse
    Export to JSON'''


    db = {}
    if not os.path.exists(TEMPDIR):
        os.mkdir(TEMPDIR)

    zip_ref = zipfile.ZipFile(IMPORTDIR + file, 'r')
    filelist = zip_ref.namelist()
    zip_ref.extractall(TEMPDIR)
    zip_ref.close()

    for atomfile in filelist:

        db.update(atomParser(TEMPDIR + atomfile))


    json_file = open(OUTPUDIR + 'output.json', 'a')
    print('Parsed data: ' + str(len(db)))

    json.dump(db, json_file, sort_keys=True, indent=2, ensure_ascii=False)
    json_file.close()


    for f in os.listdir(TEMPDIR):
        if re.search('.atom', f):
            os.remove(os.path.join(TEMPDIR, f))



def downloadHistory():
    '''Download past files'''
    pastlinks = [ 'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2012.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2013.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2014.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2015.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2016.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2017.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2018.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_201901.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_201902.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_201903.zip',
                  'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_201904.zip',
                  ]
    #TODO Carregar els links de la web oficial del estado. No carrega la web amb requests
    # http://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx'

    if os.path.exists(IMPORTDIR):
        print('''Deleting old files if exists''')
        for f in os.listdir(IMPORTDIR):
            if re.search('.zip',f):
                os.remove(os.path.join(IMPORTDIR,f))
    else:
        os.mkdir(IMPORTDIR)


    for filelink in pastlinks:
        print('Downloading ' + filelink +'...', end = " ")
        with requests.get(filelink, stream=True) as r:
            r.raise_for_status()
            with open(IMPORTDIR + str(filelink.split('/')[-1]), 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print('OK')
    print(str(len(pastlinks)) + ' Downloaded files to : ' + IMPORTDIR +' dir')





unzipandParse('licitacionesPerfilesContratanteCompleto3_2012.zip')