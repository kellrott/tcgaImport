#!/usr/bin/env python


"""
Script to scan and extract TCGA data and compile it into coherent matrices

"""

from xml.dom.minidom import parseString
import urllib
import urllib2
import time
import os
import csv
import sys
import hashlib
import tempfile
import re
import copy
import random
import json
import datetime
import hashlib
import subprocess
from glob import glob
import shutil
import subprocess
import logging
from argparse import ArgumentParser
from urlparse import urlparse



"""

Net query code

"""

class dccwsItem(object):
    baseURL = "http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query="

    def __init__(self):
        self.url = None
            
    def __iter__(self):
        next = self.url        
        while next != None:
            retry_count = 3
            while retry_count > 0:
                try:
                    data = None
                    handle = urllib.urlopen(next)
                    data = handle.read()
                    handle.close()
                    dom = parseString(data)
                    retry_count = 0
                except Exception, e:
                    retry_count -= 1
                    if retry_count <= 0:
                        sys.stderr.write("URL %s : Message Error: %s\n" % (next, data ) )
                        raise e
                    time.sleep(random.randint(10, 35))
            # there might not be any archives for a dataset
            if len(dom.getElementsByTagName('queryResponse')) > 0:
                response = dom.getElementsByTagName('queryResponse').pop()
                classList = response.getElementsByTagName('class')
                for cls in classList:
                    className = cls.getAttribute("recordNumber")
                    outData = {}
                    #aObj = Archive()
                    for node in cls.childNodes:
                        nodeName = node.getAttribute("name")
                        if node.hasAttribute("xlink:href"):
                            outData[ nodeName ] = node.getAttribute("xlink:href")            
                        else:
                            outData[ nodeName ] = getText( node.childNodes )
                    yield outData
            if len( dom.getElementsByTagName('next') ) > 0:
                nextElm = dom.getElementsByTagName('next').pop()
                next = nextElm.getAttribute( 'xlink:href' )
            else:
                next = None


class CustomQuery(dccwsItem):
    def __init__(self, query):
        super(CustomQuery, self).__init__()
        if query.startswith("http://"):
            self.url = query
        else:
            self.url = dccwsItem.baseURL + query

    
def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)
    
"""

Build Configuration

"""

class BuildConf:
    def __init__(self, platform, name, version, meta, tarlist):
        self.platform = platform
        self.name = name
        self.version = version
        self.meta = meta
        self.tarlist = tarlist
        self.abbr = ''
        self.uuid_table = None
        if 'annotations' in meta and 'acronym' in meta['annotations']:
            self.abbr = meta['annotations']['acronym']
    
    def addOptions(self, opts):
        self.workdir_base = opts.workdir_base
        self.outdir = opts.outdir
        self.sanitize = opts.sanitize
        self.mirror = opts.mirror
        self.outpath = opts.outpath
        self.download = opts.download
        self.download_only = opts.download_only
        self.metapath = opts.metapath
        self.errorpath = opts.errorpath
        self.clinical_type = opts.clinical_type

        self.clinical_type_map = {}
        for t, path, meta in opts.out_clinical:
            self.clinical_type_map[ "." + t] = (path, meta)
        
        if opts.uuid_table is not None:
            self.uuid_table = {}
            handle = open(opts.uuid_table)
            for line in handle:
                tmp = line.rstrip().split("\t")
                if len(tmp) == 2:
                    self.uuid_table[tmp[0]] = tmp[1]

    def getURLPath(self, url):
        if self.mirror is None:
            print "Define mirror location"
            sys.exit(1)

        src = url # "https://tcga-data.nci.nih.gov/" + url
        path = urlparse(url).path
        dst = os.path.join(self.mirror, re.sub("^/", "", path))
        dir = os.path.dirname(dst)
        if not os.path.exists(dir):
            print "mkdir", dir
            os.makedirs(dir)
        if not os.path.exists( dst ):
            if self.download or self.download_only:    
                print "download %s to %s" % (src, dst)
                urllib.urlretrieve(src, dst)
            else:
                raise Exception("Missing source file: %s" % url)
        return dst




    def buildRequest(self):
        return self.meta
    
    def translateUUID(self, uuid):
        if self.uuid_table is None or uuid not in self.uuid_table:
            return uuid
        return self.uuid_table[uuid]
    
    def getOutPath(self, nameGen):
        """
        if self.outpath is not None:
            return self.outpath
        if name in self.clinical_type_map:
            return self.clinical_type_map[name][0]
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)
        """
        return os.path.join(self.outdir, nameGen(self.name))

    def getOutMeta(self, nameGen):
        """
        if self.outpath is not None:
            if self.metapath is not None:
                return self.metapath
            return self.outpath + ".json"
        if name in self.clinical_type_map:
            return self.clinical_type_map[name][1]
        """
        return os.path.join(self.outdir, nameGen(self.name)) + ".json"

    def getOutError(self, name):
        if self.outpath is not None:
            if self.errorpath is not None:
                return self.errorpath
            return self.outpath + ".error"
        return os.path.join(self.outdir, self.name) + name + ".error"


def getBaseBuildConf(basename, platform, mirror):
    dates = []
    logging.debug("TCGA Query for: %s" % (basename))
    q = tcgaConfig[platform].getArchiveQuery(basename)
    urls = {}
    meta = None
    platform = None
    for e in q:
        dates.append( datetime.datetime.strptime( e['addedDate'], "%m-%d-%Y" ) )
        if meta is None:
            meta = {
                #'name' : basename,
                'annotations' : {}, 
                'species' : 'Homo sapiens',
                'disease' : 'cancer',
                'provenance' : { 'name' : 'tcgaImport', 'used' : [] }
            }            
            for e2 in CustomQuery(e['platform']):
                platform = e2['name']
                meta['platform'] = e2['name']
                meta['annotations']['platformTitle'] = e2['displayName']
            for e2 in CustomQuery(e['disease']):
                meta['annotations']['acronym'] = e2['abbreviation']
                meta['annotations']['diseaseTitle'] = e2['name']
                for e3 in CustomQuery(e2['tissueCollection']):
                    meta['tissue'] = e3['name']
            for e2 in CustomQuery(e['center']):
                meta['annotations']['centerTitle'] = e2['displayName']
                meta['annotations']['center'] = e2['name']
                meta['annotations']['basename'] = basename
        meta['provenance']['used'].append(
            {
                'url' : "https://tcga-data.nci.nih.gov" + e['deployLocation'],
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL"
            }
        )
        urls[ mirror + e['deployLocation'] ] = platform

    logging.debug("TCGA Query for mage-tab: %s" % (basename))
    q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (basename))
    for e in q:
        dates.append( datetime.datetime.strptime( e['addedDate'], "%m-%d-%Y" ) )
        q2 = CustomQuery(e['platform'])
        platform = None
        for e2 in q2:
            logging.debug("%s" % (e2))
            platform = e2['name']
        meta['provenance']['used'].append( 
            {
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                "url" : "https://tcga-data.nci.nih.gov" + e['deployLocation'] 
            }
        )
        urls[ mirror + e['deployLocation'] ] = platform
    
    if len(dates) == 0:
        logging.debug("No Files found")
        return
    dates.sort()
    dates.reverse()
    versionDate = dates[0].strftime( "%Y-%m-%d" )
    
    return BuildConf(platform, basename, versionDate, meta, urls)
    




class TableReader:
    def __init__(self, path):
        self.path = path
    
    def __iter__(self):
        if self.path is not None and os.path.exists(self.path):
            handle = open(self.path)
            for line in handle:
                tmp = line.rstrip().split("\t")
                yield tmp[0], json.loads(tmp[1])
            handle.close()


############
# Importer Classes
############

class FileImporter:
    dataSubTypes = {}
    
    excludes = [
         "MANIFEST.txt$",
         "CHANGES_DCC.txt$",
         "README_DCC.txt$",
         "README.txt$",
         "CHANGES.txt$",
         "DCC_ALTERED_FILES.txt$", 
         r'.wig$',    
         "DESCRIPTIO$"
    ]
    
    def __init__(self, config, build_req):
        self.config = config
        self.build_req = build_req
        
    def extractTars(self):  
        if not os.path.exists(self.config.workdir_base):
            os.makedirs(self.config.workdir_base)      
        self.work_dir = tempfile.mkdtemp(dir=self.config.workdir_base)
        print "Extract to ", self.work_dir
        for record in self.build_req['provenance']['used']:
            url = record['url']
            path = self.config.getURLPath(url)
            if not self.config.download_only:
                subprocess.check_call([ "tar", "xzf", path, "-C", self.work_dir])#, stderr=sys.stdout)
        
    def run(self):        
        self.extractTars()
        if self.config.download_only:
            return
        #scan the magetab
        self.out = {}
        self.ext_meta = {}
        self.scandirs(self.work_dir, None)
        for o in self.out:
            self.out[o].close()
        for dsubtype in self.dataSubTypes:
            print "Extracting: ", dsubtype
            filterInclude = None
            filterExclude = None
            if 'fileInclude' in self.dataSubTypes[dsubtype]:
                filterInclude = re.compile(self.dataSubTypes[dsubtype]['fileInclude'])
            if 'fileExclude' in self.dataSubTypes[dsubtype]:
                filterExclude = re.compile(self.dataSubTypes[dsubtype]['fileExclude'])
            self.inc = 0
            self.errors = []
            self.ext_meta = {}
            self.out = {}
            self.scandirs(self.work_dir, dsubtype, filterInclude=filterInclude, filterExclude=filterExclude)
            for o in self.out:
                self.out[o].close()
            self.fileBuild(dsubtype)
        shutil.rmtree(self.work_dir)       
    
    def checkExclude( self, name ):
        for e in self.excludes:
            if re.search( e, name ):
                return True
        return False
    
    def scandirs(self, path, dataSubType, filterInclude=None, filterExclude=None):
        if os.path.isdir(path):
            for a in glob(os.path.join(path, "*")):
                self.scandirs(a, dataSubType, filterInclude, filterExclude)
        else:
            name = os.path.basename(path)
            if self.isMage(path):
                if dataSubType is None:
                    self.mageScan(path)
            else:
                if dataSubType is not None:
                    if not self.checkExclude(name):
                        if (filterInclude is None or filterInclude.match(name)) and (filterExclude is None or not filterExclude.match(name)):
                            self.fileScan(path, dataSubType)
                        
    def isMage(self, path):
        if path.endswith( '.sdrf.txt' ) or path.endswith( '.idf.txt' ) or path.endswith("DESCRIPTION.txt"):
            return True

    
    def emit(self, key, data, port):
        if port not in self.out:
            self.out[port] = open(self.work_dir + "/" + port, "w")
        self.out[port].write( "%s\t%s\n" % (key, json.dumps(data)))

    def emitFile(self, dataSubType, meta, file):
        md5 = hashlib.md5()
        oHandle = open(self.config.getOutPath(self.dataSubTypes[dataSubType]['nameGen']), "wb")
        with open(file,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), ''): 
                md5.update(chunk)
                oHandle.write(chunk)
        oHandle.close()
        md5str = md5.hexdigest()
        meta['md5'] = md5str
        mHandle = open(self.config.getOutMeta(self.dataSubTypes[dataSubType]['nameGen']), "w")
        mHandle.write( json.dumps(meta))
        mHandle.close()
        if len(self.errors):
            eHandle = open( self.config.getOutError(dataSubType), "w" )
            for msg in self.errors:
                eHandle.write( msg + "\n" )
            eHandle.close()
    
    def addError(self, msg):
        self.errors.append(msg)

        
commonMap = {
    "mean" : "seg.mean",
    "Segment_Mean" : "seg.mean",
    "Start" : "loc.start",
    "End" : "loc.end",
    "Chromosome" : "chrom"
}


idfMap = {
    "Investigation Title" : "title",
    "Experiment Description" : "experimentalDescription",
    "Person Affiliation" : "dataProducer",
    "Date of Experiment" : "experimentalDate"
}

class TCGAGeneticImport(FileImporter):      
    
    def mageScan(self, path):
        if path.endswith(".sdrf.txt"):
            iHandle = open(path, "rU")
            read = csv.reader( iHandle, delimiter="\t" )
            colNum = None
            for row in read:
                if colNum is None:
                    colNum = {}
                    for i in range(len(row)):
                        colNum[ row[i] ] = i
                else:
                    if not colNum.has_key("Material Type") or ( not row[ colNum[ "Material Type" ] ] in [ "genomic_DNA", "total_RNA", "MDA cell line" ] ):
                        try:
                            if colNum.has_key( "Derived Array Data File" ):
                                self.emit( row[ colNum[ "Derived Array Data File" ] ].split('.')[0], row[ colNum[ "Extract Name" ] ], "targets" )
                                self.emit( row[ colNum[ "Derived Array Data File" ] ], row[ colNum[ "Extract Name" ] ], "targets" )
                            if colNum.has_key("Derived Array Data Matrix File" ):
                                self.emit( row[ colNum[ "Derived Array Data Matrix File" ] ], row[ colNum[ "Extract Name" ] ], "targets" )    
                            if colNum.has_key( "Derived Data File"):
                                self.emit( row[ colNum[ "Derived Data File" ] ].split('.')[0], row[ colNum[ "Extract Name" ] ], "targets" )  
                                self.emit( row[ colNum[ "Derived Data File" ] ], row[ colNum[ "Extract Name" ] ], "targets" )    
                            if colNum.has_key( "Hybridization Name" ):
                                self.emit( row[ colNum[ "Hybridization Name" ] ] , row[ colNum[ "Extract Name" ] ], "targets" )
                            if colNum.has_key( "Sample Name" ):
                                self.emit( row[ colNum[ "Sample Name" ] ] , row[ colNum[ "Extract Name" ] ], "targets" )
                            self.emit( row[ colNum[ "Extract Name" ] ] , row[ colNum[ "Extract Name" ] ], "targets" )
                        except IndexError:
                            pass #there can be blank lines in the SDRF
        if path.endswith(".idf.txt"):
            iHandle = open(path)
            for line in iHandle:
                row = line.split("\t")
                if len(row):
                    if row[0] in idfMap:
                        self.ext_meta[ idfMap[row[0]] ] = row[1]
            iHandle.close()
        if path.endswith("DESCRIPTION.txt"):
            handle = open(path)
            self.description = handle.read()
            handle.close()
    
    @staticmethod
    def getOutputList():
        yield "default"

    @staticmethod
    def getArchiveQuery(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=Level_3]]" % (basename))
        for e in q:
            yield e
    
    @staticmethod
    def getMageQuery(basename):
        q = "Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (basename)
        for e in q:
            yield e

    @staticmethod
    def getArchiveUrls(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=Level_3]]" % (basename))
        for e in q:
            yield e['deployLocation']

    @staticmethod
    def getMageUrl(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (basename))
        out = None
        for e in q:
            out = e['deployLocation']
        return out

    @staticmethod
    def getArchiveList(platform):
        q = CustomQuery("Archive[Platform[@name=%s]][@isLatest=1]" % platform)
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                yield name
                out[name] = True


    def translateUUID(self, uuid):
        return self.config.translateUUID(uuid)
    
    def getTargetMap(self):
        subprocess.call("sort -k 1 %s/targets > %s/targets.sort" % (self.work_dir, self.work_dir), shell=True)                
        handle = TableReader(self.work_dir + "/targets.sort")
        tTrans = {}
        for key, value in handle:
            tTrans[ key ] = value
        return tTrans
    
    def fileScan(self, path, dataSubType):
        """
        This function takes a TCGA level 3 genetic file (file name and input handle),
        and tries to extract probe levels or target mappings (experimental ID to TCGA barcode)
        it emits these values to a handle, using the 'targets' and 'probes' string to identify 
        the type of data being emited
        """
        iHandle = open(path)
        mode = None
        #modes
        #1 - segmentFile - one sample per file/no sample info inside file
        #2 - two col header matrix file
        #3 - segmentFile - sample information inside file
        target = None
        colName = None
        colType = None
        for line in iHandle:
            if colName is None:
                colName = line.rstrip().split("\t")                     
                if colName[0] == "Hybridization REF" or colName[0] == "Sample REF":
                    mode=2
                elif colName[0] == "Chromosome" or colName[0] == "chromosome":
                    mode=1
                    target=os.path.basename( path ).split('.')[0] #seg files are named by the filename before the '.' extention
                elif colName[1] == "chrom":
                    mode = 3
                    target=os.path.basename( path ).split('.')[0] #seg files are named by the filename before the '.' extention
                    
                for i in range(len(colName)):
                    if commonMap.has_key( colName[i] ):
                        colName[i] = commonMap[ colName[i] ]
            elif mode==2 and colType is None:
                colType=line.rstrip().split("\t")
                for i in range(len(colType)):
                    if commonMap.has_key( colType[i] ):
                        colType[i] = commonMap[ colType[i] ]
            else:
                tmp = line.rstrip().split("\t")
                if mode == 2:
                    out={}
                    for col in colName[1:]:
                        out[ col ] = { "target" : col }
                    for i in range(1,len(colType)):
                        try:
                            if colType[i] in self.dataSubTypes[dataSubType]['probeFields']:
                                out[ colName[i] ][ colType[i] ] = tmp[i]
                        except IndexError:
                            out[ colName[i] ][ colType[i] ] = "NA"
                    for col in out:
                        self.emit( tmp[0], out[col], dataSubType + ".probes" )
                else:
                    out = {}
                    for i in range(len(colName)):
                        out[ colName[i] ] = tmp[i]
                    out['file'] = os.path.basename(path)
                    if mode==1:
                        self.emit( target, out,  dataSubType + ".segments" )
                    elif mode == 3:
                        self.emit( tmp[0], out,  dataSubType + ".segments" )
                    else:
                        self.emit( tmp[0], out,  dataSubType + ".probes" )

        
def get_field_match(value, fields):
    for f in fields:
        if f in value:
            return value[f]

class TCGASegmentImport(TCGAGeneticImport):
    
    
    def fileScan(self, path, dataSubType):
        """
        This function takes a TCGA level 3 genetic file (file name and input handle),
        and tries to extract probe levels or target mappings (experimental ID to TCGA barcode)
        it emits these values to a handle, using the 'targets' and 'probes' string to identify 
        the type of data being emited
        """
        iHandle = open(path)
        mode = None
        #modes
        #1 - segmentFile - one sample per file/no sample info inside file
        #2 - segmentFile - sample information inside file
        target = None
        colName = None
        colType = None
        for line in iHandle:
            if colName is None:
                colName = line.rstrip().split("\t")                     
                if colName[0] == "Chromosome" or colName[0] == "chromosome":
                    mode=1
                    target=os.path.basename( path ).split('.')[0] #seg files are named by the filename before the '.' extention
                elif colName[1] == "chrom":
                    mode = 2
                    
                for i in range(len(colName)):
                    if commonMap.has_key( colName[i] ):
                        colName[i] = commonMap[ colName[i] ]
            else:
                tmp = line.rstrip().split("\t")
                out = {}
                for i in range(len(colName)):
                    out[ colName[i] ] = tmp[i]
                out['file'] = os.path.basename(path)
                if mode==1:
                    self.emit( target, out,  dataSubType + ".segments" )
                elif mode == 2:
                    self.emit( tmp[0], out,  dataSubType + ".segments" )

    
    def getMeta(self, name, dataSubType):
        matrixInfo = { 
            'name' : name + "." + dataSubType + ".bed", 
            'annotations' : {
                'filetype' : 'bed5', 
                "lastModified" : self.config.version,
                'rowKeySrc' : "tcga.%s" % (self.config.abbr),
                'dataSubType' : dataSubType,
                'dataProducer' : 'TCGA',
            }
        }
        matrixInfo = dict_merge(matrixInfo, self.ext_meta)
        matrixInfo = dict_merge(matrixInfo, self.config.meta)
        return matrixInfo
    
    def fileBuild(self, dataSubType):
        #use the target table to create a name translation table
        #also setup target name enumeration, so they will have columns
        #numbers

        tTrans = self.getTargetMap()        
        subprocess.call("sort -k 1 %s/%s.segments > %s/%s.segments.sort" % (self.work_dir, dataSubType, self.work_dir, dataSubType), shell=True)
        sHandle = TableReader(self.work_dir + "/%s.segments.sort" % (dataSubType))

        segFile = None
        curName = None
        
        curData = {}
        missingCount = 0

        startField  = ["loc.start", "Start"]
        endField    = ["loc.end", "End"]
        valField    = ["seg.mean", "Segment_Mean"]
        chromeField = ["chrom", "Chromosome"]
        
        segFile = None

        for key, value in sHandle:
            if segFile is None:
                segFile = open("%s/%s.segment_file"  % (self.work_dir, dataSubType), "w")
            try:
                curName = self.translateUUID(tTrans[key]) # "-".join( tTrans[ key ].split('-')[0:4] )
                if curName is not None:
                    try:
                        chrom = get_field_match(value, chromeField).lower()
                        if not chrom.startswith("chr"):
                            chrom = "chr" + chrom
                        chrom = chrom.upper().replace("CHR", "chr")
                        #segFile.write( "%s\t%s\t%s\t%s\t.\t%s\n" % ( curName, chrom, int(value[ startField ])+1, value[ endField ], value[ valField ] ) )
                        segFile.write( "%s\t%s\t%s\t%s\t%s\n" % ( 
                            chrom, 
                            int(get_field_match(value, startField))-1, 
                            get_field_match(value, endField), curName, 
                            get_field_match( value, valField ) ) 
                        )
                    except KeyError:
                         self.addError( "Field error: %s" % (str(value)))
            except KeyError:
                self.addError( "TargetInfo Not Found: %s" % (key))
            
        segFile.close()
        matrixName = self.config.name

        self.emitFile( dataSubType, self.getMeta(matrixName, dataSubType), "%s/%s.segment_file"  % (self.work_dir, dataSubType) )     


def dict_merge(x, y):
    #print "dict", x, y
    result = dict(x)
    for k,v in y.iteritems():
        if k in result:
            if result[k] != v:
                result[k] = dict_merge(result[k], v)
        else:
            result[k] = v
    return result

class TCGAMatrixImport(TCGAGeneticImport):
    
    def getMeta(self, name, dataSubType):
        matrixInfo = { 
            'annotations' : {
                'fileType' : 'genomicMatrix',
                "lastModified" : self.config.version,
                'dataSubType' : dataSubType,
                'dataProducer' : 'TCGA', 
                'rowKeySrc' : self.dataSubTypes[dataSubType]['probeMap'],
                'columnKeySrc' : "tcga.%s" % (self.config.abbr)
            }, 
            'name' : name + "." + dataSubType + ".tsv", 
        }
        matrixInfo = dict_merge(matrixInfo, self.ext_meta)
        matrixInfo = dict_merge(matrixInfo, self.config.meta)
        return matrixInfo
        
    def fileBuild(self, dataSubType):
        #use the target table to create a name translation table
        #also setup target name enumeration, so they will have columns
        #numbers        
        
        subprocess.call("sort -k 1 %s/%s.probes > %s/%s.probes.sort" % (self.work_dir, dataSubType, self.work_dir, dataSubType), shell=True)
        subprocess.call("sort -k 1 %s/targets > %s/targets.sort" % (self.work_dir, self.work_dir), shell=True)
                
        handles = {}
        handles[ "geneticExtract:targets" ] = TableReader(self.work_dir + "/targets.sort")
        handles[ "geneticExtract:%s.probes" % (dataSubType) ] = TableReader(self.work_dir + "/%s.probes.sort" % (dataSubType))

        tTrans = self.getTargetMap()
        
        tEnum = {}
        for t in tTrans:
            tlabel = self.translateUUID(tTrans[t])
            if tlabel is not None and tlabel not in tEnum:
                tEnum[tlabel] = len(tEnum)
                
        matrixFile = None
        segFile = None

        curName = None
        curData = {}
        missingCount = 0
        rowCount = 0
        pHandle = handles["geneticExtract:%s.probes" % (dataSubType)]
        for key, value in pHandle:
            if matrixFile is None:
                matrixFile = open("%s/%s.matrix_file" % (self.work_dir, dataSubType), "w" )            
                out = ["NA"] * len(tEnum)
                for target in tEnum:
                    out[ tEnum[ target ] ] = target
                matrixFile.write( "%s\t%s\n" % ( "#probe", "\t".join( out ) ) )        
            
            if curName != key:
                if curName is not None:
                    out = ["NA"] * len(tEnum)
                    for target in curData:
                        try:
                            ttarget = self.translateUUID(tTrans[target])
                            if ttarget is not None:
                                out[ tEnum[ ttarget ] ] = str( curData[ target ] )
                        except KeyError:
                            self.addError( "TargetInfo Not Found: %s" % (target))
                    if out.count("NA") != len(tEnum):
                        rowCount += 1
                        matrixFile.write( "%s\t%s\n" % ( curName, "\t".join( out ) ) )  
                curName = key
                curData = {}
            if "target" in value:
                for probeField in self.dataSubTypes[dataSubType]['probeFields']:
                    if probeField in value:
                        curData[ value[ "target" ] ] = value[ probeField ]
            elif "file" in value:
                for probeField in self.dataSubTypes[dataSubType]['probeFields']:
                    if probeField in value:
                        curData[ value[ "file" ] ] = value[ probeField ]
        matrixFile.close()
        matrixName = self.config.name    
        if rowCount > 0:
            self.emitFile( dataSubType, self.getMeta(matrixName, dataSubType), "%s/%s.matrix_file"  % (self.work_dir, dataSubType) )


adminNS = "http://tcga.nci/bcr/xml/administration/2.3"


def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
                if child.nodeType == child.ELEMENT_NODE:
                    if child.localName == stack[0]:
                        for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                            yield out
                    elif '*' == stack[0]:
                        for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                            yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )
                
class TCGAClinicalImport(FileImporter):
    
    def fileScan(self, path, dataSubType):
        print "Parsing", dataSubType, path
        handle = open(path)
        data = handle.read()
        handle.close()
        xml=parseString(data)
        self.parseXMLFile(xml, dataSubType)
            
    def getText(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    @staticmethod
    def getOutputList():
        return ["patient", "aliquot", "analyte", "portion", "sample", "drugs", "radiation", "followup"]

    @staticmethod
    def getArchiveQuery(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1]" % (basename))
        for e in q:
            yield e


    @staticmethod
    def getArchiveUrls(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][platform[@alias=bio]]" % (basename))
        for e in q:
            yield e['deployLocation']

    @staticmethod
    def getMageUrl(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (basename))
        out = None
        for e in q:
            out = e['deployLocation']
        return out

    def parseXMLFile(self, dom, dataSubType):    
        root_node = dom.childNodes[0]
        admin = {}
        for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/admin/*"):
            admin[stack[-1]] = { 'value' : text }
        
        patient_barcode = None
        for node, stack, attr, text in dom_scan(root_node, 'tcga_bcr/patient/bcr_patient_barcode'):
            patient_barcode = text
        
        patient_data = {}
        for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/*"):
            if 'xsd_ver' in attr:
                #print patientName, stack[-1], attr, text
                p_name = attr.get('preferred_name', stack[-1])
                if len(p_name) == 0:
                    p_name = stack[-1]
                patient_data[p_name] = { "value" : text }
        if dataSubType == "patient":
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/stage_event/*"):
                if 'xsd_ver' in attr:
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(p_name) == 0:
                        p_name = stack[-1]
                    patient_data[p_name] = { "value" : text }
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/stage_event/*/*"):
                if 'xsd_ver' in attr:
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(p_name) == 0:
                        p_name = stack[-1]
                    patient_data[p_name] = { "value" : text }
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/stage_event/tnm_categories/*/*"):
                if 'xsd_ver' in attr:
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(p_name) == 0:
                        p_name = stack[-1]
                    patient_data[p_name] = { "value" : text }
            self.emit( patient_barcode, patient_data, "patient" )
        
        if dataSubType == "sample":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample"):
                sample_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/bcr_sample_barcode"):
                    sample_barcode = c_text
                sample_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/*"):
                    if 'xsd_ver' in c_attr:
                        sample_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( sample_barcode, sample_data, "sample" )

        if dataSubType == "portion":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion"):
                portion_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/bcr_portion_barcode"):
                    portion_barcode = c_text
                portion_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/*"):
                    if 'xsd_ver' in c_attr:
                        portion_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( portion_barcode, portion_data, "portion" )
        
        if dataSubType == "analyte":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte"):
                analyte_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/bcr_analyte_barcode"):
                    analyte_barcode = c_text
                analyte_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/*"):
                    if 'xsd_ver' in c_attr:
                        analyte_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( analyte_barcode, analyte_data, "analyte" )

        if dataSubType == "aliquot":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte/aliquots/aliquot"):
                aliquot_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/bcr_aliquot_barcode"):
                    aliquot_barcode = c_text
                aliquot_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/*"):
                    if 'xsd_ver' in c_attr:
                        aliquot_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( aliquot_barcode, aliquot_data, "aliquot" )
        
        if dataSubType == "drug":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/drugs/drug"):
                drug_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/bcr_drug_barcode"):
                    drug_barcode = c_text
                drug_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/*"):
                    if 'xsd_ver' in c_attr:
                        drug_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( drug_barcode, drug_data, "drug" )

        if dataSubType == "radiation":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/radiations/radiation"):
                radiation_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/bcr_radiation_barcode"):
                    radiation_barcode = c_text
                radiation_data = {}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/*"):
                    if 'xsd_ver' in c_attr:
                        radiation_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( radiation_barcode, radiation_data, "radiation" )

        if dataSubType == "followup":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/follow_ups/follow_up"):
                follow_up_barcode = None
                sequence = s_attr['sequence']
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "follow_up/bcr_followup_barcode"):
                    follow_up_barcode = c_text
                follow_up_data = { "sequence" : {"value" : sequence}}    
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "follow_up/*"):
                    if 'xsd_ver' in c_attr:
                        follow_up_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
                self.emit( follow_up_barcode, follow_up_data, "followup" )

            

    def getMeta(self, name, dataSubType):
        fileInfo = {
            "name" : name + "." + dataSubType + ".tsv",
            "annotations" : {
                "fileType" : "clinicalMatrix",
                "lastModified" :  self.config.version,
                'dataSubType' : dataSubType,
                "rowKeySrc" : "tcga.%s" % (self.config.abbr)
            }            
        }
        
        fileInfo = dict_merge(fileInfo, self.ext_meta)
        fileInfo = dict_merge(fileInfo, self.config.meta)
        return fileInfo
    
    def fileBuild(self, dataSubType):

        if os.path.exists( "%s/%s" % (self.work_dir, dataSubType)):
            subprocess.call("cat %s/%s | sort -k 1 > %s/%s.sort" % (self.work_dir, dataSubType, self.work_dir, dataSubType), shell=True)
            handle = TableReader(self.work_dir + "/" + dataSubType + ".sort")
            matrix = {}
            colEnum = {}
            for key, value in handle:
                if key not in matrix:
                    matrix[key] = {}
                for col in value:
                    matrix[key][col] = value[col]
                    if col not in colEnum:
                        if not self.config.sanitize or col not in [ 'race', 'ethnicity' ]:
                            colEnum[col] = len(colEnum)
            
            handle = open( os.path.join(self.work_dir, dataSubType + "_file"), "w")
            cols = [None] * (len(colEnum))
            for col in colEnum:
                cols[colEnum[col]] = col
            handle.write("sample\t%s\n" % ("\t".join(cols)))
            for key in matrix:
                cols = [""] * (len(colEnum))
                for col in colEnum:
                    if col in matrix[key]:
                        cols[colEnum[col]] = matrix[key][col]['value']
                handle.write("%s\t%s\n" % (key, "\t".join(cols).encode("ASCII", "replace")))
            handle.close()
            self.emitFile( dataSubType, self.getMeta(self.config.name, dataSubType), "%s/%s_file"  % (self.work_dir, dataSubType)) 


class AgilentImport(TCGAMatrixImport):
    dataSubTypes = { 
        'geneExp' : { 
            'probeMap' : 'hugo',
            'sampleMap' : 'tcga.iddag',
            'dataType'  : 'genomicMatrix',
            'probeFields' : ['log2 lowess normalized (cy5/cy3) collapsed by gene symbol'],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.geneExp.tsv" % (x)
        }
    }
   

class CGH1x1mImport(TCGASegmentImport):
    dataSubTypes = { 
        'cna' : {
            "sampleMap" : 'tcga.iddag',
            "dataType" : 'genomicSegment',
            "probeFields" : ['seg.mean'],
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }

class SNP6Import(TCGASegmentImport):
    assembly = 'hg19'
    dataSubTypes = { 
        'cna' : { 
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['seg.mean'],
            'fileInclude' : r'^.*\.hg19.seg.txt$',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.hg19.cna.bed" % (x)
        },
        'cna_nocnv' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['seg.mean'],
            'fileInclude' : r'^.*\.nocnv_hg19.seg.txt$',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.hg19.cna_nocnv.bed" % (x)
        },
        'cna_probecount' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Num_Probes'],
            'fileInclude' : r'^.*\.hg19.seg.txt$',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.hg19.cna_probecount.bed" % (x)
        },
        'cna_nocnv_probecount' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Num_Probes'],
            'fileInclude' : r'^.*\.nocnv_hg19.seg.txt$',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.hg19.cna_nocnv_probecount.bed" % (x)
        }
    }
    
    def fileScan(self, path, dataSubType):
        handle = open(path)
        colName = None
        for line in handle:
            if colName is None:
                colName = line.rstrip().split("\t")                     
                for i, col in enumerate(colName):
                    if commonMap.has_key( col ):
                        colName[i] = commonMap[ col ]
            else:
                tmp = line.rstrip().split("\t")
                out = {}
                for i in range(1, len(colName)):
                    out[ colName[i] ] = tmp[i]
                self.emit( tmp[0], out, dataSubType )
        handle.close()
    
    def fileBuild(self, dataSubType):
        tmap = self.getTargetMap()  
        
        subprocess.call("sort -k 1 %s/%s > %s/%s.sort" % (self.work_dir, dataSubType, self.work_dir, dataSubType), shell=True)                
        handle = TableReader(self.work_dir + "/%s.sort" % (dataSubType))

        segFile = None
        curName = None
        curData = {}
        missingCount = 0

        startField  = ["loc.start", "Start"]
        endField    = ["loc.end", "End"]
        valField    = [self.dataSubTypes[dataSubType]['probeFields'][0], "Segment_Mean"]
        chromeField = ["chrom", "Chromosome"]
            
        segFile = None
        sHandle = handle
        for key, value in sHandle:
            if segFile is None:
                segFile = open("%s/%s.out"  % (self.work_dir, dataSubType), "w")
            try:
                curName = self.translateUUID(tmap[key])
                if curName is not None:
                    chrom = get_field_match(value, chromeField).lower()
                    if not chrom.startswith("chr"):
                        chrom = "chr" + chrom
                    chrom = chrom.upper().replace("CHR", "chr")
                    segFile.write( "%s\t%s\t%s\t%s\t%s\n" % ( 
                        chrom, get_field_match(value, startField), 
                        get_field_match(value, endField), 
                        curName, get_field_match(value, valField ) ) 
                    )
            except KeyError:
                self.addError( "TargetInfo Not Found: %s" % (key))
            
        segFile.close()
        meta = self.getMeta(self.config.name + ".hg19", dataSubType)
        meta['assembly'] = { "@id" : 'hg19' }
        self.emitFile(dataSubType, meta, "%s/%s.out"  % (self.work_dir, dataSubType))
       

class HmiRNAImport(TCGAMatrixImport):
    dataSubTypes = { 
        'miRNAExp' : {
            'probeMap' : 'agilentHumanMiRNA',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'probeFields' : ['unc_DWD_Batch_adjusted'],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.miRNAExp.tsv" % (x)
        }
    }
    
class CGH244AImport(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Segment_Mean'],
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }

class CGH415K_G4124A(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'chromeField' : 'Chromosome',
            'dataType' : 'genomicSegment',
            'endField' : 'End',
            'probeFields' : ['Segment_Mean'],
            'startField' : 'Start',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }

class IlluminaHiSeq_DNASeqC(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'chromeField' : 'Chromosome',
            'dataType' : 'genomicSegment',
            'endField' : 'End',
            'probeFields' : ['Segment_Mean'],
            'startField' : 'Start',
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }
    
    def translateUUID(self, uuid):
        out = self.config.translateUUID(uuid)
        #censor out normal ids
        if re.search(r'^TCGA-..-....-1', out):
            return None
        return out

class HT_HGU133A(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'probeMap' : 'affyU133a',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'probeFields' : ['Signal'],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.geneExp.tsv" % (x)
        }
    }

class HuEx1_0stv2(TCGAMatrixImport):
    dataSubTypes = {
        'miRNAExp' : {
            'probeMap' : 'hugo',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'probeFields' : ['Signal'],
            'fileInclude' : '^.*gene.txt$|^.*sdrf.txt$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.miRNAExp.tsv" % (x)
        }
    }

class Human1MDuoImport(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['mean'],
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }

class HumanHap550(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['mean'],
            'extension' : 'bed',
            'nameGen' : lambda x : "%s.cna.bed" % (x)
        }
    }

class HumanMethylation27(TCGAMatrixImport):
    dataSubTypes = {
        'betaValue' : {
            'probeMap' : 'illuminaMethyl27K_gpl8490',
            'sampleMap' :  'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'fileExclude' : '.*.adf.txt',
            'probeFields' : ['Beta_Value', 'Beta_value'],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.betaValue.tsv" % (x)
        }
    }
    

class HumanMethylation450(TCGAMatrixImport):
    dataSubTypes =  {
        'betaValue' : {
            'probeMap' :  'illuminaHumanMethylation450',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'fileExclude' : '.*.adf.txt',
            'probeFields' :  ['Beta_value', 'Beta_Value'],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.betaValue.tsv" % (x)
        }
    }

    def fileScan(self, path, dataSubType):
        """
        This function takes a TCGA level 3 genetic file (file name and input handle),
        and tries to extract probe levels or target mappings (experimental ID to TCGA barcode)
        it emits these values to a handle, using the 'targets' and 'probes' string to identify 
        the type of data being emited
        """
        iHandle = open(path)
        mode = None
        #modes
        #1 - two col header matrix file
        target = None
        colName = None
        colType = None
        for line in iHandle:
            if colName is None:
                colName = line.rstrip().split("\t")                     
                if colName[0] == "Hybridization REF" or colName[0] == "Sample REF":
                    mode=1                    
                for i in range(len(colName)):
                    if commonMap.has_key( colName[i] ):
                        colName[i] = commonMap[ colName[i] ]
            elif mode==1 and colType is None:
                colType=line.rstrip().split("\t")
                for i in range(len(colType)):
                    if commonMap.has_key( colType[i] ):
                        colType[i] = commonMap[ colType[i] ]
            else:
                tmp = line.rstrip().split("\t")
                if mode == 1:
                    out={}
                    for col in colName[1:]:
                        out[ col ] = { "target" : col }
                    for i in range(1,len(colType)):
                        try:
                            if colType[i] in self.dataSubTypes[dataSubType]['probeFields']:
                                out[ colName[i] ][ colType[i] ] = "%.4f" % float(tmp[i])
                        except IndexError:
                            out[ colName[i] ][ colType[i] ] = "NA"
                        except ValueError:
                            out[ colName[i] ][ colType[i] ] = "NA"
                    for col in out:
                        self.emit( tmp[0], out[col], dataSubType + ".probes" )
                
class Illumina_RNASeq(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*\.gene.quantification.txt$|^.*sdrf.txt$',
            'probeFields' : ['RPKM'],
            'probeMap' : 'hugo.unc',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.geneExp.tsv" % (x)
        }
    }

class Illumina_RNASeqV2(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*rsem.genes.normalized_results$|^.*sdrf.txt$',
            'probeFields' : ['normalized_count'],
            'probeMap' : 'hugo.unc',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.geneExp.tsv" % (x)
        },
        'isoformExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*rsem.isoforms.results$',
            'probeFields' : ['raw_count'],
            'probeMap' : 'ucsc.id',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.isoformExp.tsv" % (x)
        }
    }

class IlluminaHiSeq_RNASeq(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*gene.quantification.txt$',
            'probeFields' : ['RPKM'],
            'probeMap' : 'hugo.unc',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.geneExp.tsv" % (x)
        }
    }

class MDA_RPPA_Core(TCGAMatrixImport):
    dataSubTypes = {
        "RPPA" : {
            'sampleMap' : 'tcga.iddag',
            'probeMap' : "md_anderson_antibodies",
            'fileExclude' : r'^.*.antibody_annotation.txt|^.*array_design.txt$',
            'probeFields' : [ 'Protein Expression', 'Protein.Expression' ],
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.RPPA.tsv" % (x)
        }
    }

    def getTargetMap(self):
        subprocess.call("sort -k 1 %s/targets > %s/targets.sort" % (self.work_dir, self.work_dir), shell=True)                
        handle = TableReader(self.work_dir + "/targets.sort")
        tTrans = {}
        for key, value in handle:
            value = re.sub(r'\.SD', '', value)
            tTrans[ key ] = value
        return tTrans


class Illumina_miRNASeq(TCGAMatrixImport):
    dataSubTypes = {
        'miRNAExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '^.*.mirna.quantification.txt$',
            'probeFields' : ['reads_per_million_miRNA_mapped'],
            'probeMap' : 'hsa.mirna',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.miRNAExp.tsv" % (x)
        }
    }

class bioImport(TCGAClinicalImport):
    dataSubTypes = {
        "patient" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.patient.tsv" % (x)		
        }, 
        "sample" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.sample.tsv" % (x)
        }, 
        "radiation" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.radiation.tsv" % (x)
        }, 
        "drug" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.drug.tsv" % (x)
        }, 
        "portion" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.portion.tsv" % (x)
        }, 
        "analyte" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.analyte.tsv" % (x)
        }, 
        "aliquot" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.aliquot.tsv" % (x)
        }, 
        "followup" : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$',
            'extension' : 'tsv',
            'nameGen' : lambda x : "%s.followup.tsv" % (x)
        } 
    }

class MafImport(FileImporter):
    dataSubTypes = {
        'maf' : {
            'fileInclude' : '.*.maf$',
            'extension' : 'maf',
            'nameGen' : lambda x : "%s.maf" % (x)
        }
    }

    def getMeta(self, name, dataSubType):
        fileInfo = {
            "name" : name + "." + dataSubType,
            "annotations" : {
                "dataSubType" : "mutation",
                "fileType" : "maf",
                "lastModified" :  self.config.version,
            }
        }
        fileInfo = dict_merge(fileInfo, self.ext_meta)
        fileInfo = dict_merge(fileInfo, self.config.meta)
        return fileInfo
    
    def fileScan(self, path, dataSubType):
        self.emitFile(dataSubType, self.getMeta(self.config.name, dataSubType), path)

    def mageScan(self, path):
        if path.endswith(".idf.txt"):
            iHandle = open(path)
            for line in iHandle:
                row = line.split("\t")
                if len(row):
                    if row[0] in idfMap:
                        self.ext_meta[ idfMap[row[0]] ] = row[1]
            iHandle.close()
        if path.endswith("DESCRIPTION.txt"):
            handle = open(path)
            self.description = handle.read()
            handle.close()

    @staticmethod    
    def getOutputList():
        return ["default"]

    @staticmethod
    def getArchiveQuery(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1]" % (basename))
        for e in q:
            u = e['deployLocation']
            if u.count("anonymous"):
                yield e
    
    @staticmethod
    def getArchiveQuery(basename):
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1]" % (basename))
        for e in q:
            u = e['deployLocation']
            if u.count("anonymous"):
                yield e
    

    @staticmethod
    def getArchiveUrls(basename):
        q = CustomQuery("Archive[@isLatest=1][@baseName=%s]" % (basename))
        for e in q:
            u = e['deployLocation']
            if u.count("anonymous"):
                yield u

    @staticmethod
    def getMageUrl(basename):
        q = CustomQuery("Archive[@isLatest=1][@baseName=%s][ArchiveType[@type=mage-tab]]" % (basename))
        out = None
        for e in q:
            u = e['deployLocation']
            if u.count("anonymous"):
                out = u
        return out

    @staticmethod
    def getArchiveList(platform):
        q = CustomQuery("Archive[Platform[@name=%s]][@isLatest=1]" % platform)
        out = {}
        for e in q:
            if  e['deployLocation'].count("anonymous"):
                name = e['baseName']
                if name not in out:
                    yield name
                    out[name] = True


    def fileBuild(self, dataSubType):
        print "File building"            
        
tcgaConfig = {
    'AgilentG4502A_07' : AgilentImport,
    'AgilentG4502A_07_1' : AgilentImport,
    'AgilentG4502A_07_2' : AgilentImport,
    'AgilentG4502A_07_3': AgilentImport,
    'CGH-1x1M_G4447A': CGH1x1mImport,    
    'Genome_Wide_SNP_6': SNP6Import,
    'H-miRNA_8x15K': HmiRNAImport,
    'H-miRNA_8x15Kv2': HmiRNAImport,
    'HG-CGH-244A': CGH244AImport,
    'HG-CGH-415K_G4124A': CGH415K_G4124A,
    'HT_HG-U133A': HT_HGU133A,
    'HG-U133_Plus_2' : HT_HGU133A,
    'HuEx-1_0-st-v2': HuEx1_0stv2,
    'Human1MDuo': Human1MDuoImport,
    'HumanHap550': HumanHap550,
    'IlluminaHiSeq_DNASeqC' : IlluminaHiSeq_DNASeqC,
    'HumanMethylation27': HumanMethylation27,
    'HumanMethylation450': HumanMethylation450,
    'IlluminaHiSeq_RNASeq': IlluminaHiSeq_RNASeq,
    'IlluminaGA_RNASeq' : Illumina_RNASeq,
    'IlluminaGA_RNASeqV2' : Illumina_RNASeqV2,
    'IlluminaGA_mRNA_DGE' : Illumina_RNASeq,
    'IlluminaHiSeq_RNASeqV2' : Illumina_RNASeqV2,
    'MDA_RPPA_Core' : MDA_RPPA_Core,
    'IlluminaGA_miRNASeq' : Illumina_miRNASeq,
    'IlluminaHiSeq_miRNASeq' : Illumina_miRNASeq,
    'bio' : bioImport,
    'IlluminaGA_DNASeq' : MafImport,
    'SOLiD_DNASeq' : MafImport,
    'ABI' : MafImport,
    'Mutation Calling' : MafImport
}



############
# Utility Functions
############

def fileDigest( file ):
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), ''): 
            md5.update(chunk)
    return md5.hexdigest()


def platform_list():
    #q = CustomQuery("Platform")
    #for e in q:
    #    yield e['name']
    for a in tcgaConfig:
        yield a

def supported_list():
    q = CustomQuery("Platform")
    for e in q:
        if e['name'] in tcgaConfig:
            yield e['name']

def archive_list(platform=None):
    if platform is None:
        q = CustomQuery("Archive[@isLatest=1][ArchiveType[@type=Level_3]]")
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                out[name] = True
        return out.keys()
    else:
        q = CustomQuery("Archive[@isLatest=1][ArchiveType[@type=Level_3]][Platform[@alias=%s]]" % (platform))
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                out[name] = True
        return out.keys()


def main_list(options):
    #################
    #list operations
    #################
    if options.list_type == "platform":
        for e in platform_list():
            print e
    
    if options.list_type == "supported":
        for e in supported_list():
            print e
    
    """    
    if options.platform:
        for name in tcgaConfig[options.platform].getArchiveList(options.platform):
            print name
    """


    if options.list_type == "archives":
        for c in archive_list():
            print c

    if options.list_type == "clinical":
        q = CustomQuery("Archive[@isLatest=1][Platform[@alias=bio]]")
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                print name
                out[name] = True

    if options.list_type == "mutation":
        q = CustomQuery("Archive[@isLatest=1][Platform[@alias=Mutation Calling]]")
        out = {}
        for e in q:
            if e['deployLocation'].count("anonymous"):
                name = e['baseName']
                if name not in out:
                    print name
                    out[name] = True        

    """
    if options.list_platform_outputs:
        for p in tcgaConfig[options.list_platform_outputs].getOutputList():
            print p
    """


    if options.list_type == "cancer":
        if options.name is None:
            q = CustomQuery("Disease")
            for e in q:
                print e['abbreviation']

        else:
            q = CustomQuery("Archive[@isLatest=1][Disease[@abbreviation=%s]][ArchiveType[@type=Level_3]]" % (options.name))
            out = {}
            for e in q:
                name = e['baseName']
                if name not in out:
                    print name
                    out[name] = True


    if options.list_type == "filelist":
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=Level_%s]]" % (options.filelist, options.level))
        for e in q:
            print e['deployLocation']
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (options.filelist))
        for e in q:
            print e['deployLocation']    
    return 0


def main_download(options):

    ##################
    #other data importer 
    ##################

    if options.download_type == 'uuid':
        url="https://tcga-data.nci.nih.gov/uuid/uuidBrowserExport.htm"
        data = {}
        data['exportType'] = 'tab'
        data['cols'] = "uuid,barcode"
        if options.output is not None:
            urllib.urlretrieve( url, options.output, data=urllib.urlencode(data))
        else:
            print urllib.urlopen(url, data=urllib.urlencode(data)).read()

    if options.download_type == 'samples':
        url="https://tcga-data.nci.nih.gov/datareports/aliquotExport.htm"
        data = {}
    
        data['exportType'] = 'tab'
        data['cols'] = 'aliquotId,disease,bcrBatch,center,platform,levelOne,levelTwo,levelThree'
        data['filterReq'] = json.dumps({"disease":"","levelOne":"","aliquotId":"","center":"","levelTwo":"","bcrBatch":"","platform":"","levelThree":""})
        data['formFilter'] = json.dumps({"disease":"","levelOne":"","aliquotId":"","center":"","levelTwo":"","bcrBatch":"","platform":"","levelThree":""})
        handle = urllib.urlopen( url + "?" + urllib.urlencode(data))
    
        for line in handle:
            tmp = line.rstrip().split("\t")
            if tmp[7] == "Submitted":
                if tmp[0][13]=='0':
                    print "\t".join( [ tmp[0], tmp[1], "Tumor", tmp[4] ] )
                elif tmp[0][13] == '1':
                    print "\t".join( [ tmp[0], tmp[1], "Normal", tmp[4] ] )

    if options.download_type == 'barcode_dag':
        url="https://tcga-data.nci.nih.gov/datareports/aliquotExport.htm"
        data = {}        
        data['exportType'] = 'tab'
        data['cols'] = 'aliquotId'
        data['filterReq'] = json.dumps({"disease":"","levelOne":"","aliquotId":"","center":"","levelTwo":"","bcrBatch":"","platform":"","levelThree":""})
        data['formFilter'] = json.dumps({"disease":options.barcode_dag,"levelOne":"","aliquotId":"","center":"","levelTwo":"","bcrBatch":"","platform":"","levelThree":""})
        handle = urllib.urlopen( url + "?" + urllib.urlencode(data))

        for line in handle:
            if line.startswith("TCGA"):
                tmp = line.rstrip().split('-')
                
                print "%s\t%s" % ("-".join( tmp[0:3] ), "-".join( tmp[0:4] ))
                print "%s\t%s" % ("-".join( tmp[0:4] ), "-".join( tmp[0:4] + [tmp[4][0:2]] ))
                
                print "%s\t%s" % ("-".join( tmp[0:4] + [tmp[4][0:2]] ), "-".join( tmp[0:5] ))
                print "%s\t%s" % ("-".join( tmp[0:5] ), "-".join( tmp ))
    return 0


def get_basename_platform(basename):
    q = CustomQuery("Archive[@isLatest=1][baseName=%s]" % (basename))
    platform_url = None
    for e in q:
        platform_url = e['platform']
    q = CustomQuery(platform_url)
    for e in q:
        basename_platform_alias = e['alias']
    return basename_platform_alias


def main_build(options):

    #if archive name is provided, determine the platform
    basename_platform_alias = None
    if options.basename:
        basename_platform_alias = get_basename_platform(options.basename)
 

    ###################
    # Archive Importers
    ###################

    if basename_platform_alias is not None:
        if options.checksum or options.checksum_delete:
            urls = []
            q = tcgaConfig[basename_platform_alias].getArchiveUrls(options.basename)
            for e in q:
                urls.append( e )
            mage_url = tcgaConfig[basename_platform_alias].getMageUrl(options.basename)
            if mage_url:
                urls.append(mage_url)
            
            for url in urls:
                dst = os.path.join(options.mirror, re.sub("^/", "", url))
                if not os.path.exists( dst ):
                    print "NOT_FOUND:", dst
                    continue
                if not os.path.exists( dst + ".md5" ):
                    print "MD5_NOT_FOUND", dst
                    continue

                handle = open( dst + ".md5" )
                line = handle.readline()
                omd5 = line.split(' ')[0]
                handle.close()

                nmd5 = fileDigest( dst )
                if omd5 != nmd5:
                    print "CORRUPT:", dst
                    if options.checksum_delete:
                        os.unlink(dst)
                        os.unlink(dst + ".md5")
                else:
                    print "OK:", dst        

        if options.mirror is None:
            sys.stderr.write("Need mirror location\n")
            return 1
        
        conf = getBaseBuildConf(options.basename, basename_platform_alias, options.mirror)
        conf.addOptions(options)
        if conf.platform not in tcgaConfig:
            sys.stderr.write("Platform %s not supported\n" % (conf.platform))
            return 1

        if options.report:
            print json.dumps(conf.buildRequest(), indent=4)
            return 0

        ext = tcgaConfig[conf.platform](conf, conf.buildRequest())
        ext.run()
    return 0




if __name__ == "__main__":
    
    parser = ArgumentParser()
    #Stack.addJobTreeOptions(parser) 

    subparsers = parser.add_subparsers(title="subcommand")


    parser_list = subparsers.add_parser('list')
    parser_list.add_argument("list_type", choices=[
        "platforms",
        "archives",
        "mutation", 
        "platform",
        "supported",
        "files",
        "cancer",
        "clinical"
    ])
    parser_list.add_argument("name", nargs="?", default=None )

    """
    parser_list.add_argument("platforms", dest="all_platform", action="store_true", help="Get list of supported platforms", default=False)
    parser_list.add_argument("archives", dest="all_archives", action="store_true", help="List all archives", default=False)
    parser_list.add_argument("mutations", dest="all_mutation", action="store_true", default=False)
    parser_list.add_argument("platform", dest="platform", help="Platform Selection", default=None)
    parser_list.add_argument("supported", dest="supported_list", action="store_true", help="List Supported Platforms", default=None)
    parser_list.add_argument("files", dest="filelist", help="List files needed to convert TCGA project basename into cgData", default=None)
    parser_list.add_argument("cancer", dest="cancer", help="List Archives by cancer type", default=None)
    parser_list.add_argument("outputs", dest="list_platform_outputs", default=None)
    parser_list.add_argument("clinical", dest="all_clinical", action="store_true", help="List all clinical archives", default=False)
    """
    parser_list.set_defaults(func=main_list)


    #other importer options

    parser_download = subparsers.add_parser('download')
    parser_download.add_argument("download_type", choices=[
        "uuid",
        "samples",
        "barcode-dag"
    ])
    parser_download.add_argument("-o", "--output", help="Output Path", default=None)


    """
    parser_download.add_argument("uuid", dest="uuid_download", help="Download UUID/Barcode Table", default=False)
    parser_download.add_argument("samples", dest="get_samples", action="store_true", default=False)
    parser_download.add_argument("barcode-dag", dest="barcode_dag", help="Write TCGA Barcode DAG for cancer type", default=None)
    """
    parser_download.set_defaults(func=main_download)

    #archive importers
    parser_build = subparsers.add_parser('build')

    parser_build.add_argument("basename", help="Convert TCGA project basename into cgData", default=None)
    parser_build.add_argument("--clinical-type", dest="clinical_type", help="Clinical Data Type", default=None)
    parser_build.add_argument("--out-clinical", dest="out_clinical", action="append", nargs=3, default=[])

    #import options    
    parser_build.add_argument("-u", "--uuid", dest="uuid_table", help="UUID to Barcode Table", default=None)
    parser_build.add_argument("-m", "--mirror", dest="mirror", help="Mirror Location", default=None)
    parser_build.add_argument("-w", "--workdir", dest="workdir_base", help="Working directory", default="/tmp")
    parser_build.add_argument("-d", "--download", dest="download", help="Download files for archive", action="store_true", default=False)
    parser_build.add_argument("--download-only", dest="download_only", help="Download files for archive then quit", action="store_true", default=False)
    parser_build.add_argument("-e", "--level", dest="level", help="Data Level ", default="3")
    parser_build.add_argument("--checksum", dest="checksum", help="Check project md5", action="store_true", default=False)
    parser_build.add_argument("--checksum-delete", dest="checksum_delete", help="Check project md5 and delete bad files", action="store_true", default=False)
    parser_build.add_argument("-r", "--sanitize", dest="sanitize", action="store_true", help="Remove race/ethnicity from clinical data", default=False) 

    #output
    parser_build.add_argument("--report", dest="report", help="Print Build Report", action="store_true", default=False)
    parser_build.add_argument("--outdir", dest="outdir", help="Working directory", default="./")
    parser_build.add_argument("-o", "--out", dest="outpath", help="Output Dest", default=None)    
    parser_build.add_argument("--out-error", dest="errorpath", help="Output Error", default=None)
    parser_build.add_argument("--out-meta", dest="metapath", help="Output Meta", default=None)
    parser_build.set_defaults(func=main_build)


    args = parser.parse_args()
    sys.exit(args.func(args))
