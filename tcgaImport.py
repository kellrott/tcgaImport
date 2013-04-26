#!/usr/bin/env python


"""
Script to scan and extract TCGA data and compile it into the cgData

Usage::
    
    tcga2cgdata.py [options]

Options::
    
      -h, --help            show this help message and exit
      -a, --platform-list   Get list of platforms
      -p PLATFORM, --platform=PLATFORM
                            Platform Selection
      -l, --supported       List Supported Platforms
      -f FILELIST, --filelist=FILELIST
                            List files needed to convert TCGA project basename
                            into cgData
      -b BASENAME, --basename=BASENAME
                            Convert TCGA project basename into cgData
      -m MIRROR, --mirror=MIRROR
                            Mirror Location
      -w WORKDIR_BASE, --workdir=WORKDIR_BASE
                            Working directory
      -o OUTDIR, --out-dir=OUTDIR
                            Working directory
      -c CANCER, --cancer=CANCER
                            List Archives by cancer type
      -d DOWNLOAD, --download=DOWNLOAD
                            Download files for archive
      -e LEVEL, --level=LEVEL
                            Data Level
      -s CHECKSUM, --check-sum=CHECKSUM
                            Check project md5
      -r, --sanitize        Remove race/ethnicity from clinical data


Example::
    
    ./scripts/tcga2cgdata.py -b intgen.org_KIRC_bio -m /inside/depot -e 1 -r -w tmp


"""

from xml.dom.minidom import parseString
import urllib
import urllib2
import os
import csv
import sys
import hashlib
import tempfile
import re
import copy
import json
import datetime
import hashlib
import subprocess
from glob import glob
import shutil
import subprocess
from argparse import ArgumentParser




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
            handle = urllib.urlopen(next)
            data = handle.read()
            handle.close()
            dom = parseString(data)
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
        if 'diseaseAbbr' in meta:
            self.abbr = meta['diseaseAbbr']
    
    def addOptions(self, opts):
        self.workdir_base = opts.workdir_base
        self.outdir = opts.outdir
        self.sanitize = opts.sanitize
        self.outpath = opts.outpath
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
                self.uuid_table[tmp[0]] = tmp[1]
    
    def translateUUID(self, uuid):
        if self.uuid_table is None or uuid not in self.uuid_table:
            return uuid
        return self.uuid_table[uuid]
    
    def getOutPath(self, name):
        if self.outpath is not None:
            return self.outpath
        if name in self.clinical_type_map:
            return self.clinical_type_map[name][0]
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)
        return os.path.join(self.outdir, self.name) + name

    def getOutMeta(self, name):
        if self.outpath is not None:
            if self.metapath is not None:
                return self.metapath
            return self.outpath + ".json"
        if name in self.clinical_type_map:
            return self.clinical_type_map[name][1]
        return os.path.join(self.outdir, self.name) + name + ".json"

    def getOutError(self, name):
        if self.outpath is not None:
            if self.errorpath is not None:
                return self.errorpath
            return self.outpath + ".error"
        return os.path.join(self.outdir, self.name) + name + ".error"


def getBaseBuildConf(basename, platform, mirror):
    dates = []
    print "TCGA Query for: ", basename
    q = tcgaConfig[platform].getArchiveQuery(basename)
    urls = {}
    meta = None
    platform = None
    for e in q:
        dates.append( datetime.datetime.strptime( e['addedDate'], "%m-%d-%Y" ) )
        if meta is None:
            meta = {"sourceUrl" : []}            
            for e2 in CustomQuery(e['platform']):
                platform = e2['name']
                meta['platform'] = e2['name']
                meta['platformTitle'] = e2['displayName']
            for e2 in CustomQuery(e['disease']):
                meta['diseaseAbbr'] = e2['abbreviation']
                meta['diseaseTitle'] = e2['name']
                for e3 in CustomQuery(e2['tissueCollection']):
                    meta['tissue'] = e3['name']
            for e2 in CustomQuery(e['center']):
                meta['centerTitle'] = e2['displayName']
                meta['center'] = e2['name']
        meta['sourceUrl'].append( "http://tcga-data.nci.nih.gov/" + e['deployLocation'] )
        urls[ mirror + e['deployLocation'] ] = platform

    print "TCGA Query for mage-tab: ", basename
    q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (basename))
    for e in q:
        dates.append( datetime.datetime.strptime( e['addedDate'], "%m-%d-%Y" ) )
        q2 = CustomQuery(e['platform'])
        platform = None
        for e2 in q2:
            print e2
            platform = e2['name']
        meta['sourceUrl'].append( "http://tcga-data.nci.nih.gov/" + e['deployLocation'] )
        urls[ mirror + e['deployLocation'] ] = platform
    
    if len(dates) == 0:
        print "No Files found"
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
    
    def __init__(self, config):
        self.config = config
        
    def extractTars(self):  
        if not os.path.exists(self.config.workdir_base):
            os.makedirs(self.config.workdir_base)      
        self.work_dir = tempfile.mkdtemp(dir=self.config.workdir_base)
        print "Extract to ", self.work_dir
        for path in self.config.tarlist:
            subprocess.check_call([ "tar", "xzf", path, "-C", self.work_dir])#, stderr=sys.stdout)
        
    def run(self):        
        self.extractTars()
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
        #shutil.rmtree(self.work_dir)       
    
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

    def emitFile(self, name, dataSubType, meta, file):
        md5 = hashlib.md5()
        oHandle = open(self.config.getOutPath(name), "wb")
        with open(file,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), ''): 
                md5.update(chunk)
                oHandle.write(chunk)
        oHandle.close()
        md5str = md5.hexdigest()
        meta['md5'] = md5str
        mHandle = open(self.config.getOutMeta(name), "w")
        mHandle.write( json.dumps(meta))
        mHandle.close()
        if len(self.errors):
            eHandle = open( self.config.getOutError(name), "w" )
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
            self.ext_meta['description'] = handle.read()
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
            '@context' : "http://purl.org/cgdata/",
            '@type' : 'bed5', 
            '@id' : name, 
            "lastModified" : self.config.version,
            'rowKeySrc' : {
                    '@type' :  'idDAG',
                    '@id' : "tcga.%s" % (self.config.abbr)
            },
            'dataSubType' : { "@id" : dataSubType },
            'dataProducer' : 'TCGA Import',
            "accessMap" : "public", "redistribution" : "yes" 
        }
        matrixInfo.update(self.ext_meta)
        matrixInfo.update(self.config.meta)
        return matrixInfo
    
    def fileBuild(self, dataSubType):
        #use the target table to create a name translation table
        #also setup target name enumeration, so they will have columns
        #numbers

        tTrans = self.getTargetMap()        
        subprocess.call("sort -k 1 %s/segments > %s/segments.sort" % (self.work_dir, self.work_dir), shell=True)
        sHandle = TableReader(self.work_dir + "/segments.sort")

        segFile = None
        curName = None
        
        curData = {}
        missingCount = 0

        startField  = "loc.start"
        endField    = "loc.end"
        valField    = "seg.mean"
        chromeField = "chrom"
        
        segFile = None

        for key, value in sHandle:
            if segFile is None:
                segFile = open("%s/%s.segment_file"  % (self.work_dir, dataSubType), "w")
            try:
                curName = self.translateUUID(tTrans[key]) # "-".join( tTrans[ key ].split('-')[0:4] )
                if curName is not None:
                    try:
                        chrom = value[ chromeField ].lower()
                        if not chrom.startswith("chr"):
                            chrom = "chr" + chrom
                        chrom = chrom.upper().replace("CHR", "chr")
                        #segFile.write( "%s\t%s\t%s\t%s\t.\t%s\n" % ( curName, chrom, int(value[ startField ])+1, value[ endField ], value[ valField ] ) )
                        segFile.write( "%s\t%s\t%s\t%s\t%s\n" % ( chrom, int(value[ startField ])-1, value[ endField ], curName, value[ valField ] ) )
                    except KeyError:
                         self.addError( "Field error: %s" % (str(value)))
            except KeyError:
                self.addError( "TargetInfo Not Found: %s" % (key))
            
        segFile.close()
        matrixName = self.config.name

        self.emitFile( "", self.getMeta(matrixName, 'cna'), "%s/%s.segment_file"  % (self.work_dir, dataSubType) )     



class TCGAMatrixImport(TCGAGeneticImport):
    
    def getMeta(self, name, dataSubType):
        matrixInfo = { 
            "@context" : 'http://purl.org/cgdata/',
            '@type' : 'genomicMatrix', 
            '@id' : name, 
            "lastModified" : self.config.version,
            'dataSubType' : { "@id" : dataSubType },
            'dataProducer' : 'TCGA', 
            "accessMap" : "public", 
            "redistribution" : "yes",
            'rowKeySrc' : {
                "@type" : "probe", "@id" : self.dataSubTypes[dataSubType]['probeMap']
            },
            'columnKeySrc' : {
                "@type" : "idDAG", "@id" :  "tcga.%s" % (self.config.abbr)
            }
        }
        matrixInfo.update(self.ext_meta)
        matrixInfo.update(self.config.meta)
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
            self.emitFile( "." + dataSubType, self.dataSubTypes[dataSubType], self.getMeta(matrixName, dataSubType), "%s/%s.matrix_file"  % (self.work_dir, dataSubType) )


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
        print "Parsing", path
        handle = open(path)
        data = handle.read()
        handle.close()
        xml=parseString(data)
        self.parseXMLFile(xml)
            
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

    def parseXMLFile(self, dom):    
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
                patient_data[attr.get('preferred_name', stack[-1])] = { "value" : text }
        self.emit( patient_barcode, patient_data, "patient" )
        
        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample"):
            sample_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/bcr_sample_barcode"):
                sample_barcode = c_text
            sample_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/*"):
                if 'xsd_ver' in c_attr:
                    sample_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( sample_barcode, sample_data, "sample" )

        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion"):
            portion_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/bcr_portion_barcode"):
                portion_barcode = c_text
            portion_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/*"):
                if 'xsd_ver' in c_attr:
                    portion_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( portion_barcode, portion_data, "portion" )
        
        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte"):
            analyte_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/bcr_analyte_barcode"):
                analyte_barcode = c_text
            analyte_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/*"):
                if 'xsd_ver' in c_attr:
                    analyte_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( analyte_barcode, analyte_data, "analyte" )


        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte/aliquots/aliquot"):
            aliquot_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/bcr_aliquot_barcode"):
                aliquot_barcode = c_text
            aliquot_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/*"):
                if 'xsd_ver' in c_attr:
                    aliquot_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( aliquot_barcode, aliquot_data, "aliquot" )
        
        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/drugs/drug"):
            drug_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/bcr_drug_barcode"):
                drug_barcode = c_text
            drug_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/*"):
                if 'xsd_ver' in c_attr:
                    drug_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( drug_barcode, drug_data, "drug" )

        for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/radiations/radiation"):
            radiation_barcode = None
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/bcr_radiation_barcode"):
                radiation_barcode = c_text
            radiation_data = {}    
            for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/*"):
                if 'xsd_ver' in c_attr:
                    radiation_data[c_attr.get('preferred_name', c_stack[-1])] = { "value" : c_text }
            self.emit( radiation_barcode, radiation_data, "radiation" )


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

            

    def getMeta(self, name):
        fileInfo = {
            "@context" : "http://purl.org/cgdata/",
            "@type" : "clinicalMatrix",
            "@id" : name,
            "lastModified" :  self.config.version,
            'dataSubType' : { "@id" : "clinical" },
            "rowKeySrc" : {
                "@type" : "idDAG", "@id" :  "tcga.%s" % (self.config.abbr)
            }
            
        }
        fileInfo.update(self.ext_meta)
        fileInfo.update(self.config.meta)
        return fileInfo
    
    def fileBuild(self, dataSubType):

        matrixList = [ "patient", "sample", "radiation", "drug", "portion", "analyte", "aliquot", "followup" ]
        if self.config.clinical_type is not None:
            matrixList = [ self.config.clinical_type ]

        for matrixName in matrixList:
            if os.path.exists( "%s/%s" % (self.work_dir, matrixName)):
                subprocess.call("cat %s/%s | sort -k 1 > %s/%s.sort" % (self.work_dir, matrixName, self.work_dir, matrixName), shell=True)
                handle = TableReader(self.work_dir + "/" + matrixName + ".sort")
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
                
                handle = open( os.path.join(self.work_dir, matrixName + "_file"), "w")
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
                self.emitFile( "." + matrixName, matrixName, self.getMeta(self.config.name + "." + matrixName), "%s/%s_file"  % (self.work_dir, matrixName)) 
        
        
        
class AgilentImport(TCGAMatrixImport):
    dataSubTypes = { 
        'geneExp' : { 
            'probeMap' : 'hugo',
            'sampleMap' : 'tcga.iddag',
            'dataType'  : 'genomicMatrix',
            'probeFields' : ['log2 lowess normalized (cy5/cy3) collapsed by gene symbol']
        }
    }
   

class CGH1x1mImport(TCGASegmentImport):
    dataSubTypes = { 
        'cna' : {
            "sampleMap" : 'tcga.iddag',
            "dataType" : 'genomicSegment',
            "probeFields" : ['seg.mean']
        }
    }

class SNP6Import(TCGASegmentImport):
    assembly = 'hg19'
    dataSubTypes = { 
        'cna' : { 
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['seg.mean'],
            'fileInclude' : r'^.*\.hg19.seg.txt$'
        },
        'cna_nocnv' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['seg.mean'],
            'fileInclude' : r'^.*\.nocnv_hg19.seg.txt$'
        },
        'cna_probecount' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Num_Probes'],
            'fileInclude' : r'^.*\.hg19.seg.txt$'
        },
        'cna_nocnv_probecount' : {
            'sampleMap' :'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Num_Probes'],
            'fileInclude' : r'^.*\.nocnv_hg19.seg.txt$'
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

        startField  = "loc.start"
        endField    = "loc.end"
        valField    = self.dataSubTypes[dataSubType]['probeFields'][0]
        chromeField = "chrom"
            
        segFile = None
        sHandle = handle
        for key, value in sHandle:
            if segFile is None:
                segFile = open("%s/%s.out"  % (self.work_dir, dataSubType), "w")
            try:
                curName = self.translateUUID(tmap[key])
                if curName is not None:
                    chrom = value[ chromeField ].lower()
                    if not chrom.startswith("chr"):
                        chrom = "chr" + chrom
                    chrom = chrom.upper().replace("CHR", "chr")
                    segFile.write( "%s\t%s\t%s\t%s\t%s\n" % ( chrom, value[ startField ], value[ endField ], curName, value[ valField ] ) )
            except KeyError:
                self.addError( "TargetInfo Not Found: %s" % (key))
            
        segFile.close()
        meta = self.getMeta(self.config.name + ".hg19." + dataSubType, dataSubType)
        meta['assembly'] = { "@id" : 'hg19' }
        self.emitFile(".hg19." + dataSubType, dataSubType, meta, "%s/%s.out"  % (self.work_dir, dataSubType))
       

class HmiRNAImport(TCGAMatrixImport):
    dataSubTypes = { 
        'miRNAExp' : {
            'probeMap' : 'agilentHumanMiRNA',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'probeFields' : ['unc_DWD_Batch_adjusted']
        }
    }
    
class CGH244AImport(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['Segment_Mean']
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
            'startField' : 'Start'
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
            'startField' : 'Start'
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
            'probeFields' : ['Signal']
        }
    }

class HuEx1_0stv2(TCGAMatrixImport):
    dataSubTypes = {
        'miRNAExp' : {
            'probeMap' : 'hugo',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'probeFields' : ['Signal'],
            'fileInclude' : '^.*gene.txt$|^.*sdrf.txt$'
        }
    }

class Human1MDuoImport(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['mean']
        }
    }

class HumanHap550(TCGASegmentImport):
    dataSubTypes = {
        'cna' : {
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicSegment',
            'probeFields' : ['mean']
        }
    }

class HumanMethylation27(TCGAMatrixImport):
    dataSubTypes = {
        'DNAMethylation' : {
            'probeMap' : 'illuminaMethyl27K_gpl8490',
            'sampleMap' :  'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'fileExclude' : '.*.adf.txt',
            'probeFields' : ['Beta_Value', 'Beta_value']
        }
    }
    

class HumanMethylation450(TCGAMatrixImport):
    dataSubTypes =  {
        'DNAMethylation' : {
            'probeMap' :  'illuminaHumanMethylation450',
            'sampleMap' : 'tcga.iddag',
            'dataType' : 'genomicMatrix',
            'fileExclude' : '.*.adf.txt',
            'probeFields' :  ['Beta_value', 'Beta_Value']
        }
    }

    def fileScan(self, path):
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
                            if colType[i] in self.probeFields:
                                out[ colName[i] ][ colType[i] ] = "%.4f" % float(tmp[i])
                        except IndexError:
                            out[ colName[i] ][ colType[i] ] = "NA"
                        except ValueError:
                            out[ colName[i] ][ colType[i] ] = "NA"
                    for col in out:
                        self.emit( tmp[0], out[col], "probes" )
                
class Illumina_RNASeq(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*\.gene.quantification.txt$|^.*sdrf.txt$',
            'probeFields' : ['RPKM'],
            'probeMap' : 'hugo.unc'
        }
    }

class Illumina_RNASeqV2(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*rsem.genes.normalized_results$|^.*sdrf.txt$',
            'probeFields' : ['normalized_count'],
            'probeMap' : 'hugo.unc'
        },
        'isoformExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*rsem.isoforms.results$',
            'probeFields' : ['raw_count'],
            'probeMap' : 'ucsc.id'
        }
    }

class IlluminaHiSeq_RNASeq(TCGAMatrixImport):
    dataSubTypes = {
        'geneExp' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : r'^.*gene.quantification.txt$',
            'probeFields' : ['RPKM'],
            'probeMap' : 'hugo.unc'
        }
    }

class MDA_RPPA_Core(TCGAMatrixImport):
    dataSubTypes = {
        "RPPA" : {
            'sampleMap' : 'tcga.iddag',
            'probeMap' : "md_anderson_antibodies",
            'fileExclude' : r'^.*.antibody_annotation.txt|^.*array_design.txt$',
            'probeFields' : [ 'Protein Expression', 'Protein.Expression' ]
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
        'miRNA' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '^.*.mirna.quantification.txt$',
            'probeFields' : ['reads_per_million_miRNA_mapped'],
            'probeMap' : 'hsa.mirna'
        }
    }

class bioImport(TCGAClinicalImport):
    dataSubTypes = {
        'bio' : {
            'sampleMap' : 'tcga.iddag',
            'fileInclude' : '.*.xml$'
        }
    }

class MafImport(FileImporter):
    dataSubTypes = {
        'mutation' : {
            'fileInclude' : '.*.maf$'
        }
    }

    def getMeta(self, name):
        fileInfo = {
            "@context" : "http://purl.org/cgdata/",
            "@type" : "maf",
            "@id" : name,
            "lastModified" :  self.config.version,
        }
        fileInfo.update(self.ext_meta)
        fileInfo.update(self.config.meta)
        return fileInfo
    
    def fileScan(self, path):
        name = os.path.basename(path)
        self.emitFile(name, self.getMeta(name), path)

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
            self.ext_meta['description'] = handle.read()
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
    'HuEx-1_0-st-v2': HuEx1_0stv2,
    'Human1MDuo': Human1MDuoImport,
    'HumanHap550': HumanHap550,
    'IlluminaHiSeq_DNASeqC' : IlluminaHiSeq_DNASeqC,
    'HumanMethylation27': HumanMethylation27,
    'HumanMethylation450': HumanMethylation450,
    'IlluminaHiSeq_RNASeq': IlluminaHiSeq_RNASeq,
    'IlluminaGA_RNASeq' : Illumina_RNASeq,
    'IlluminaHiSeq_RNASeqV2' : Illumina_RNASeqV2,
    'MDA_RPPA_Core' : MDA_RPPA_Core,
    'IlluminaGA_miRNASeq' : Illumina_miRNASeq,
    'IlluminaHiSeq_miRNASeq' : Illumina_miRNASeq,
    'bio' : bioImport,
    'IlluminaGA_DNASeq' : MafImport,
    'SOLiD_DNASeq' : MafImport,
    'ABI' : MafImport
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


if __name__ == "__main__":
    
    parser = ArgumentParser()
    #Stack.addJobTreeOptions(parser) 

    #other importer options
    parser.add_argument("-t", "--uuid-download", dest="uuid_download", help="Download UUID/Barcode Table", default=False)
    parser.add_argument("--samples", dest="get_samples", action="store_true", default=False)
    parser.add_argument("--barcode-dag", dest="barcode_dag", help="Write TCGA Barcode DAG for cancer type", default=None)

    #list operations
    parser.add_argument("-a", "--all-platform", dest="all_platform", action="store_true", help="Get list of supported platforms", default=False)
    parser.add_argument("-z", "--all-archives", dest="all_archives", action="store_true", help="List all archives", default=False)
    parser.add_argument("--all-mutation", dest="all_mutation", action="store_true", default=False)
    parser.add_argument("-p", "--platform", dest="platform", help="Platform Selection", default=None)
    parser.add_argument("-l", "--supported", dest="supported_list", action="store_true", help="List Supported Platforms", default=None)
    parser.add_argument("-f", "--filelist", dest="filelist", help="List files needed to convert TCGA project basename into cgData", default=None)
    parser.add_argument("-c", "--cancer", dest="cancer", help="List Archives by cancer type", default=None)
    parser.add_argument("--list-platform-outputs", dest="list_platform_outputs", default=None)

    #archive importers
    parser.add_argument("-b", "--basename", dest="basename", help="Convert TCGA project basename into cgData", default=None)
    parser.add_argument("--clinical-type", dest="clinical_type", help="Clinical Data Type", default=None)
    parser.add_argument("--all-clinical", dest="all_clinical", action="store_true", help="List all clinical archives", default=False)
    parser.add_argument("--out-clinical", dest="out_clinical", action="append", nargs=3, default=[])

    #import options    
    parser.add_argument("-u", "--uuid", dest="uuid_table", help="UUID to Barcode Table", default=None)
    parser.add_argument("-m", "--mirror", dest="mirror", help="Mirror Location", default=None)
    parser.add_argument("-w", "--workdir", dest="workdir_base", help="Working directory", default="/tmp")
    parser.add_argument("-d", "--download", dest="download", help="Download files for archive", action="store_true", default=False)
    parser.add_argument("-e", "--level", dest="level", help="Data Level ", default="3")
    parser.add_argument("--checksum", dest="checksum", help="Check project md5", action="store_true", default=False)
    parser.add_argument("--checksum-delete", dest="checksum_delete", help="Check project md5 and delete bad files", action="store_true", default=False)
    parser.add_argument("-r", "--sanitize", dest="sanitize", action="store_true", help="Remove race/ethnicity from clinical data", default=False) 

    #output
    parser.add_argument("--outdir", dest="outdir", help="Working directory", default="./")
    parser.add_argument("-o", "--out", dest="outpath", help="Output Dest", default=None)    
    parser.add_argument("--out-error", dest="errorpath", help="Output Error", default=None)
    parser.add_argument("--out-meta", dest="metapath", help="Output Meta", default=None)
    

    options = parser.parse_args()

    #if archive name is provided, determine the platform
    basename_platform_alias = None
    if options.basename:
        q = CustomQuery("Archive[@isLatest=1][baseName=%s]" % (options.basename))
        platform_url = None
        for e in q:
            platform_url = e['platform']
        q = CustomQuery(platform_url)
        for e in q:
            basename_platform_alias = e['alias']
        print basename_platform_alias

    ##################
    #other data importer 
    ##################
    if options.uuid_download:
        url="https://tcga-data.nci.nih.gov/uuid/uuidBrowserExport.htm"
        data = {}
        data['exportType'] = 'tab'
        data['cols'] = "uuid,barcode"
        urllib.urlretrieve( url, options.uuid_download, data=urllib.urlencode(data))


    if options.get_samples:
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

    if options.barcode_dag is not None:
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

        
    #################
    #list operations
    #################
    if options.all_platform:
        for e in platform_list():
            print e
    
    if options.supported_list:
        for e in supported_list():
            print e
            
    if options.platform:
        for name in tcgaConfig[options.platform].getArchiveList(options.platform):
            print name

    if options.all_archives:
        q = CustomQuery("Archive[@isLatest=1][ArchiveType[@type=Level_%s]]" % (options.level))
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                print name
                out[name] = True

    if options.all_clinical:
        q = CustomQuery("Archive[@isLatest=1][Platform[@alias=bio]]")
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                print name
                out[name] = True

    if options.all_mutation:
        q = CustomQuery("Archive[@isLatest=1][Platform[@alias=IlluminaGA_DNASeq]]")
        out = {}
        for e in q:
            if e['deployLocation'].count("anonymous"):
                name = e['baseName']
                if name not in out:
                    print name
                    out[name] = True
        q = CustomQuery("Archive[@isLatest=1][Platform[@alias=SOLiD_DNASeq]]")
        out = {}
        for e in q:
            if e['deployLocation'].count("anonymous"):
                name = e['baseName']
                if name not in out:
                    print name
                    out[name] = True

    if options.list_platform_outputs:
        for p in tcgaConfig[options.list_platform_outputs].getOutputList():
            print p



    if options.cancer is not None:
        q = CustomQuery("Archive[@isLatest=1][Disease[@abbreviation=%s]][ArchiveType[@type=Level_%s]]" % (options.cancer, options.level))
        out = {}
        for e in q:
            name = e['baseName']
            if name not in out:
                print name
                out[name] = True

    if options.filelist:
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=Level_%s]]" % (options.filelist, options.level))
        for e in q:
            print e['deployLocation']
        q = CustomQuery("Archive[@baseName=%s][@isLatest=1][ArchiveType[@type=mage-tab]]" % (options.filelist))
        for e in q:
            print e['deployLocation']

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


        if options.download:
            if options.mirror is None:
                print "Define mirror location"
                sys.exit(1)

            urls = []
            
            for e in tcgaConfig[basename_platform_alias].getArchiveUrls(options.basename):
                urls.append( e )
                urls.append( e + ".md5" )          

            e = tcgaConfig[basename_platform_alias].getMageUrl(options.basename)
            if e:
                urls.append( e )
                urls.append( e + ".md5" )          
      
            for url in urls:
                src = "https://tcga-data.nci.nih.gov/" + url
                dst = os.path.join(options.mirror, re.sub("^/", "", url))
                dir = os.path.dirname(dst)
                if not os.path.exists(dir):
                    print "mkdir", dir
                    os.makedirs(dir)
                if not os.path.exists( dst ):
                    print "download %s to %s" % (src, dst)
                    urllib.urlretrieve(src, dst)

        if options.mirror is None:
            sys.stderr.write("Need mirror location\n")
            sys.exit(1)
        
        conf = getBaseBuildConf(options.basename, basename_platform_alias, options.mirror)
        conf.addOptions(options)
        if conf.platform not in tcgaConfig:
            sys.stderr.write("Platform %s not supported\n" % (conf.platform))
            sys.exit(1)

        ext = tcgaConfig[conf.platform](conf)
        ext.run()


