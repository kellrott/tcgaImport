#!/usr/bin/env python

from argparse import ArgumentParser
import sys
from xml.dom.minidom import parseString
from urlparse import urlparse

from rdflib import Namespace, BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF

import json
import time
import random
import urllib
import os
import re
import hashlib
import tempfile
import subprocess
from glob import glob
import shutil

TCGA_NS = Namespace("http://purl.org/bmeg/tcga/")
TCGA_OWL = Namespace("http://purl.org/bmeg/tcga.owl#")
BMEG_NS = Namespace("http://purl.org/bmeg/owl#")

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



def fileDigest( file ):
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), ''): 
            md5.update(chunk)
    return md5.hexdigest()


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



def fileDigest( file ):
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), ''): 
            md5.update(chunk)
    return md5.hexdigest()

def main_build(options):

    urls = []    
    centerName = None
    acronym = None
    q = CustomQuery("Archive[@baseName=%s][@isLatest=1]" % (options.basename))
    for e in q:
        if acronym is None:
            for a in CustomQuery(e['disease']):
                acronym = a['abbreviation']
        if centerName is None:
            for a in CustomQuery(e['center']):
                centerName = a['name']
        urls.append( "https://tcga-data.nci.nih.gov" + e['deployLocation'])
    
    if options.mirror is None:
        sys.stderr.write("Need mirror location\n")
        return 1

    try_count = 0
    while try_count < 3:
        tar_path_list = []
        ok = True
        for url in urls:
            path = urlparse(url).path
            dst = os.path.join(options.mirror, re.sub("^/", "", path))
            dir = os.path.dirname(dst)
            if not os.path.exists(dir):
                print "mkdir", dir
                os.makedirs(dir)
            if not os.path.exists( dst ):
                print "download %s to %s" % (url, dst)
                urllib.urlretrieve(url, dst)
                urllib.urlretrieve(url + ".md5", dst + ".md5")


            if not os.path.exists( dst ):
                print "NOT_FOUND:", dst
                ok = False
            if not os.path.exists( dst + ".md5" ):
                print "MD5_NOT_FOUND", dst
                of = False

            handle = open( dst + ".md5" )
            line = handle.readline()
            omd5 = line.split(' ')[0]
            handle.close()

            nmd5 = fileDigest( dst )
            if omd5 != nmd5:
                ok = False
                print "CORRUPT:", dst
                os.unlink(dst)
                os.unlink(dst + ".md5")
            else:
                print "OK:", dst     

            tar_path_list.append( dst )   
        if ok:
            break
        try_count += 1
    if not ok:
        print "Download failed"
        return 1
    print tar_path_list

    if not os.path.exists(options.workdir_base):
        os.makedirs(options.workdir_base)      
    work_dir = tempfile.mkdtemp(dir=options.workdir_base)
    print "Extract to ", work_dir
        
    for tar_path in tar_path_list:
        subprocess.check_call([ "tar", "xzf", tar_path, "-C", work_dir])#, stderr=sys.stdout)

    clin = ClinicalParser()
    for path in scandirs(work_dir, re.compile(r'.xml$')):
        handle = open(path)
        data = handle.read()
        handle.close()
        xml=parseString(data)
        for dataSubType in ["patient", "aliquot", "analyte", "portion", "sample", "drugs", "radiation", "followup"]:
            clin.parseXMLFile(xml, dataSubType)

    if args.output is not None:
        output_path = args.output
    else:
        output_path = args.basename + ".ttl"

    ohandle = open(output_path, "w")
    ohandle.write(clin.gr.serialize(format="turtle"))
    ohandle.close()  

    meta_data = {
        "name" : args.basename + ".ttl",
        "provenance" : { "used" : [], "name" : "tcgaImportRDF" },
        "annotations" : {
            "fileType" : "ttl",
            "basename" : args.basename,
            "center" : centerName,
            "acronym" : acronym,
            "platform" : "bio"
        },
        "platform": "bio",
        "species": "Homo sapiens",
        "md5": fileDigest( output_path )
    }
    for u in urls:
        meta_data['provenance']['used'].append( 
            {
                "url" : u,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL"
            }
        )
    
    ohandle = open(output_path + ".json", "w")
    ohandle.write(json.dumps(meta_data))
    ohandle.close()

    shutil.rmtree(work_dir)       
      

pred_mapping = {
    'sample_type' : lambda x: TCGA_OWL[x.replace(" ", "_")],
    'gel_image_file' : lambda x: URIRef(x),
    'analyte_type' : lambda x: TCGA_OWL[re.sub(r'[ \(\)]', "_", x)],
    'tissue_source_site' : lambda x: TCGA_OWL[x.replace(" ", "_")],
    'sample' : lambda x: TCGA_NS[x],
    'drug' : lambda x: TCGA_NS[x],
    'radiation' : lambda x: TCGA_NS[x],
    'followup' : lambda x: TCGA_NS[x],
    'tumor_tissue_site' : lambda x: TCGA_OWL[x.replace(" ", "_")],
    'vital_status' : lambda x: TCGA_OWL[x],
    'gender' : lambda x: TCGA_OWL[x],
    'ajcc_tumor_pathologic_pt' : lambda x: TCGA_OWL[x],
    'ajcc_nodes_pathologic_pn' : lambda x: TCGA_OWL[ re.sub(r'[ \(\)]', "_",x) ],
    'ajcc_metastasis_pathologic_pm' : lambda x: TCGA_OWL[ re.sub(r'[ \(\)]', "_",x) ],
    'analysis' : lambda x : TCGA_NS[x],
    'disease_code' : lambda x: TCGA_OWL[x]
}

class ClinicalParser:

    def __init__(self):
        self.gr = Graph()
        self.gr.bind( "tcga", TCGA_NS )
        self.gr.bind( "tcga_clin", TCGA_OWL )

    def parseXMLFile(self, dom, dataSubType):    
        root_node = dom.childNodes[0]
        """
        admin = {}
        for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/admin/*"):
            admin[stack[-1]] = { 'value' : text }
        """

        patient_barcode = None
        for node, stack, attr, text in dom_scan(root_node, 'tcga_bcr/patient/bcr_patient_barcode'):
            patient_barcode = text

        if dataSubType == "patient":
            self.emit( patient_barcode, 'type', "Patient")
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/*"):
                if 'xsd_ver' in attr:
                    #print patientName, stack[-1], attr, text
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(p_name) == 0:
                        p_name = stack[-1]
                    if len(text):
                        self.emit( patient_barcode, p_name, text )
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/stage_event/*/*"):
                if 'xsd_ver' in attr:
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(text):
                        self.emit(patient_barcode, p_name, text)
            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/patient/stage_event/tnm_categories/*/*"):
                if 'xsd_ver' in attr:
                    p_name = attr.get('preferred_name', stack[-1])
                    if len(text):
                        self.emit(patient_barcode, p_name, text)

            for node, stack, attr, text in dom_scan(root_node, "tcga_bcr/admin/disease_code"):
                self.emit(patient_barcode, 'disease_code', text)
                    
        if dataSubType == "sample":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample"):
                sample_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/bcr_sample_barcode"):
                    sample_barcode = c_text
                self.emit( patient_barcode, "sample", sample_barcode)
                self.emit( sample_barcode, "patient", patient_barcode)
                
                self.emit( sample_barcode, 'type', "Sample")
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "sample/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( sample_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

        if dataSubType == "portion":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion"):
                portion_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/bcr_portion_barcode"):
                    portion_barcode = c_text
                self.emit( portion_barcode, 'type', "Portion")
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "portion/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( portion_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )
    
        if dataSubType == "analyte":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte"):
                analyte_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/bcr_analyte_barcode"):
                    analyte_barcode = c_text
                self.emit( analyte_barcode, 'type', "Analyte")
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "analyte/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( analyte_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

        if dataSubType == "aliquot":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/samples/sample/portions/portion/analytes/analyte/aliquots/aliquot"):
                aliquot_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/bcr_aliquot_barcode"):
                    aliquot_barcode = c_text
                self.emit( aliquot_barcode[:16], 'analyte', aliquot_barcode ) #simplfying the TCGA id hierarchy
                self.emit( aliquot_barcode, 'sample', aliquot_barcode[:16] )
                self.emit( aliquot_barcode, 'type', "Aliquot")
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "aliquot/*"):
                    if 'xsd_ver' in c_attr:
                        self.emit( aliquot_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

        if dataSubType == "drug":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/drugs/drug"):
                drug_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/bcr_drug_barcode"):
                    drug_barcode = c_text
                self.emit( patient_barcode, 'drug', drug_barcode )
                self.emit( drug_barcode, 'type', "DrugEvent" )                
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "drug/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( drug_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

        if dataSubType == "radiation":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/radiations/radiation"):
                radiation_barcode = None
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/bcr_radiation_barcode"):
                    radiation_barcode = c_text
                self.emit( patient_barcode, 'radiation', radiation_barcode )
                self.emit( radiation_barcode, 'type', "RadiationEvent" )
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "radiation/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( radiation_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

        if dataSubType == "followup":
            for s_node, s_stack, s_attr, s_text in dom_scan(root_node, "tcga_bcr/patient/follow_ups/follow_up"):
                follow_up_barcode = None
                sequence = s_attr['sequence']
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "follow_up/bcr_followup_barcode"):
                    follow_up_barcode = c_text
                self.emit( patient_barcode, 'followup', follow_up_barcode  )
                self.emit( follow_up_barcode, 'type', 'FollowupEvent')
                
                #follow_up_data = { "sequence" : {"value" : sequence}}    
                self.emit( follow_up_barcode, 'sequence', sequence)
                for c_node, c_stack, c_attr, c_text in dom_scan(s_node, "follow_up/*"):
                    if 'xsd_ver' in c_attr:
                        if len(c_text):
                            self.emit( follow_up_barcode, c_attr.get('preferred_name', c_stack[-1]), c_text )

    def emit(self, sub, pred, obj):
        if pred == 'type':
            self.gr.add( (TCGA_NS[sub], RDF.type, TCGA_OWL[obj]) )
        else:
            if (pred in pred_mapping):
                obj_val = pred_mapping[pred](obj)
            else:
                obj_val = Literal(obj)
            self.gr.add( (TCGA_NS[sub], TCGA_OWL[pred.replace(' ', '_')], obj_val) )
        
def scandirs(path, filter_re):
    if os.path.isdir(path):
        out = []
        for a in glob(os.path.join(path, "*")):
            out.extend(scandirs(a, filter_re))
        return out
    else:
        name = os.path.basename(path)
        if filter_re.search(name):
            return [path]
        return []
        
    

def main_list(options):

    q = CustomQuery("Archive[@isLatest=1][Platform[@alias=bio]]")
    out = {}
    for e in q:
        name = e['baseName']
        if name not in out:
            print name
            out[name] = True


if __name__ == "__main__":
    
    parser = ArgumentParser()
    #Stack.addJobTreeOptions(parser) 

    subparsers = parser.add_subparsers(title="subcommand")

    parser_list = subparsers.add_parser('list')    
    parser_list.set_defaults(func=main_list)


    parser_build = subparsers.add_parser('build')    
    parser_build.add_argument("basename")
    parser_build.set_defaults(func=main_build)
    parser_build.add_argument("-m", "--mirror", dest="mirror", help="Mirror Location", default=None)
    parser_build.add_argument("-w", "--workdir", dest="workdir_base", help="Working directory", default="/tmp")
    parser_build.add_argument("-o", dest="output", default=None)



    args = parser.parse_args()
    sys.exit(args.func(args))
