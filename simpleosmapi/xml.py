from .elements import Node, Way, Relation
import urllib.request

try:
    import lxml.etree as ET
except ImportError as ie:
    print(ie)
    import xml.etree.ElementTree as ET
    
    
def _to_xml_internal(tag, props, data, children):
        
    root = ET.Element(tag)
    for k,v in props.items():
        root.attrib[k]=str(v)
    
    if not data is None:
        root.set_data(data)
    
    if not children is None:
        for a,b,c,d in children:
            root.append(_to_xml_internal(a,b,c,d))
    
    return root
    

def to_xml(tag, props, data, children, pretty_print=False):
    """constructs an xml from given data

Args:
    tag (str): tag name
    props (dict): properties
    data (str): tag contents
    children (iterable or None): tuples of (tag, props, data, children)
    pretty_print (bool): pretty_print result
Returns:
    str xml data as string
    
Examples:
    >>> to_xml("tag", {"a":"b", "c":"d"}, "", [("child", {}, "data", None)], True)
    \"\"\"<tag a="b" c="d">
        <child>data</child>
    </tag>\"\"\"

"""    

    
    xx = _to_xml_internal(tag,props,data,children)
    try:
        pp=indent is not None
        return ET.tostring(xx,pretty_print=pp)
    except:
        return ET.tostring(xx)

def _mkint(ff):
    if ff<0:
        return int(ff*10000000-0.5)
    return int(ff*10000000+0.5)


def _read_obj(ele, active=True):
    tags = dict((t.attrib['k'],t.attrib['v']) for t in ele if t.tag=='tag')
    
    id = int(ele.attrib['id'])
    changeset = int(ele.attrib['changeset']) if 'changeset' in ele.attrib else None
    version = int(ele.attrib['version']) if 'version' in ele.attrib else None
    timestamp = ele.attrib['timestamp'] if 'timestamp' in ele.attrib else None
    user = ele.attrib['user'] if 'user' in ele.attrib else None
    uid = int(ele.attrib['uid']) if 'uid' in ele.attrib else None
    
            
    if ele.tag == 'node':
        lon,lat=None,None
        if 'lon' in ele.attrib:
            lon = _mkint(float(ele.attrib['lon']))
            lat = _mkint(float(ele.attrib['lat']))
            
        return Node(id, changeset, version, timestamp, user, uid, tags, active, lon, lat)
    
    elif ele.tag=='way':
        refs = [int(nd.attrib['ref']) for nd in ele if nd.tag=='nd']
        return Way(id, changeset, version, timestamp, user, uid, tags, active, refs)
    
    elif ele.tag=='relation':
        members = [{'type': mem.attrib['type'], 'ref': int(mem.attrib['ref']), 'role': mem.attrib['role']} for mem in ele if mem.tag=='member']
        return Relation(id, changeset, version, timestamp, user, uid, tags, active, members)

def read_osm_xml(download_text):
    """read osm elements from osm xml string

Args:
    download_text (str): xml data
Yields:
    Node, Way or Relation objects
"""
    root=ET.fromstring(download_text)
    for ele in root:
        yield _read_obj(ele)
    
def read_osm_change_xml(upload_text):
    """read osm elements from osm change xml string

Args:
    download_text (str): xml data
Yields:
    tuples of change type ('create', 'modify', 'delete') and
    Node, Way or Relation objects
"""
    root = ET.fromstring(upload_text)
    
    for group in root:
        for ele in group:
            yield group.tag, _read_obj(ele,group.tag!='delete')

def elements_from_api(box, host='http://localhost:9005'):
    url = '%s/api/0.6/map?bbox=%f,%f,%f,%f' % (host,box[0],box[1],box[2],box[3])
    txt =urllib.request.urlopen(url).read()
    return read_osm_xml(txt)

def make_osm_xml(eles):
    resp = []
    for ele in eles:
        props = {'id': ele.id, 'version': ele.version, 'timestamp': ele.timestamp, 'user': ele.user, 'uid': ele.uid, 'changeset': ele.changeset}
        data = [('tag',{'k':k,'v':v},None,None) for k,v in ele.tags.items()]
        if ele.type=='node':
            props['lon'] = ele.lon*0.0000001
            props['lat'] = ele.lat*0.0000001
        elif ele.type=='way':
            data += [('nd', {'ref': n},None,None) for n in ele.refs]
        elif ele.type=='relation':
            data += [('member', m,None,None) for m in ele.members]
        
        
        resp.append((ele.type,props,None,data))
    
    return to_xml('osm',osm_headers,None,resp,0)


def make_osm_change_xml(ele_changes):
    resp = []
    
    lastct = None
    part = []
    for changetype, ele in ele_changes:
        if changetype != lastct:
            if part:
                resp.append((lastct,{},None,part))
            lastct = changetype
            part = []
        
        
        props = {'id': ele.id, 'version': ele.version, 'timestamp': ele.timestamp, 'user': ele.user, 'uid': ele.uid, 'changeset': ele.changeset}
        data = [('tag',{'k':k,'v':v},None,None) for k,v in ele.tags.items()]
        if ele.type=='node':
            props['lon'] = ele.lon*0.0000001
            props['lat'] = ele.lat*0.0000001
        elif ele.type=='way':
            data += [('nd', {'ref': n},None,None) for n in ele.refs]
        elif ele.type=='relation':
            data += [('member', m,None,None) for m in ele.members]
        part.append((ele.type,props,None,data))
        
    if part:
        resp.append((lastct,{},None,part))
    
    return to_xml('osmChange',osm_headers,None,resp,0)

osm_headers = {'version':"0.6", 'generator': "simpleosmserver server",
    'copyright': "OpenStreetMap and contributors",
    'attribution': "http://www.openstreetmap.org/copyright",
    'license': "http://opendatacommons.org/licenses/odbl/1-0/"}

def commit_changes(ele_changes, host='http://localhost:9005'):
    
    create_request = urllib.request.Request(
        "%s/api/0.6/changeset/create" % (host, ),
        method="PUT")
        
    txt = urllib.request.urlopen(create_request).read()
    changeset_id = int(txt)
    
    xml = make_osm_change_xml(ele_changes)    
    upload_request = urllib.request.Request(
        "%s/api/0.6/changeset/%d/upload" % (host,changeset_id),
        xml,
        method='POST')
    
    resp = urllib.request.urlopen(upload_request).read()
    
    close_request = urllib.request.Request(
        "%s/api/0.6/changeset/%d/close" % (host, changeset_id),
        method="PUT")
    
    return urllib.request.urlopen(close_request).read()
        
    
    
    
