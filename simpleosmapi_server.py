import sqlite3, os, sys,time, json

import pkg_resources,mimetypes, argparse
import xml.etree.ElementTree as ET

from bottle import route, run, template,static_file,request,post, response, put, hook
import bottle

from simpleosmapi import to_xml, OsmData, read_osm_change_xml, make_sqlite, make_osm_xml, osm_headers


parser = argparse.ArgumentParser(description="""
server responding to openstreetmap api calls""")

parser.add_argument("filename", metavar='filename', type=str, nargs=1,
    help="sqlite database")
parser.add_argument("-i", "--user_id", metavar='userid', type=int,default=1)
parser.add_argument("-u", "--user_name", metavar='username', type=str,default="one")
parser.add_argument("-p", "--port", metavar='port', type=int,default=9005)
parser.add_argument("-c", "--create", action='store_true')

args = parser.parse_args()
print(args)
filename = args.filename[0]



if not os.path.exists(filename):
    if args.create:
        make_sqlite(filename,True)
    else:
        raise Exception("database %s doesn't exist" % filename)
    

    
stored_data = OsmData(filename, args.user_id, args.user_name)

@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'
    
    
def resource_file(fname):
    response.set_header('Content-type', mimetypes.guess_type(fname))
    return static_path(fname)
    #return pkg_resources.resource_stream(rg.__name__,'data/'+fname)



    

    

@route('/api/capabilities')
def capabailities_x():
    response.content_type = "text/xml"
    return to_xml('osm', osm_headers, None, capabilities_content)    

@route('/api/0.6/capabilities')
def capabailities():
    response.content_type = "text/xml"
    return to_xml('osm', osm_headers, None, capabilities_content)

capabilities_content = [
    ('api',{},None, [
        ('version',{'minimum':'0.6','maximum': '0.6'},None,None),
        ('area',{'maximum': '0.25'},None,None),
        ('note_area',{'maximum': '25'},None,None),
        ('tracepoints',{'per_page': '5000'},None,None),
        ('changesets',{'maximum_elements': '10000'},None,None),
        ('timeout',{'seconds': '300'},None,None),
        ('status',{'database': "online", 'api': "online", 'gpx': "online"},None,None),
    ]),
    ('policy',{},None, [
        ('imagery', {}, None, [
            ('blacklist', {'regex':".*\.google(apis)?\..*/(vt|kh)[\?/].*([xyz]=.*){3}.*"},None,None),
            ('blacklist', {'regex':"http://xdworld\.vworld\.kr:8080/.*"},None,None),
        ]),
    ]),
]


@route('/api/0.6/changesets')
def changesets():
    response.content_type = 'text/xml'
    return to_xml('osm',osm_headers,None,[changeset_xml(c) for c in stored_data.changesets.itervalues()])
    

@route('/api/0.6/changeset/create',method=['OPTIONS','PUT'])
def changeset_create():
    
    if request.method=='OPTIONS':
        
        response.headers['Access-Control-Allow-Origin'] = request.headers['Origin']
        response.headers['Access-Control-Allow-Methods']= request.headers['Access-Control-Request-Method']
        print(request.method, list(request.headers.items()), list(response.headers.items()))
        return 
    
    chg = stored_data.next_changeset()
    return str(chg.id)
    

def changeset_xml(chg):
    props = {'id':chg.id, 'user': chg.user, 'uid': chg.uid, 'created_at': chg.created_at, 'open': 'true' if chg.active else 'false'}
    
    props['min_lon'] = str(chg.minlon*0.0000001) if chg.minlon else '0'
    props['min_lat'] = str(chg.minlat*0.0000001) if chg.minlat else '0'
    props['max_lon'] = str(chg.maxlon*0.0000001) if chg.maxlon else '0'
    props['max_lat'] = str(chg.maxlat*0.0000001) if chg.maxlat else '0'
    children=[('tag',{'k':k,'v':v},None,None) for k,v in chg.tags.items()]
    return ('changeset',props,None,children if children else None)

@put('/api/0.6/changeset/<cid:int>')
def changeset_reopen(cid):
    req_data = request.body.read()
    
    ele=ET.fromstring(req_data)
    if len(ele)!=1:
        raise Exception("unexpected data: "+req_data)
    if ele[0].tag != 'Changeset':
        raise Exception("unexpected data: "+req_data)
    tags = dict((t.attrib['k'], t.attrib['v']) for t in ele[0] if t.tag=='tag')
    
    chg=stored_data.add_changeset_tags(cid, tags)
    response.content_type='text/xml'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return to_xml('osm',osm_headers,None,[changeset_xml(chg)])
    

@post('/api/0.6/changeset/<cid:int>/upload')
def changeset_upload(cid):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if not cid in stored_data.changesets:
        response.response_code = 404
        return
    
    if not stored_data.changesets[cid].active:
        response.response_code = 409
        return "changeset %d closed" % cid
    
    
    req_data = request.body.read()
    
    elements = list(read_osm_change_xml(req_data))
    response_data = stored_data.add_changeset_data(cid, elements)
    
    response.content_type = 'text/xml'
    return to_xml('diffResult', {'generator':'simpleosmserver', 'version': "0.6"}, None, response_data)

@put('/api/0.6/changeset/<cid:int>/close')
def changeset_close(cid):
    print('changeset_close', cid)
    stored_data.close_changeset(cid)
    stored_data.save()
    return
    
@route('/api/0.6/map')
def map_data():
    rd = request.query.decode()
    #print(req_data)
    
    
    response.content_type = 'text/xml'
    response.headers['Access-Control-Allow-Origin'] = '*'
    
    
    box = None
    if 'bbox' in rd:
        box=[float(q) for q in rd['bbox'].split(",")]
    
    eles = stored_data.iter_elements(box)
    
    return make_osm_xml(eles)
    
    

@route('/api/0.6/user/details')
def user_details():
    response.content_type = 'text/xml'
    response.headers['Access-Control-Allow-Origin'] = '*'
    
    ll = [('user',{'display_name':v, 'id': k},None,[]) for k,v in stored_data.users.items()]
    
    return to_xml('osm',osm_headers,None,ll)

@route('/oauth/authorize')
def ouath2():
    response.headers['Access-Control-Allow-Origin'] = '*'
    return {'access_token':'frog','token_type':'what'}

@route("/")
def index():
    return static_file('index.html', root='./')

@route("/<fname:path>")
def file(fname):
    return static_file(fname, root='./')

if __name__ == "__main__":
    run(host='localhost', port=args.port)
