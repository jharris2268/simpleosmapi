from .elements import Node, Way, Relation, Changeset, element_key
from .xml import _mkint
import json,time, sqlite3, os



def make_sqlite(fn, create=False, readonly=False):
    """open an sqlite connection to given filename. If empty, and
create=True, create tables

Args:
    filename (str): sqlite filename
    create (bool): create schema if empty
Returns:
    sqlite3 connection object

"""
    if create and readonly:
        raise Exception("incompaitable options create and readonly")
    if not create:
        if not os.path.exists(fn):
            raise Exception("file %s doesn't exist" % fn)
    
    conn=None
    if readonly:
        conn=sqlite3.connect('file:%s?mode=ro' % fn, uri=True)
    else:
        conn=sqlite3.connect(fn,isolation_level=None)
    try:
        conn.execute("select count(1) from changesets")
        return conn
    except:
        pass
    if not create:
        raise Exception("not the expected schema")
        
        
    box="minlon int, minlat int, maxlon int, maxlat int"
    conn.execute("create table changesets (id integer, user string, uid integer, created string, tags blob, "+box+")")
    conn.execute("create table users (id integer, displayname string)")    
    
    common = "id integer, current bool, changeset integer, version integer, timestamp string, user string, uid integer, visible bool, tags blob"
    
    conn.execute("create table node ("+common+", lon int, lat int)")
    conn.execute("create table way  ("+common+", refs blob, "+box+")")
    conn.execute("create table relation ("+common+", members blob, "+box+")")
    conn.execute("create index node_id on node (id)")
    conn.execute("create index way_id on way (id)")
    conn.execute("create index relation_id on relation (id)")
    conn.execute("create index node_loc on node (lon,lat)")
    conn.execute("create index way_box on way (minlon,minlat,maxlon,maxlat)")
    
    return conn
def overlaps(A, B):
    if A is None or B is None: return True
    if A[0]>B[2]: return False
    if A[1]>B[3]: return False
    if B[0]>A[2]: return False
    if B[1]>A[3]: return False
    return True

boxstr = lambda bb: "[%10d %10d %10d %10d]" % (bb[0],bb[1],bb[2],bb[3])

tagstr = lambda tt: "{%s}" % ", ".join("%s='%.20s'" % (k,v) for k,v in tt.iteritems())


def _make_ele_curs(ty, row):
    if ty=='node':
        return Node(row[0],row[2],row[3],row[4],row[5],row[6],json.loads(row[8]),row[7],row[9],row[10],None)
    
    if ty=='way':
        return Way(row[0],row[2],row[3],row[4],row[5],row[6],json.loads(row[8]),row[7],json.loads(row[9]),None if row[10] is None else row[10:14])
    
    if ty=='relation':
        return Relation(row[0],row[2],row[3],row[4],row[5],row[6],json.loads(row[8]),row[7],json.loads(row[9]),None if row[10] is None else row[10:14])




def _make_changeset(row):
    return Changeset(row[0],row[1],row[2],row[3],json.loads(row[4]),None if row[5] is None else row[5:9],False )
    

def _eles_dict(ee):
    res={}
    for e in ee:
        if not e.type in res: res[e.type]={}
        res[e.type][e.id]=e
    return res

def _find_relations(eles, ni, wi):
    rr = []
    ri=set([])
    for e in eles:
        if e.type == 'relation':
            if any(m for m in e.members if (m['type']=='node' and m['ref'] in ni) or (m['type']=='way' and m['ref'] in wi)):
                ri.add(e.id)
            
            if any(m for m in e.members if m['type']=='relation'):
                rr.append(e)
    print("%d rels with mem, %d rel rels" % (len(ri),len(rr)))
    for i in range(5):
        for r in rr:
            if any(m for m in r.members if m['type']=='relation' and m['ref'] in ri):
                #print("%d has rel %d" % (r.id,m['ref']))
                ri.add(r.id)
    print("%d rels" % len(ri))
    return ri
        

def _iter_elements(curs, box):
    
    if box is None:
        return _iter_elements_int(curs, None)

    
    boxp=[_mkint(box[0]),_mkint(box[1]),_mkint(box[2]),_mkint(box[3])]
    
    eles = list(_iter_elements_int(curs, boxp))
    if not eles:
        return
    
    #print('have %d eles' % len(eles))
    ww = [e for e in eles if e.type=='way' and overlaps(boxp, e.bbox)]
    #print('have %d ways' % len(ww))
    ni = set(n for w in ww for n in w.refs)
    nm = ni.difference(set(e.id for e in eles if e.type=='node'))
    wi=set(w.id for w in ww)
    
    ri = _find_relations(eles, ni, wi)    
    
    #print('have %d nodes [%d missing]' % (len(ni), len(nm)))
    if nm:
        if len(nm)>50:
            raise Exception("too many nodes missing? [%d]" % len(nm))
        for i in nm:
            curs.execute("select * from node where id=? and current=1 and visible=1", (i,))
            n=list(curs)
            if len(n)!=1:
                print("still missing node %d" % i)
            else:
                eles.append(_make_ele_curs('node',n[0]))
        eles.sort(key=element_key)
    return _filter_eles(eles, wi, ni,ri)
    
def _filter_eles(eles, wi, ni, ri):
    
    for e in eles:
        if e.type=='node' and e.id in ni:
            yield e
        elif e.type=='way' and e.id in wi:
            yield e
        elif e.type=='relation' and e.id in ri:
            yield e
            
            
    
    
def _iter_elements_int(curs, boxp):
    eles=[]
    for ty in ('node','way','relation'):
        qu = "select * from "+ty+" where current=1 and visible=1"
        
        if boxp is None:
            qu += " order by id"
            curs.execute(qu)
        else:
            if ty=='node':
                qu += " and lon>=?-1000000 and lat>=?-1000000 and lon<=?+1000000 and lat<= ?+1000000"
                curs.execute(qu,tuple(boxp))
            elif ty=='way':
                qu += " and maxlon>=? and maxlat>=? and minlon<=? and minlat<=?"
                curs.execute(qu,tuple(boxp))
            else:
                curs.execute(qu)
            
        
        
        
        for rr in curs:                
            yield _make_ele_curs(ty, rr)
            
    
