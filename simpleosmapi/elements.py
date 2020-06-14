import json

def _tagstr(tgs):
    return "{%s}" % (", ".join("%s: '%.20s'" % (k,v) for k,v in sorted(tgs.items())),)

def _boxstr(bx):
    return "[% 10d, % 10d, % 10d, % 10d]" % tuple(bx)

class WithBbox:
    def __init__(self, bbox=None):
        self.bbox=bbox
    @property
    def minlon(self):
        if self.bbox: return self.bbox[0]
    
    @property
    def minlat(self):
        if self.bbox: return self.bbox[1]
    
    @property
    def maxlon(self):
        if self.bbox: return self.bbox[2]
    
    @property
    def maxlat(self):
        if self.bbox: return self.bbox[3]
    
    def expand_bbox(self, box):
        if not box:
            return
            
        if self.bbox is None:
            self.bbox = [x for x in box]
        else:
        
            self.bbox = [min(self.bbox[0],box[0]), min(self.bbox[1],box[1]), max(self.bbox[2],box[2]), max(self.bbox[3],box[3])]

        
    @property
    def json(self):
        return dict((k,v) for k,v in self.__dict__.items() if not v is None)
    
class Element(WithBbox):
    def __init__(self, id, changeset, version, timestamp, user, uid, tags, visible, bbox=None):
        WithBbox.__init__(self, bbox)
        self.id = id
        self.changeset=changeset
        self.version = version
        self.timestamp = timestamp
        self.user = user
        self.uid = uid
        self.tags = tags
        self.visible=visible
        self.bbox=bbox
        
        
    

    


class Node(Element):
    def __init__(self, id, changeset, version, timestamp, user, uid, tags, visible, lon, lat, bbox=None):
        Element.__init__(self, id,changeset,version,timestamp,user,uid,tags,visible,bbox)
        self.lon = lon
        self.lat=lat
        self.type='node'
    def __repr__(self):
        return "Node(%d %s % 10d % 10d)" % (self.id, _tagstr(self.tags), self.lon, self.lat)
    
    def insert(self, curs,check=True):
        if check: curs.execute("update node set current=0 where id=?",(self.id,))
        curs.execute("insert into node values (%s)" % ",".join("?"*11), tuple(
            [self.id,True,self.changeset,self.version,self.timestamp,self.user,self.uid,self.visible,
            json.dumps(self.tags),self.lon,self.lat]))
        
    
    def write_bbox(self, curs):
        curs.execute("update node set minlon=?, minlat=?, maxlon=?, maxlat=? where id=?", (self.minlon,self.minlat,self.maxlat,self.maxlon,self.id))
        
class Way(Element):
    def __init__(self, id, changeset, version, timestamp, user, uid, tags, visible, refs, bbox=None):
        Element.__init__(self, id,changeset,version,timestamp,user,uid,tags,visible,bbox)
        self.refs=refs
        self.type='way'
    
    def __repr__(self):
        return "Way(%d %s %d nodes %s)" % (self.id, _tagstr(self.tags), len(self.refs), _boxstr(self.bbox) if self.bbox else '')

    def insert(self, curs,check=True):
        if check: curs.execute("update way set current=0 where id=?",(self.id,))
        curs.execute("insert into way values (%s)" % ",".join("?"*14), tuple(
            [self.id,True,self.changeset,self.version,self.timestamp,self.user,self.uid,self.visible,
            json.dumps(self.tags),json.dumps(self.refs),self.minlon,self.minlat,self.maxlon,self.maxlat]))
class Relation(Element):
    def __init__(self, id, changeset, version, timestamp, user, uid, tags, visible, members, bbox=None):
        Element.__init__(self, id,changeset,version,timestamp,user,uid,tags,visible,bbox)
        self.members=members
        self.type='relation'
    def insert(self, curs, check=True):
        if check: curs.execute("update relation set current=0 where id=?",(self.id,))
        curs.execute("insert into relation values (%s)" % ",".join("?"*14), tuple(
            [self.id,True,self.changeset,self.version,self.timestamp,self.user,self.uid,self.visible,
            json.dumps(self.tags),json.dumps(self.members),self.minlon,self.minlat,self.maxlon,self.maxlat]))

    def __repr__(self):
        return "Relation(%d %s %d members)" % (self.id, _tagstr(self.tags), len(self.members))


class Changeset(WithBbox):
    def __init__(self, id, user, uid, created_at, tags, bbox,active):
        WithBbox.__init__(self, bbox)
        self.id=id
        self.user=user
        self.uid=uid
        self.created_at=created_at
        self.tags=tags
        self.active=active
        
    def insert(self, curs):
        curs.execute("delete from changesets where id=?", (self.id,))
        curs.execute("insert into changesets values (%s)" % ",".join("?"*9), tuple(
            [self.id,self.user,self.uid,self.created_at,
            json.dumps(self.tags),self.minlon,self.minlat,self.maxlon,self.maxlat]))


def element_key(ele):
    ty = 0 if ele.type=='node' else 1 if ele.type=='way' else 2 if ele.type=='relation' else None
    
    return (ty,ele.id)

def element_change_key(ele):
    ty,id = element_key(ele[1])
    vs = ele[1].version
    
    ct = 0 if ele[0]=='delete' else 1 if ele[0]=='modify' else 2 if ele[0]=='create' else None
    
    return (ty,id,vs,ct)
