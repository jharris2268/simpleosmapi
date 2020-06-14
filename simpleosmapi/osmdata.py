from .elements import WithBbox, Node, Way, Relation, Changeset, element_key, element_change_key
from .xml import ET, read_osm_xml, read_osm_change_xml, _mkint
from .database import make_sqlite, _iter_elements, _make_changeset, _make_ele_curs
import time


def timestamp():
    """current time in the expected osm format"""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ") 

class OsmData:
    """
Represents a database of osm elements, backed by a sqlite connection.

Data can be read using the elements_iter member function, equivilant
to an GET /api/0.6/map call.

Data can be modified using the next_changeset, add_ele, add_changeset_tags
and close_changeset members, equivilant to the /api/0.6/changeset calls.

Example:
    >>> data = OsmData("filename", 1, "user")
    >>> chg = data.next_changeset() #open new changeset
    >>> r0 = data.add_ele('create', chg.id, 'node', Node(...))
    >>> r1 = data.add_ele('modify', chg.id, 'way', Way(...))
    >>> r2 = data.add_ele('delete', chg.id, 'relation, Relation(...))
    >>> data.add_changeset_tags(chg.id, {'comment':'example'})
    >>> data.close_changeset(chg.id) #finalize changeset
    
"""

    def __init__(self, fn, uid, user):
        """
Args:
    filename (str): filename of existing sqlite database. Call make_sqlite
to create new database
    uid (int): user id for new changesets
    user (str): user name for new changesets.
"""
        self.filename = fn
        self.uid = uid
        self.username = user
        
        self.conn = make_sqlite(self.filename)
       
        self.curs = self.conn.cursor()
        self.curs.execute("select * from changesets")
        self.changesets={}
        for row in self.curs:
            chg = _make_changeset(row)
            self.changesets[chg.id]=chg
        
        
        self.users = {}
        self.curs.execute("select * from users")
        for row in self.curs:
            self.users[row[0]] = row[1]
    
        if not uid in self.users:
            print("new user %d %s" % (uid,user))
            self.curs.execute("insert into users values (?, ?)", (uid, user))
            self.users[uid]=user
            
        if self.users[uid]!=user:
            print("rename user %d from %s to %s" % (uid, self.users[uid], user))
            self.curs.execute("alter users set displayname=? where id=?", (user,uid))
            self.users[uid]=user
        
        if not self.changesets:
            self.next_ids = {'changeset':1,'node':1,'way':1,'relation':1}
        else:
            self.next_ids = {'changeset': max(self.changesets)+1}
            for ty in ('node','way','relation'):
                (curr,), = self.curs.execute("select max(id) from "+ty)
                self.next_ids[ty] = 1 if curr is None else curr+1
        print("have %d changesets, next_ids: %s" % (len(self.changesets), self.next_ids))
    
        self.in_transaction=False
    
    def next_changeset(self):
        """start new changeset

Equivilant to POST /api/0.6/changeset/create call

Returns:
    Changeset object
"""

        cid = self.next_id('changeset')
        self.changesets[cid] = Changeset(cid,self.username,self.uid,timestamp(),{},None,True)
        return self.changesets[cid]
    
    def add_changeset_tags(self, cid, tags):
        """add tags to given changeset

Equivilant to POST /api/0.6/changeset/#id

Args:
    cid (int): Changeset id
    tags (dict): tags to add to changeset
Returns:
    Changeset object
"""


        chg = self.changesets[cid]
        for k,v in tags.items():
            chg.tags[k]=v
        
        return chg
        
    def close_changeset(self, cid):
        """finalize changeset

Equivilant to PUT /api/0.6/changeset/#id/close

Args:
    cid (int): Changeset id
"""
        
        chg=self.changesets[cid]
        chg.active=False
        chg.insert(self.curs)
        
            
        
    
    def find_ele(self, ty, id_):
        """fetch object of given type and id

Args:
    type (str): 'node', 'way' or 'relation'
    id (int): object id
Returns:
    Node, Way or Relation object if present, None otherwise
"""
        rows = list(self.curs.execute("select * from "+ty+" where id=? and current=1", (id_,)))
        if rows and rows[0][1]:
            return _make_ele_curs(ty, rows[0])
    
    
    
    def calc_boxes(self, way):
        
        nn = []
        for n in way.refs:
            e = self.find_ele('node',n)
            if not e:
                print('missing node %d' % (n,))
            else:
                nn.append(e)
        for n in nn:
            way.expand_bbox([n.lon,n.lat,n.lon,n.lat])
        
        for n in nn:
            n.expand_bbox(way.bbox)
            
        return nn
    
    def start_transaction(self):
        """start transaction on internal sqlite connection"""
        pass
        #if self.in_transaction:
        #    self.curs.execute("commit")
            
        #self.curs.execute("begin")
        #self.in_transaction=True
    
    def finish_transaction(self):
        """finish transaction on internal sqlite connection"""
        
        pass
        #if not self.in_transaction:
        #    return
        #self.curs.execute("commit")
        #self.in_transaction=False
    
    def save(self):
        """finalize any open changesets, and finish transaction on
internal sqlite connection"""
        for k,v in self.changesets.items():
            if v.active:
                v.active=False
                v.insert(self.curs)
        self.finish_transaction()
        
    
    def next_id(self, ty):
        """id for new object of given type

Called by add_ele when the change type is create

Args:
    type (str): 'node','way' or 'relation'
Returns:
    id (int)
"""

        ans = self.next_ids[ty]
        self.next_ids[ty]+=1
        return ans
        
    
       
    
    def add_ele(self, changeset_id, change_type, element, replacement_ids):
        """add element to the database

In all cases the element user and uid are replaced the with values given
in the constructor. The timestamp is replaced the the current time.

The exact behaviour depends on the change_type:
create: replace placeholder id with one given by next_id, set version to 1
modify: set any existing objects with given id to be invisible, set version
to the last value plus one.
delete: set any existing objects with given id to be invisible

Created element ids are added to replacement_ids {(type,placeholder_id):new_id}.
This is used to replace the placeholder ids in way refs and relation members.

Args:
    changeset_id (int): changeset
    change_type (str): 'create', 'modify' or 'delete'
    element (Node, Way or Relation): element
    replacement_ids (dict): see above
Returns:
    tuple of (element_type,
        {'old_id': int, 'new_id': int, 'new_version: int} (as appropiate),
        None, None)
"""
        
        if element.type=='way':
            for i,n in enumerate(element.refs):
                if ('n',n) in replacement_ids:
                    element.refs[i]=replacement_ids['n',n]
                elif n<0:
                    raise Exception("unknown node %d" % (n,))
                
        elif element.type=='relation':
            for i,mem in enumerate(element.members):
                ii=(mem['type'][0],int(mem['ref']))
                if ii in replacement_ids:
                    mem['ref'] = replacement_ids[ii]
                elif ii[1] < 0:
                    raise Exception("unknown member %s %d" % ii)
        
        element.changeset=changeset_id
        element.user=self.username
        element.uid=self.uid
        element.timestamp=timestamp()
        
            
            
        if change_type=='create':
            old_id = element.id
            element.id = self.next_id(element.type)
            replacement_ids[element.type[0], old_id] = element.id
            
            element.version=1
            
            if element.type=='way': self.calc_boxes(element)
            element.insert(self.curs)
            
            self.changesets[changeset_id].expand_bbox(element.bbox)
            
            return (element.type, {'old_id': old_id,'new_id':element.id,'new_version': element.version},None,None)
            
        elif change_type=='modify':
            
            old_ele=self.find_ele(element.type,element.id)
            element.version = 1 if old_ele is None else old_ele.version+1
            
            if element.type=='way': self.calc_boxes(element)
            element.insert(self.curs)
            
            self.changesets[changeset_id].expand_bbox(element.bbox)
            return (element.type, {'old_id': element.id,'new_id':element.id,'new_version': element.version},None,None)
        
        elif change_type=='delete':
            
            old_ele=self.find_ele(element.type,element.id)
            element.version = 1 if old_ele is None else old_ele.version+1
            
            element.insert(self.curs)
            return (element.type, {'old_id': element.id},None,None)
        else:
            raise Exception('wrong change_type %s' % repr(change_type))
    
    def add_changeset_data(self, cid, elements):
        """add elements to database

Calls add_ele for each element in elements"""
        self.start_transaction()
        response_data = []
        repls = {}
        elements.sort(key=element_change_key)
        
        self.curs.execute("begin")
        pp=[]
        for ty, ele in elements:
            try:
                response_data.append(self.add_ele(cid, ty, ele, repls))
            except:
                pp.append((ty,ele))
        
        
        for i in range(5):
            if pp:
                print('pass %d: have %d problems' % (i,len(pp)))
                qq=[]
                for ty,ele in pp:
                    try:
                        response_data.append(self.add_ele(cid, ty, ele, repls))
                    except:
                        qq.append((ty,ele))
                
                pp=qq
        self.curs.execute("commit")
        if pp:
            print('still have %d problems' % len(pp))
            print(pp)
            raise Exception("failed")
        
        self.finish_transaction()
        print(response_data)
        return response_data
        
    
    def iter_elements(self, box=None):
        
        return _iter_elements(self.curs, box)
        
        

    def elements_dict(self, box=None):
        return _eles_dict(self.iter_elements(box))
        
        
