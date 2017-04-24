###  this version of the arcGIS python API has a bug
###  this is a temp workaround

import os

paths = (os.path.join(root, filename)
         for root, _, filenames in os.walk ('/data/pb_files/ZIP')
         for filename in filenames)
for path in paths:
    newname = path.replace('-','_')
    if newname !=path:
        os.rename(path, newname)
        
    
         