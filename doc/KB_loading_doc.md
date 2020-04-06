### Documentation of libs used for loading content into KB

1.  [KB_pattern_writer.add_anatomy_image_set](https://github.com/VirtualFlyBrain/VFB_neo4j/blob/master/src/uk/ac/ebi/vfb/neo4j/KB_tools.py#L642) 

```.py
from uk.ac.ebi

kbw = KB_pattern_writer(endpoint, usr, pwd) # Please request details of endpoint and credentials to use this.
 
kbw.add_anatomy_image_set(anatomical_type='FBbt_', label='fu neuron of Zwart2019', 
                          dataset='Zwart2019', start=100000, template='VFBc_00050000', imaging_type = 'TEM')
                          
# Note tempalte currently requires channel ID.  These can be looked up on pdb easily, but better to switch this to names.  

```

1. [KB_pattern_writer.add_dataSet]()
1. Update ontology - [node_importer.update_from_obograph](https://github.com/VirtualFlyBrain/VFB_neo4j/blob/master/src/uk/ac/ebi/vfb/neo4j/KB_tools.py#L459)
1. Update Features from FlyBase [node_importer.update_current_features_from_FlyBase]()
1. Load new feature from FlyBase [node_importer.update_from_flybase(['list of feature ids'])]()
                          
