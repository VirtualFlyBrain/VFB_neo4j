## Server dependencies

docker run -d --name pdb -p 7475:7474 -p 7688:7687 --env=NEO4J_AUTH=none --env=NEOREADONLY=false --volume={local path here}:/data/import/ virtualflybrain/docker-vfb-neo4j-productiondb


docker run -d --name pdb -p 7475:7474 -p 7688:7687 --env=NEO4J_AUTH=none --env=NEOREADONLY=false --volume={local path here}:/data/import/ virtualflybrain/docker-vfb-neo4j-productiondb


- docker run -d --name db -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none --env=NEOREADONLY=false --env=NEO4J_dbms_memory_heap_maxSize=1G --volume=$TRAVIS_BUILD_DIR/src/:/import/ virtualflybrain/docker-vfb-neo4j:enterprise 
  - docker run -d --name pdb -p 7475:7474 -p 7688:7687 --env=NEO4J_AUTH=none --env=NEOREADONLY=false --env=NEO4J_CACHE_MEMORY=1G --env=NEO4J_HEAP_MEMORY=2560 --volume=$TRAVIS_BUILD_DIR/src/:/import/ virtualflybrain/docker-vfb-neo4j-productiondb 
  - sleep 50s