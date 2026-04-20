docker exec spark-driver pip install $1
docker exec spark-worker-1 pip install $1
docker exec spark-worker-2 pip install $1