from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

cloud_config= {
  'secure_connect_bundle': '../secure-connect-cs122d-spring.zip'
}
auth_provider = PlainTextAuthProvider('GQtdIHboroKMJCqUuywiapyD', 
                                      'CYbJngevTcoALZft1F.lFsEYtznzHFKcNOe6PFgFnRHGvTXo3OZA-NZzH4kUMlZCRc,xGDJmHk4,TsZO2aLMhLkG4L+mxc8fKrdbiRSy1bIZpJl3JgZ.iRexErzoJXLf')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")
  

def add_rating(rating):
  session.execute("insert into interchange.ratings (buyer_id, seller_id, quality, pricing, delivery, rating_date) VALUES (%s, %s, %s, %s, %s, %s)", 
                  (rating["buyer_id"], rating["seller_id"], int(rating["quality"]), int(rating["pricing"]), int(rating["delivery"]), rating["rating_date"]))
  

  
if __name__ == "__main__":
  input = '{"buyer_id": "VF0E6", "seller_id": "ZVZGY", "quality": "5", "pricing": "4","delivery": "5", "rating_date": "2022-02-10"}'
  rating = json.loads(input)
  add_rating(rating)

  # test code
  result = session.execute("select * from interchange.ratings where buyer_id = %(bid)s and seller_id = %(sid)s", {'bid': rating["buyer_id"],'sid':rating["seller_id"]}).one() 
  print(result.buyer_id, result.seller_id, result.quality, result.pricing, result.delivery, result.rating_date)