import random
import json
from randomtimestamp import randomtimestamp

mpp_lifetime_data_final=[]
mpp_lifetime_data=[]

for x in range(4000):
  print(" Records created {}!".format(x))
  category_rand_no=random.randrange(0, 4)
  rand_creative = random.randrange(0, 3)

  mpp_lifetime_data = {
      "Reporting date":0,
      "Scheme Name": random.randrange(4000, 7000),
      "Purchase Date": random.randrange(0, 30),
      "Purchase Amount": random.randrange(100, 1000),
      "No. of units purchased": random.randrange(2, 4),
      "Purchase Price/NAV":random.randrange(2, 4),
      "Closing unit balance":random.randrange(2, 40),
  }
  
  # print("data is {}".format(mpp_lifetime_data))
  mpp_lifetime_data_final.append(mpp_lifetime_data)
  # print("final data is {}".format(mpp_lifetime_data_final))


f = open("mpp_lifetime_data.json", "w")
f.write(json.dumps(mpp_lifetime_data_final))
f.close()

import pandas as pd
df = pd.DataFrame(mpp_lifetime_data_final)
df.to_csv('C:\\Users\\Gajraj\\Python\\Demo\\Travel\\mpp_lifetime_data.csv')
			
