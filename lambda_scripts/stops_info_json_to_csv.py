import json
import pandas as pd
with open("Stops.json") as file:
	data = json.load(file)


new_json=[]
for stop,values in data["BUS"].items():
	values['stop_id'] = stop
	new_json.append(values)
	#print("--------------------------------")


df = pd.json_normalize(new_json)

df.to_csv("stops.csv",header=True,index=False)