import json
import pandas as pd
with open("routeinfo.json") as file:
	data = json.load(file)


new_json=[]
for route_id,values in data.items():
	values['route_id'] = route_id
	values['seq'] =' '.join(map(str, values['seq']))
	try:
		values['sf'] = ' '.join(map(str, values['sf']))
	except KeyError:
		pass

	try:
		values['o'] = ' '.join(map(str, values['o']))
	except KeyError:
		pass

	try:
		values['polyIndicesForStops'] = ' '.join(map(str, values['polyIndicesForStops']))
	except KeyError:
		pass
	new_json.append(values)
	

df = pd.json_normalize(new_json)
#df_1 = df[['route_id','sf','o','polyIndicesForStops']]


df.to_csv("route_info.csv",header=True,index=False)