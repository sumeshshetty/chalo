# chalo

**************X*******X************* LOGIC *********************X**************************X******************

using

SELECT 
d."sessionstarttime: descending",
d."sessionendtime: descending",
d."routeid.keyword: descending",
g.longitude, g."latitude", g."timestamp"
FROM "chalo"."raw_dispatch" d 
full outer join "chalo"."raw_gps_raw"  g
on d."userid.keyword: descending" = g.vehicle_no
and g."timestamp" between d."sessionstarttime: descending" and d."sessionendtime: descending"
limit 10

stop info

[{
   "BUS":{
      "AEVKsBkZ":{
         "lat":23.242775,
         "lon":77.245355,
         "name":"Rajput Dhaba"
      },
      "APTbXPfu":{
         "lat":23.30228,
         "lon":77.404398,
         "name":"Karond Chouraha"
      }
   }
}]


we can join with stop info or using stop info we need to figure are the location are stop or not os after that processing we get

   
tbl1

sessionstarttime
sessionendtime: 
routeid.
longitude_bus
latitude_bus
timestamp
is_stop: can be true or false
stop name
stop id
longitude_stop
latitude_stop


afte this drop all rows having is_stop:false
drop 
longitude_bus
latitude_bus
longitude_stop
latitude_stop
is_stop


we have tbl2
sessionstarttime
sessionendtime: 
routeid.
timestamp 
day #calculate from timestamp
hr #calculate from timestamp
#minutes
#year
stop name
stop id

from 3-4 in route r1 bus was at stop s1 at time 3 15pm


convert route info to something like

up_route_info_tbl
route id    source   destination  seqno
AfGxJmXG    s1       s2            1
AfGxJmXG    s2       s3            2
AfGxJmXG    s3       s4            3


r2          s1       s2            1
r3          s1       s2            2
r4          s1       s2            3


now join up_route_info_tbl with tbl2 on route_id, source = stop id and get timestamp(conevrt to day and hr year before hand )

#we will require seq no
tbl3
route id    source   destination  seqno day hr timestamp_source

join tbl3 with tbl2 on destination = stop id , hr =hr, day= day, year=year

route id    source   destination  seqno day hr timestamp_source timestamp_dest


calulate total time taken timestamp_dest- timestamp_source


final_tbl
route id    source   destination   day hr time_taken


**************X*******X************* LOGIC *********************X**************************X******************
s3 path: s3://chalo-quess-ml-test/tables/dispatch