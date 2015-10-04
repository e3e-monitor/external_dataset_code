import shapefile
import sys
import datetime
import elasticsearch

sf = shapefile.Reader('original_data/Kobane_20150122_shp/Kobane_Damage_Sites.shp')

shapes = sf.shapes()
records = sf.records()

es_host = "localhost"
es_port = 9200
es = elasticsearch.Elasticsearch(host=es_host, port=es_port)

if len(shapes) != len(records):
  print "number of shapes does not match number of records"
  sys.exit(1)

for i in xrange(0,len(shapes)-1):
  location = { "lat": shapes[i].points[0][0], "lon": shapes[i].points[0][1] }
  #['SensorDate', 'D', 8, 0], ['SensorID', 'C', 254, 0], ['Confidence', 'C', 254, 0], ['FieldValid', 'C', 254, 0], ['Settlement', 'C', 100, 0], ['Notes', 'C', 100, 0], ['Main_Damag', 'C', 254, 0], ['Grouped_Da', 'C', 254, 0], ['StaffID', 'C', 254, 0], ['EventCode', 'C', 50, 0], ['ImageID_Nu', 'N', 9, 0]
  record = records[i]
  blob = {
    "location"       : location,
    "timestamp"      : datetime.datetime(record[1][0], record[1][1], record[1][2]),
    "sensor_id"      : record[2],
    "confidence"     : record[3],
    "valid"          : record[4],
    "settlement"     : record[5],
    "notes"          : record[6],
    "damage"         : record[7],
    "grouped_damage" : record[8],
    "staff_id"       : record[9],
    "event_code"     : record[10],
    "image_id"       : record[11]
  }
  
  es.index(index='external', doc_type='kobane', body=blob)
  print blob
  # print shapes[i].points
  # print records[i][1:]

print sf.fields

