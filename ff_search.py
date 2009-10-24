from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from django.utils import simplejson
import logging, os, string, urllib, random

# faultline friendly geo search
import ffGeoSearch

# sample datamodel
class ffMarker(db.Model):
	lat = db.FloatProperty(required=True)
	lng = db.FloatProperty(required=True)
	geohash = db.StringProperty(required=True)
	geostring = db.StringProperty(required=True)

# sample spatial query handler
class SpatialQueryHandler(webapp.RequestHandler):
	def get(self):
	
		kwargs = {}
	
		# bounding box standard is west, south, east, north
		if 'bbox' in self.request.arguments():
			kwargs['bbox'] = self.request.get('bbox')

		# 0 = off, 1 = on, 2 = double
		if 'correction' in self.request.arguments():
			kwargs['correction'] = int(self.request.get('correction'))

		if 'limit' in self.request.arguments():
			kwargs['limit'] = int(self.request.get('limit'))

		if 'border' in self.request.arguments():
			kwargs['border'] = float(self.request.get('border'))
		
		kwargs['cache_ttl'] = 300
		
		if self.request.get('logging', default_value='off') == 'on':
			kwargs['logging'] = True
		
		# initialize search
		geo = ffGeoSearch.ffGeoSearch(**kwargs)
	
		# execute search
		geo.search('SELECT * FROM ffMarker')

		# collect results in an object
		features = []
		for result in geo.results:
			features.append({
				'type' : 'Feature',
				'geometry' : {
					'type' : 'Point',
					'coordinates' : [result['lng'], result['lat']]
				},
				'properties' : {
					'geohash' : result['geohash']
				}
			})
			
		# convert to geojson	
		obj = {
			'type' : 'FeatureCollection',
			'features' : features
		}
		if len(geo.log):
			obj['log'] = geo.log
		
		self.response.headers['Content-Type'] = 'application/json'

		callback = self.request.get("callback")
		if callback:
			self.response.out.write(callback + ' && ' + callback)
		
		self.response.out.write('(' + simplejson.dumps(obj) + ')')

# add 100 random markers
# you will need to call this a few times using /load_sample_data if you want to experiment with ffMarker entities
class LoadSampleData(webapp.RequestHandler):
	def get(self):
		
		inserts = []
		for sample in range(1, 100):
			lat = float(random.randint(-800, 800)/10)
			lng = float(random.randint(-1800, 1800)/10)
			
			marker = ffMarker(
				lat = lat,
				lng = lng,
				geohash = str(geohash.Geohash((lng, lat))),
				geostring = str(geohash.Geostring((lng, lat)))
			)
		
			inserts.append(marker)				

		if (len(inserts)):
			db.put(inserts)
	
application = webapp.WSGIApplication([
	('/ff_search.json', SpatialQueryHandler),
	('/load_sample_data', LoadSampleData)
], debug=True)

def main():
	run_wsgi_app(application)

if __name__ == "__main__":
	main()