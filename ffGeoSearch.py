"""
Faultline-Friendly Geohash Search

Written by Bob Hitching, October 2009

http://hitching.net/contact
Twitter @hitching

Usage in three easy steps:

1. initialize search
>>> geo = ffGeoSearch(**kwargs)
	
where kwargs contains:
bbox - bounding box "west, south, east, north"
limit - number of markers to fetch
correction - 0 = off, 1 = on, 2 = double, increment further at your own CPU risk!
border - if a sub-query will be less than this mix (default value = 0.15), do not split.  instead, nudge a single query to safety
cache_ttl - memcache ttl. non zero will also trigger precision rounding of bounding box to increase cache hit rate
logging - set to True to also generate geo.log for debugging

2. execute search
>>> geo.search('SELECT * FROM ffMarker')

3. scan results
>>> for result in geo.results: logging.info(result)
	
See http://geohash-fcdemo.appspot.com/ for the demo
"""

# datastore
from google.appengine.ext import db

# geohash from http://mappinghacks.com/code/geohash.py.txt
import geohash

# asynctools from http://code.google.com/p/asynctools/
from asynctools import CachedMultiTask, AsyncMultiTask, QueryTask

# needed for precision rounding which is used to increase cache hits
from math import log10

# splits a bbox spatial query into 1, 2 or 4 geohash queries
class ffGeoSearch(object):

	# the width of a split hair
	precision = 1e-8

	# initialize a search
	def __init__(self, **kwargs):

		if 'bbox' in kwargs:
			# bbox standard is west, south, east, north
			[self.west, self.south, self.east, self.north] = map(float, kwargs['bbox'].split(','))
		else:
			[self.west, self.south, self.east, self.north] = (-180, -90, 180, 90)

		# global wraparound
		if cmp(self.west, 0) == cmp(self.east, 0) and self.east < self.west:
			[self.west, self.east] = (-180, 180)

		if 'limit' in kwargs:
			self.limit = kwargs['limit']
		else:
			self.limit = 1000
			
		if 'correction' in kwargs:
			self.correction = kwargs['correction']
		else:
			self.correction = 0

		if 'border' in kwargs:
			self.border = float(kwargs['border'])
		else:
			self.border = 0.15
			
		# cached or not?
		if 'cache_ttl' in kwargs and kwargs['cache_ttl'] > 0:
			self.cache = True
			self.task_runner = CachedMultiTask(time=kwargs['cache_ttl'])
		else:
			self.cache = False
			self.task_runner = AsyncMultiTask()
			
		# keep some logging
		self.log = []
		if 'logging' in kwargs and kwargs['logging'] == True:
			self.logging = True
		else:
			self.logging = False

		# special cases apply for crossing the dateline
		wraparound = self.west > self.east
		span = self.east - self.west
		if wraparound: span += 360

		# special special case of 180 to -180 lng span
		if span == 0: span = 360
		
		# if caching, use precision rounding to increase chance of a hit
		if self.cache:
			lng_prec = int(1-round(log10(span)))
			self.west = round(self.west, lng_prec)
			self.east = round(self.east, lng_prec)
			
			lat_prec = int(1-round(log10(self.north - self.south)))
			self.south = round(self.south, lat_prec)
			self.north = round(self.north, lat_prec)

		# make sure the precision rounding or the client isn't on some other planet
		self.west = max([-180,min([180-self.precision, self.west])])
		self.south = max([-90,min([90-self.precision, self.south])])
		self.east = max([-180,min([180-self.precision, self.east])])
		self.north = max([-90,min([90-self.precision, self.north])])
		
		# array of bounds
		self.boxes = []

		# whole box
		self.boxes.append({
			'south' : self.south,
			'west' : self.west,
			'north' : self.north,
			'east' : self.east,
			'limit' : self.limit
		});

		# 0, 1, 2 (double)
		for count in range(self.correction):
			split_boxes = []
			for box in self.boxes:
				split_boxes += self.split(box)
		
			self.boxes = split_boxes
		
		# nudge final boxes without splitting further
		if self.correction > 0:
			for box in self.boxes:
				box = self.split(box, False)[0]
	
		#logging.info(self.boxes)

		if self.logging:
			for box in self.boxes:
				self.log.append({
					'type' : 'bounds',
					'geometry' : {
						'type' : 'Polygon',
						'coordinates' : [ [box['west'], box['south']], [box['east'], box['south']], [box['east'], box['north']], [box['west'], box['north']], [box['west'], box['south']] ]
					}
				})
		
		
	# split box to avoid faultlines
	def split(self, box, split=True):

		# return array
		boxes = [box]
	
		# special cases apply for crossing the dateline
		wraparound = box['west'] > box['east']
		span = box['east'] - box['west']
		if wraparound: span += 360

		# locate faultlines; the epicentre is the centre of the geohash bounding box around the sw and ne corners
		sw_geostring = geohash.Geostring((box['west'], box['south']))
		ne_geostring = geohash.Geostring((box['east'], box['north']))
		[fault_lng, fault_lat] = (sw_geostring + ne_geostring).point()

		# TODO check this
		if wraparound: fault_lng = -180
		
		# crossing the latitude fault line
		if cmp(box['south'], fault_lat) != cmp(box['north'], fault_lat):
			fault_mix = (box['north'] - fault_lat) / (box['north'] - box['south'])
		
			if fault_mix < self.border:
				# nudge down
				boxes[0]['north'] = fault_lat - self.precision
				
			elif fault_mix > (1 - self.border):
				# nudge up
				boxes[0]['south'] = fault_lat
				
			elif split == True:
				# split into two boxes
				new_limit = int(box['limit'] * fault_mix)
			
				boxes.append({
					'south' : fault_lat,
					'west' : box['west'],
					'north' : box['north'],
					'east' : box['east'],
					'limit' : new_limit
				})				
			
				boxes[0]['north'] = fault_lat - self.precision
				boxes[0]['limit'] -= new_limit

		# crossing the longitude fault line
		if wraparound or cmp(box['west'], fault_lng) != cmp(box['east'], fault_lng):
			more_boxes = []
			
			# TODO check this
			fault_mix = (box['east'] - fault_lng) / span
			#east_span = self.east if self.east > 0 else self.east + 180
			# west_span = 180-self['west'] if self['west'] > 0 else -self['west']
			
			for box in boxes:
				if fault_mix < self.border:
					# nudge left
					box['east'] = 179.9999999 if wraparound else fault_lng - self.precision
					
				elif fault_mix > (1 - self.border):
					# nudge right
					box['west'] = -180.0 if wraparound else fault_lng
					
				elif split == True:
					# split into two boxes
					new_limit = int(box['limit'] * fault_mix)
					more_boxes.append({
						'south' : box['south'],
						'west' : -180.0 if wraparound else fault_lng,
						'north' : box['north'],
						'east' : box['east'],
						'limit' : new_limit
					})
					box['east'] = 179.9999999 if wraparound else fault_lng - self.precision
					box['limit'] -= new_limit
		
			boxes += more_boxes
				
		return boxes
	
	
	# args is additional parameters to bind to the gql, e.g. :query
	# using asynctools to fetch queries in parallel	
	def search(self, gql):
		# bounded search
		gql += (' AND' if 'WHERE' in gql else ' WHERE') + ' geohash > :sw_geohash AND geohash < :ne_geohash ORDER BY geohash'
		query = db.GqlQuery(gql)

		kwargs = {}
		
		for box in self.boxes:
			kwargs['sw_geohash'] = str(geohash.Geohash((box['west'], box['south'])))
			kwargs['ne_geohash'] = str(geohash.Geohash((box['east'], box['north'])))
			query.bind(**kwargs)
			self.task_runner.append(QueryTask(query, limit=box['limit']))
			
			if self.logging:
				self.log.append({
					'type' : 'message',
					'content' : 'SELECT * FROM myMarkers WHERE geohash > ' + kwargs['sw_geohash'] + ' AND geohash < ' + kwargs['ne_geohash'] + ' LIMIT ' + str(box['limit'])
				})	
			
		#logging.info(kwargs)

		self.task_runner.run()
		
		# dict of resultSet arrays
		self.results = []
		
		for key in range(len(self.task_runner)):
			self.results += self.task_runner[key].get_result()
