from glob import glob
from datetime import date, datetime
import argparse
import csv
import re
import os
from math import floor
from sets import Set

unavailable = ['N/A', 'Month rent price is unavailable', '***NO UNITS FOUND***', '***NO CURRENT AVAILABILITY***']

class vulture:
	def __init__(self, args):
		self.input = []
		self.infile = os.path.normpath(args.infile)
		self.yardi = {}
		self.database = {}
		if os.path.split(self.infile)[1] == '': self.infile = os.path.split(self.infile)[0]
		if args.error:
			self.err = self.infile
			self.infile = os.path.split(self.infile)[0]

		m1 = os.path.split(self.infile)[1]
		m2 = os.path.split(os.path.split(self.infile)[0])[1]
		m3 = os.path.split(os.path.split(os.path.split(self.infile)[0])[0])[0]
		self.header = re.sub("[a-zA-Z]", "", m2)
		filename = self.header + "Outputs"
		self.output = os.path.join(m3,  "Output Files/%s/%s" % (filename, m1))
		if not os.path.exists(self.output):
			os.makedirs(self.output)
		database = [x for x in glob(m3 + "/*.*") if "database" in x.lower()][0]
		with open(database, 'r') as d_file:
			for line in csv.DictReader(d_file, delimiter='\t'):
				if not self.database.has_key(line['property_id']):
					self.database[line['property_id']] = {}
				self.database[line['property_id']][line['unit_name']] = line
				if not self.yardi.has_key(line['property_id']):
					self.yardi[line['property_id']] = {}
				self.yardi[line['property_id']][line['unit_name']] = line['floorplan_name']

	def average(self, building):
		output = list(building)
		floorplans = {}
		bedbath = {}
		for unit in building:
			if unit['floorplan_name'] == 'Refresh':
				continue
			keys = [x.strip() for x in re.split("-", unit['floorplan_name'])]
			if not floorplans.has_key(keys[0]):
				floorplans[keys[0]] = [0,0]

			if not bedbath.has_key(keys[1]):
				bedbath[keys[1]] = [0,0]

			floorplans[keys[0]][0] += float(unit['price'])
			floorplans[keys[0]][1] += 1.0
			bedbath[keys[1]][0] += float(unit['price'])
			bedbath[keys[1]][1] += 1.0

		for unit in output:
			if unit['floorplan_name'] == 'Refresh':
				unit['fp_ave'] = unit['bb_ave'] = unit['db_ave'] = unit['pp_sqft'] = 'N/A'
				continue
			keys = [x.strip() for x in re.split("-", unit['floorplan_name'])]
			unit['fp_ave'] = "%0.2f" % ((float(unit['price']) * floorplans[keys[0]][1]) / floorplans[keys[0]][0] - 1.0)
			unit['bb_ave'] = "%0.2f" % ((float(unit['price']) * bedbath[keys[1]][1]) / bedbath[keys[1]][0] - 1.0)
			unit['pp_sqft'] = 'N/A'
			if unit['sqft'] != '' and unit['sqft'] != None and unit['sqft'] != '-':
				unit['pp_sqft'] = "%0.2f" % (float(unit['price']) / float(unit['sqft']))
			unit['db_ave'] = 'N/A'
			db = self.database.get(unit['property_id'], {})
			d_unit = db.get(unit['unit_name'], db.get(re.split("-",unit['unit_name'])[0].strip(), False))
			if d_unit:
				unit['db_ave'] = "%0.2f" % (float(self.database[unit['property_id']][d_unit['unit_name']]['price']) / float(unit['price']) - 1.0)


		return output

	def error(self):
		err, building = [], []
		outpath = os.path.split(self.err)[0]
		with open(self.err, 'r') as r_file:
			input = csv.DictReader(r_file)
			propID = ''
			for line in input:
				if propID != line['property_id']:
					propID = line['property_id']
					err += self.average(building)
					building = []
				building.append(line)
		timestamp = self.timestamp()
		self.write(err, os.path.join(outpath, "%s Error_Trend.csv" % timestamp), None)




	def masteri(self):
		infile = self.infile
		yardi_err = []
		if infile.endswith("/"): 
			infile += "*"
		else: 
			infile += "/*"


		for f in glob(infile):
			with open(f, 'r') as i_file:
				dialect = csv.Sniffer().sniff(i_file.read(), delimiters=",\t")
				i_file.seek(0)
				for line in csv.DictReader(i_file, dialect=dialect):
					result = self.yardi.get(line['property_id'])
					if result:
						un = line['unit_name']
						if result.get(un):
							line['floorplan_name'] = result[line['unit_name']]
							self.input.append(dict(line)) 
						elif un not in unavailable and un != None and un != '':
							yardi_err.append(dict(line))
						else:
							fp = line['floorplan_name']
							if fp != None and fp != '':
								line['floorplan_name'] = "\"%s\"" % fp.strip()
							self.input.append(dict(line))
					else:
						self.input.append(dict(line))
		timestamp = self.timestamp()
		for i in xrange(0, len(self.input)):
			for key, val in dict.iteritems(self.input[i]):
				if val != None:
					self.input[i][key] = val.strip()
				else:
					self.input[i][key] = ''

		self.write(self.input, self.output + "/%s master_input.csv" % timestamp)
		self.write(yardi_err, self.output + "/%s yardi_err.csv" % timestamp)



	def timestamp(self):
		now = datetime.now()
		return "%d.%d.%d %d_%d" % (now.year, now.month, now.day, now.hour, now.minute)

	
	def write(self, lines, outfile, master=False):
		if master == False:
			with open(outfile, 'wb') as w_file:
				w_file.write("property_id,floorplan_name,unit_name,sqft,bed,bath,price,date_available\n")
				for line in lines:
					w_file.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (line.get('property_id', ''),
						line.get('floorplan_name', ''),
						line.get('unit_name', ''),
						line.get('sqft', ''),
						line.get('bed', ''),
						line.get('bath', ''),
						line.get('price', ''),
						line.get('date_available', line.get('available_date', ''))))
		elif master == True:
			lines = sorted(lines, key=lambda x: x['property_id'])
			propID = ''
			now = datetime.today()
			with open(outfile, 'wb') as w_file:
				w_file.write("property_id,floorplan_name,unit_name,sqft,bed,bath,price,date_available\n")

				for line in lines:
					if propID != line.get('property_id'):
						propID = line.get('property_id')		
						w_file.write('%s,Refresh,Refresh,,1,1,%s,unavailable\n' % (propID, "9%s%s" % (str(now.month).zfill(2), str(now.day).zfill(2))))
					
					w_file.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (line.get('property_id', ''),
							line.get('floorplan_name', ''),
							line.get('unit_name', ''),
							line.get('sqft', ''),
							line.get('bed', ''),
							line.get('bath', ''),
							line.get('price', ''),
							line.get('date_available', line.get('available_date', ''))))
		else:
			with open(outfile, 'wb') as w_file:
				writer = csv.DictWriter(w_file, fieldnames=['property_id', 'floorplan_name', 'unit_name', 'sqft', 'bed', 'bath', 'price', 'date_available', 'bb_ave', 'db_ave', 'fp_ave', 'pp_sqft'])
				writer.writeheader()
				writer.writerows(sorted(lines, key=lambda x: x['property_id']))

	def normalize(self, line):
		n_line = dict(line)
		n_line['price'] = int(round(float(re.sub('[^\d.-]', '', n_line['price']))))
		n_line['bed'] = int(float(n_line['bed']))
		n_line['bath'] = floor(float(n_line['bath']) * 2.0)/2.0
		if int(n_line['bath']) == n_line['bath']: n_line['bath'] = int(n_line['bath'])
		if n_line['sqft'] == '-': n_line['sqft'] = ''

		try:
			date = datetime.strptime(line.get('date_available', line.get('available_date', '')),'%m/%d/%Y')
			if n_line.has_key('date_available'):
				n_line['date_available'] = "%s-%s-%s" % (date.year, date.month, date.day)
			else:
				n_line['available_date'] = "%s-%s-%s" % (date.year, date.month, date.day)
		except:
			pass
		try:
			date = datetime.strptime(line.get('date_available', line.get('available_date', '')), '%Y/%m/%d')
			if n_line.has_key('date_available'):
				n_line['date_available'] = "%s-%s-%s" % (date.year, date.month, date.day)
			else:
				n_line['available_date'] = "%s-%s-%s" % (date.year, date.month, date.day)
		except:
			pass
		n_line['floorplan_name'] = n_line['floorplan_name'].strip("\"")
		n_line['floorplan_name'] += " - %s/%s" % (n_line['bed'], n_line['bath'])
		n_line['unit_name'] += " - (%s)" % n_line['floorplan_name']
		n_line['floorplan_name'] = "\"%s\"" % n_line['floorplan_name']
		n_line['unit_name'] = "\"%s\"" % n_line['unit_name']

		return n_line

	def filter_lines(self, line):
		if line.get('floorplan_name') in unavailable:
			return None

		try:
			int(line['property_id'])
			if line['unit_name'] == None or line['unit_name'] == '': return False
			if line['floorplan_name'] == None or line['floorplan_name'] == '': return False
			if float(re.sub('[^\d.-]', '', line['price'])) < 350: return False
			sqft = line['sqft']
			if not (sqft == '' or sqft == '-'):
				if int(sqft) == 0: return False
			float(line['bed'])
			float(line['bath'])
			errors = 0
			try:
				datetime.strptime(line.get('date_available', line.get('available_date', '')),'%m/%d/%Y')
			except:
				errors += 1

			try:
				datetime.strptime(line.get('date_available', line.get('available_date', '')), '%Y/%m/%d')
			except:
				errors += 1
			if errors == 2: return False

		except:
			return False
		else:
			return True

	def process(self):
		output, n_data, e_data = [], [], []
		for line in self.input:
			result = self.filter_lines(line)
			if result:
				output.append(self.normalize(line))
			else:
				if result == None:
					n_data.append(line)
				else:
					e_data.append(line)
		timestamp = self.timestamp()

		pid = Set([x['property_id'] for x in output])
		for i in xrange(len(n_data) - 1, -1, -1):
			if n_data[i]['property_id'] in pid:
				n_data.pop(i)


		self.write(output, os.path.join(self.output,"%s master_output.csv" % timestamp), True)
		self.write(n_data, os.path.join(self.output,"%s no_data.csv" % timestamp))
		self.write(e_data, os.path.join(self.output,"%s data_err.csv" % timestamp))




def main():
	parser = argparse.ArgumentParser(description='Process and conglomerate all RTA data.')

	parser.add_argument('infile', nargs='?', type=str)

	parser.add_argument('-e', '--error', action='store_true', default=False, help='Create an error trend.')

	parser.add_argument('-s', '--split', action='store_true', default=False, help='Split the master output file for upload.')

	args = parser.parse_args()

	conglo = vulture(args)


	if args.error:
		conglo.error()
	elif args.split:
		pass
	else:
		
		conglo.masteri()
		conglo.process()


if __name__ == "__main__":
	main()