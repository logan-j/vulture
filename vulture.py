from glob import glob
from datetime import date, datetime
import argparse
import csv
import re
import os
import sys
import psycopg2
import psycopg2.extras
from math import floor
from sets import Set

unavailable = [	'N/A', '12 MONTH RENT PRICE IS UNAVAILABLE', '***NO UNITS FOUND***', 
				'***NO CURRENT AVAILABILITY***', "MONTH RENT PRICE IS UNAVAILABLE",
				"***UNKNOWN ERROR***"]

class vulture:
	def __init__(self, args):
		self.input = []
		self.infile = os.path.normpath(args.infile)
		self.yardi = {}
		self.o_yardi = []
		self.database = {}
		if os.path.split(self.infile)[1] == '': self.infile = os.path.split(self.infile)[0]
		if args.error:
			self.err = self.infile
			self.infile = os.path.split(self.infile)[0]

		m1 = os.path.split(self.infile)[1]
		m2 = os.path.split(os.path.split(self.infile)[0])[1]
		m3 = os.path.split(os.path.split(os.path.split(self.infile)[0])[0])[0]
		self.header = re.sub("[a-zA-Z]", "", m2)
		filename = self.header + " Outputs"
		self.output = os.path.join(m3,  "Output Files/%s/%s" % (filename, m1))
		if not os.path.exists(self.output):
			os.makedirs(self.output)

		blank = False

		if not args.error:
			yardi = os.path.normpath(args.yardi[0])
			if args.yardi[0] == '':
				try:
					yardi = [x for x in glob(m3 + "/*.*") if "yardi" in x.lower()][0]
				except:
					self.yardi = {}
					blank = True

			if not blank:
				with open(yardi, 'r') as y_file:
					for line in csv.DictReader(y_file, delimiter='\t'):
						if not self.yardi.has_key(line['property_id']):
							self.yardi[line['property_id']] = {}
						self.yardi[line['property_id']][line['unit_name']] = line['floorplan_name']

		else:

			database = os.path.normpath(args.database[0])
			if args.database[0] == '':
				args.file = True
				try:
					database = [x for x in glob(m3 + "/*.*") if "database" in x.lower()][0]
				except:
					blank = True
					self.database = {}
			if not blank:
				if args.file and not args.cached:
					self.database = self.access_database(database)
					
				else:
					self.database = self.access_database(database, True, args.cached)

				
	def average(self, building):
		output = list(building)
		floorplans = {}
		bedbath = {}
		for unit in building:
			if unit['floorplan_name'] == 'Refresh':
				continue
			#keys = [x.strip() for x in re.split("-", unit['floorplan_name'])]
			keys = [unit['floorplan_name'], "%s%s" % (unit['bed'], unit['bath'])]
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
				unit['fp_ave'] = unit['bb_ave'] = unit['db_ave'] = unit['pp_sqft'] = unit['fp_ave_actual'] = unit['bb_ave_actual'] = unit['db_ave_actual'] = 'N/A'
				continue
			keys = [unit['floorplan_name'], "%s%s" % (unit['bed'], unit['bath'])]
			unit['fp_ave'] = "%0.3f" % ((float(unit['price']) * floorplans[keys[0]][1]) / floorplans[keys[0]][0] - 1.0)
			unit['fp_ave_actual'] = "%0.3f" % (floorplans[keys[0]][0]/ floorplans[keys[0]][1])
			unit['bb_ave'] = "%0.3f" % ((float(unit['price']) * bedbath[keys[1]][1]) / bedbath[keys[1]][0] - 1.0)
			unit['bb_ave_actual'] = "%0.3f" % (bedbath[keys[1]][0]/ bedbath[keys[1]][1])
			unit['pp_sqft'] = 'N/A'
			if unit['sqft'] != '' and unit['sqft'] != None and unit['sqft'] != '-' and str(unit['sqft']) != '0':
				unit['pp_sqft'] = "%0.3f" % (float(unit['price']) / float(unit['sqft']))
			db = self.database.get(unit['property_id'], {})
			if db.get(re.sub('\s', '', unit['unit_name'])):
				s_name = re.sub('\s', '', unit['unit_name'])
				unit['db_ave'] = "%0.3f" % ((float(unit['price']) * self.database[unit['property_id']][s_name][1]) / self.database[unit['property_id']][s_name][0] - 1.0)
				unit['db_ave_actual'] = "%0.3f" %  self.database[unit['property_id']][s_name][0] / self.database[unit['property_id']][s_name][1]


		return output

	def error(self):
		err, building = [], []
		outpath = os.path.split(self.err)[0]
		with open(self.err, 'r') as r_file:
			input = csv.DictReader(r_file, delimiter=",")
	
			propID = ''
			for line in sorted(input, key=lambda x: int(x['property_id'])):
				if propID != line['property_id']:
					propID = line['property_id']
					err += self.average(building)
					building = []
				building.append(line)
			err += self.average(building)

		timestamp = self.timestamp()
		self.write(err, os.path.join(outpath, "%s Error_Trend.csv" % timestamp), None)


	def access_database(self, dbase, dcred=False, cached=False):
		if dbase == '':
			return {}
		elif not dcred:
			sys.stderr.write("Reading in database file...\n")
			d_out = {}
			with open(dbase, 'r') as r_file:
				reader = csv.DictReader(r_file)
				error = False
				for line in reader:
					try:
						if not d_out.has_key(line['property_id']):
							d_out[line['property_id']] = {}
						if not d_out[line['property_id']].has_key(re.sub('\s', '', line['unit_name'])):
							d_out[line['property_id']][re.sub('\s', '', line['unit_name'])] = [0,0]
						d_out[line['property_id']][re.sub('\s', '', line['unit_name'])][0] += float(line['price'])
						d_out[line['property_id']][re.sub('\s', '', line['unit_name'])][1] += 1.0
					except:
						error = True
				if error:
					sys.stderr.write("Some malformed lines in database input.\n")
			return d_out
		else:
			sys.stderr.write("Establishing database connection...\n")

			if cached:
				dbase = "postgres://uc1kkn0rr14fbo:pfr07r9ro6ck8gdp78amss9nl6e@ec2-54-235-154-118.compute-1.amazonaws.com:5552/d753p6ts62if6p"
			d_out = {}
			try:
				conn = psycopg2.connect(dbase)
				cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
				cur.execute('''SELECT * FROM "RTA Curation Upload History"''')
				lines = cur.fetchall()
				conn.close()
				for line in lines:
					
					if not d_out.has_key(line['property_id']):
						d_out[line['property_id']] = {}
					if not d_out[line['property_id']].has_key(re.sub('\s', '', line['unit_name'])):
						d_out[line['property_id']][re.sub('\s', '', line['unit_name'])] = [0,0]
					d_out[line['property_id']][re.sub('\s', '', line['unit_name'])][0] += float(line['price'])
					d_out[line['property_id']][re.sub('\s', '', line['unit_name'])][1] += 1.0
				return d_out
			except Exception as inst:
				sys.stderr.write("Database Connection Failed. %s\n" % inst)
				return {}

			
						


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
						if result.get(un) or un == '':
							line['floorplan_name'] = result[line['unit_name']]
							self.o_yardi.append(dict(line)) 
						elif un not in unavailable and un != None and un != '':
							yardi_err.append(dict(line))
						else:
							fp = line['floorplan_name']
							if fp != None and fp != '':
								line['floorplan_name'] = "%s" % fp.strip()
							self.o_yardi.append(dict(line))
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
		fieldnames = ['property_id', 'floorplan_name', 'unit_name', 'sqft', 'bed', 'bath', 'price', 'date_available']

		if master == False:
			with open(outfile, 'wb') as w_file:
				
				writer = csv.DictWriter(w_file, fieldnames=fieldnames, quotechar='\"', extrasaction='ignore')
				writer.writeheader()
				writer.writerows(lines)
				
		elif master == True:
			fieldnames = ['property_id','floorplan_name','unit_name','sqft','bed','bath','price','date_available']
			lines = sorted(lines, key=lambda x: int(x['property_id']))
			propID = ''
			now = datetime.today()
			with open(outfile, 'wb') as w_file:
				writer = csv.DictWriter(w_file, delimiter=',', fieldnames=fieldnames, extrasaction='ignore', quotechar='\"')
				writer.writeheader()
				for line in lines:
					if line.has_key('date_available'):
						pass
					elif line.has_key('available_date'):
						line['date_available'] = line['available_date']
					else:
						line['date_available'] = ''
					if propID != line.get('property_id'):
						propID = line.get('property_id')
						writer.writerow({	"property_id": propID,
											"floorplan_name": 'Refresh',
											'unit_name': 'Refresh',
											'sqft': '',
											'bed': 1,
											'bath': 1,
											'price': "9%s%s" % (str(now.month).zfill(2), str(now.day).zfill(2)),
											'date_available': 'unavailable'
										})
					writer.writerow(line)

		else:
			with open(outfile, 'wb') as w_file:
				fieldnames = [	'property_id', 'floorplan_name', 'unit_name', 'sqft', 'bed', 'bath', 'price', 'date_available',
								'bb_ave', 'db_ave', 'fp_ave', 'pp_sqft', 'bb_ave_actual', 'db_ave_actual', 'fp_ave_actual']

				writer = csv.DictWriter(w_file, fieldnames=fieldnames, quotechar='\"')
				writer.writeheader()
				writer.writerows(sorted(lines, key=lambda x: int(x['property_id'])))

	def normalize(self, line, yardi=False):
		n_line = dict(line)
		n_line['price'] = int(round(float(re.sub('[^\d.-]', '', n_line['price']))))
		n_line['bed'] = int(float(n_line['bed']))
		n_line['bath'] = floor(float(n_line['bath']) * 2.0)/2.0
		if int(n_line['bath']) == n_line['bath']: n_line['bath'] = int(n_line['bath'])
		if n_line['sqft'] == '-' or n_line['sqft'] == '--': n_line['sqft'] = ''

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
		if not yardi:
			n_line['floorplan_name'] = n_line['floorplan_name'].strip("\"")
			n_line['floorplan_name'] += " - %s/%s" % (n_line['bed'], n_line['bath'])
			n_line['unit_name'] = n_line['unit_name'].strip("\"")
			n_line['unit_name'] += " - (%s)" % n_line['floorplan_name']
		n_line['floorplan_name'] = "\"%s\"" % n_line['floorplan_name']
		n_line['unit_name'] = "\"%s\"" % n_line['unit_name']


		return n_line


	def filter_lines(self, line):
		if line.get('floorplan_name').upper().strip().strip('\"') in unavailable:
			return None

		try:
			int(line['property_id'])
			if line['unit_name'] == None or line['unit_name'] == '': return False
			if line['floorplan_name'].strip("\"") == None or line['floorplan_name'].strip("\"") == '': return False
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
		output, n_data, e_data, yardi = [], [], [], []
		for line in self.input:
			result = self.filter_lines(line)
			if result:
				output.append(self.normalize(line))
			else:
				if result == None:
					n_data.append(line)
				else:
					e_data.append(line)

		for line in self.o_yardi:
			result = self.filter_lines(line)
			if result:
				output.append(self.normalize(line, True))

		timestamp = self.timestamp()


		pid = Set([x['property_id'] for x in output])
		for i in xrange(len(n_data) - 1, -1, -1):
			if n_data[i]['property_id'] in pid:
				del n_data[i]


		fps = Set()
		for i in xrange(len(output) - 1, -1, -1):
			pid = output[i]['property_id']
			un = output[i]['unit_name']
			tester = pid + un

			if tester in fps:
				del output[i]
			else:
				fps.add(tester)

		self.write(yardi, os.path.join(self.output, "%s master_yardi_output.csv" % timestamp))
		self.write(output, os.path.join(self.output,"%s master_output.csv" % timestamp), True)
		self.write(n_data, os.path.join(self.output,"%s no_data.csv" % timestamp))
		self.write(e_data, os.path.join(self.output,"%s data_err.csv" % timestamp))




def main():
	parser = argparse.ArgumentParser(description='Process and conglomerate all RTA data.')

	parser.add_argument('infile', nargs='?', type=str)

	parser.add_argument('-e', '--error', action='store_true', default=False, help='Create an error trend.')

	parser.add_argument('-s', '--split', action='store_true', default=False, help='Split the master output file for upload.')

	parser.add_argument('-d', '--database', nargs=1, type=str, default=[''], help='Manually specify the database file or credentials.')

	parser.add_argument('-y', '--yardi', nargs=1, type=str, default=[''], help='Manually specify the yardi file.')

	parser.add_argument('-f', '--file', action='store_true', default=False, help='Specify file-mode for the database argument. Credential-mode otherwise.')

	parser.add_argument('-c', '--cached', action='store_true', default=False, help='Use the stored database credentials.')

	args = parser.parse_args()

	conglo = vulture(args)


	if args.error:
		conglo.error()
	elif args.split:
		pass #part 5
	else:
		
		conglo.masteri()
		conglo.process()


if __name__ == "__main__":
	main()