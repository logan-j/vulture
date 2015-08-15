from glob import glob
from datetime import date, datetime
import argparse
import csv
import re
import os
from math import floor

unavailable = ['N/A', 'Month rent price is unavailable', '***NO UNITS FOUND***']

class vulture:
	def __init__(args):
		self.input = []
		self.infile = args.infile
		self.yardi = {}
		dirs = re.split("/|\\\\", self.infile)
		if dirs[-1] == '': dirs.pop()
		self.header = re.sub("[a-zA-Z]", "", dirs[-2])[0]
		filename = self.header + "Outputs"
		self.output = "/".join(dirs[:-3]) + "/Output Files/%s/%s" % (filename, dirs[-1])
		if not os.path.exists(self.output):
			os.makedirs(self.output)
		database = [x for x in glob("/".join(dirs[:-3]) if "database" in x.lower()][0]
		with open(databse, 'r') as d_file:
			for line in csv.DictReader(d_file, delimiter='\t'):
				if not self.yardi.has_key(line['property_id']):
					self.yardi[line['property_id']] = {}
				self.yardi[line['property_id']][line['unit_name']] = line['floorplan_name']

				

		
	def masteri():
		infile = self.infile
		yardi_err = []
		if infile.endswith("/"): 
			infile += "*"
		else: 
			infile += "/*"


		for f in glob(infile):
			with open(f, 'r') as i_file:
				dialect = csv.Sniffer().sniff(i_file.read(1024), delimiters="\t,")
				i_file.seek(0)
				for line in csv.DictReader(i_file, dialect):
					result = self.yardi.get(line['property_id'])
					if result:
						if result.get(line['unit_name']):
							line['floorplan_name'] = result[line['unit_name']]
							self.input.append(dict(line)) 
						else:
							yardi_err.append(dict(line))
					else:
						self.input.append(dict(line))
		timestamp = self.timestamp()
		for i in xrange(0, len(self.input)):
			for key, val in dict.iteritems(self.input[i]): self.input[i][key] = val.strip()
		self.write(self.input, self.output + "/%s master_input.csv" % timestamp)
		self.write(yardi_err, self.output + "/%s yardi_err.csv" % timestamp)



	def timestamp():
		now = datetime.now()
		return "%d.%d.%d %d_%d" % (now.year, now.month, now.day, now.hour, now.minute)

	
	def write(lines, outfile, master=False):
		if not master:
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
						line.get('date_available', line.get('available', ''))))
		else:
			lines = sorted(lines, key=lambda x: x['property_id'])


	def normalize(line):
		n_line = dict(line)
		n_line['price'] = int(round(float(re.sub('[^\d.-]', '', n_line['price']))))
		n_line['bed'] = int(float(n_line['bed']))
		n_line['bath'] = floor(float(n_line['bath']) * 2.0)/2.0

		try:
			date = datetime.strptime(line.get('date_available', line.get('available', '')),'%m/%d/%Y')
			if n_line.has_key('date_available'):
				n_line['available'] = "%s-%s-%s" % (date.year, date.month, date.day)
			else:
				n_line['available'] = "%s-%s-%s" % (date.year, date.month, date.day)
		except:
			pass
		try:
			date = datetime.strptime(line.get('date_available', line.get('available', '')), '%Y/%m/%d')
			if n_line.has_key('date_available'):
				n_line['available'] = "%s-%s-%s" % (date.year, date.month, date.day)
			else:
				n_line['available'] = "%s-%s-%s" % (date.year, date.month, date.day)
		except:
			pass
		n_line['floorplan_name'] += " - %s/%s" % (n_line['bed'], n_line['bath'])
		n_line['unit_name'] += " - %s" % n_line['floorplan_name']


		return n_line

	def filter_lines(line):
		if line.get('floorplan_name') in unavailable:
			return None

		try:
			int(line['property_id'])
			if line['unit_name'] == None or line['unit_name'] == '': return False
			if line['floorplan_name'] == None or line['floorplan_name'] == '': return False
			if float(re.sub('[^\d.-]', '', line['price'])) < 350: return False
			sqft = line['sqft']
			if not sqft == '':
				if int(sqft) == 0: return False
			float(line['bed'])
			float(line['bath'])
			errors = 0
			try:
				datetime.strptime(line.get('date_available', line.get('available', '')),'%m/%d/%Y')
			except:
				errors += 1

			try:
				datetime.strptime(line.get('date_available', line.get('available', '')), '%Y/%m/%d')
			except:
				errors += 1
			if errors == 2: return False

		except:
			return False
		else:
			return True

	def process():
		output, n_data, e_data = [], [], []
		for line in self.input:
			result = filter_lines(line)
			if result:
				output.append(normalize(line))
			else:
				if result == None:
					n_data.append(line)
				else:
					e_data.append(line)
		timestamp = self.timestamp()
		self.write(output, self.output + "%s master_output.csv" % timestamp)
		self.write(n_data, self.output + "%s no_data.csv" % timestamp)
		self.write(e_data, self.output + "%s data_err.csv" % timestamp)




def main():
	parser = argparse.ArgumentParser(description='Process and conglomerate all RTA data.')

	parser.add_argument('infile', nargs='?', type=str)

	parser.add_argument('-e', '--error', action='store_true', default=False, help='Create an error trend.')

	parser.add_argument('-s', '--split', action='store_true', default=False, help='Split the master output file for upload.')

	args = parser.parse_args()

	if args.error:
		pass
	elif args.split:
		pass
	else:
		conglo = vulture(args)
		conglo.masteri()
		conglo.process()


if __name__ == "__main__":
	main()