import csv

FILEPATH = "visitor_log.csv"

def get_dict():
	name_dict = {}
	count = 0

	reader = csv.reader(open(FILEPATH))

	for row in reader:
		if row[0] != "NAMELAST":
			name = " ".join(filter(lambda x: x != "", [row[1], row[2], row[0]])).title()
			name_dict[name] = name_dict.get(name, 0) + 1

			count += 1

	return name_dict, count

def visit_ten_times(name_dict):
	log = []
	for name, count in name_dict.items():
		if count >= 10:
			log.append(name)
	return log