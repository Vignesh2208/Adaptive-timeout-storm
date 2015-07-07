#!/usr/bin/python

import sys
from numpy import loadtxt, exp, ceil
from scipy.stats import johnsonsu
import time
import os

if(len(sys.argv) == 5):

	inputFile =sys.argv[1]
	outputFile = sys.argv[2]
	start_time = time.time()
	data = loadtxt(inputFile, comments='#')
	#max_rate = sys.argv[3]
	#min_rate = sys.argv[4]
	topology_info = sys.argv[3]
	base_dir = sys.argv[4]
	print data,len(data)
	if len(data) > 1 :
		max_val = max(data)
		print "max_val = ", max_val
		gamma, delta, xi, lambda1 = johnsonsu.fit(data)
	#normal inverse of
	#0.95 : 1.6449
	#0.975: 1.9600
	#0.99 : 2.3263
	#0.995: 2.5758
	#0.999: 3.0902
		timeout = (2.3263 - gamma)/delta
		timeout = lambda1*(exp(timeout) - exp(-timeout))/2 + xi
		timeout = int(ceil(timeout/1000)) #converting from milliseconds to seconds, rounding to nearest int
		if timeout > 100 :
			timeout = 100
		#f = open("/app/home/storm/" + "max_rate_" + max_rate + "_min_rate_" + min_rate + "/" + "spout_timeout" + ".append", 'a') #a: append w:overwrite
		f = open(base_dir + topology_info + "/" + "spout_timeout" + ".append", 'a') 
		print >> f, timeout, gamma, delta, xi, lambda1
		f.close()
		f = open(outputFile, 'w') #a: append w:overwrite
		print >> f , timeout #, gamma, delta, xi, lambda1
		f.close()
	end_time =time.time()
	os.remove(inputFile)
	print "time elapsed = ", end_time - start_time
	print "timeout = ",timeout
	#open(inputFile, 'w').close()
