# This code is heavily based on a blog post by Andre Dietrich, currently
# found at http://www.aizac.info/simple-check-of-a-sample-against-80-distributions/

import os
import scipy.stats
import warnings
from math import log
from os import listdir
import numpy as np
import sys
from scipy.stats import genextreme
import matplotlib.pyplot as plt

# just for surpressing warnings
warnings.simplefilter('ignore')

  
# list of all available distributions
cdfs = [
    "norm",            #Normal (Gaussian)

    "anglit",          #Anglit

    "chi",             #Chi

    "cosine",          #Cosine
    "dgamma",          #Double Gamma
    "dweibull",        #Double Weibull

    "expon",           #Exponential
    "exponweib",       #Exponentiated Weibull
    "exponpow",        #Exponential Power

    "foldnorm",        #Folded Normal
    "frechet_r",       #Frechet Right Sided, Extreme Value Type II
    "frechet_l",       #Frechet Left Sided, Weibull_max
    "gamma",           #Gamma

    "genexpon",        #Generalized Exponential
    "genextreme",      #Generalized Extreme Value
    "gengamma",        #Generalized gamma
    "genlogistic",     #Generalized Logistic


    "gompertz",        #Gompertz (Truncated Gumbel)
    "gumbel_l",        #Left Sided Gumbel, etc.
    "gumbel_r",        #Right Sided Gumbel

    "halflogistic",    #Half Logistic
    "halfnorm",        #Half Normal

    "johnsonsb",       #Johnson SB
    "johnsonsu",       #Johnson SU
    "laplace",         #Laplace
    "logistic",        #Logistic
    "loggamma",        #Log-Gamma

    "lognorm",         #Log-Normal

    "maxwell",         #Maxwell

    "nct",             #Non-central Student's T

    "powerlaw",        #Power-function

    "powernorm",       #Power normal

    "rayleigh",        #Rayleigh
    "rice",            #Rice

    "t",               #Student's T

    "truncexpon",      #Truncated Exponential

    "wald",            #Wald
    "weibull_min",     #Minimum Weibull (see Frechet)
    "weibull_max",     #Maximum Weibull (see Frechet)


#    "genpareto",       #Generalized Pareto
#    "genhalflogistic", #Generalized Half Logistic
#    "fatiguelife",     #Fatigue Life (Birnbaum-Sanders)
#    "gausshyper",      #Gauss Hypergeometric
#    "foldcauchy",      #Folded Cauchy
#    "f",               #F (Snecdor F)
#    "fisk",            #Fisk
#    "erlang",          #Erlang
#    "chi2",            #Chi-squared
#    "alpha",           #Alpha
#    "gilbrat",         #Gilbrat
#    "halfcauchy",      #Half Cauchy
#    "arcsine",         #Arcsine
#    "beta",            #Beta
#    "betaprime",       #Beta Prime
#    "bradford",        #Bradford
#    "burr",            #Burr
#    "cauchy",          #Cauchy
#    "hypsecant",       #Hyperbolic Secant
#    "invgamma",        #Inverse Gamma
#    "invnorm",         #Inverse Normal
#    "invweibull",      #Inverse Weibull
#    "lomax",           #Lomax (Pareto of the second kind)
#    "loglaplace",      #Log-Laplace (Log Double Exponential)
#    "mielke",          #Mielke's Beta-Kappa
#    "nakagami",        #Nakagami
#    "ncx2",            #Non-central chi-squared
#    "ncf",             #Non-central F
#    "pareto",          #Pareto
#    "powerlognorm",    #Power log normal
#    "rdist",           #R distribution
#    "reciprocal",      #Reciprocal
#    "recipinvgauss",   #Reciprocal Inverse Gaussian
#    "semicircular",    #Semicircular
#    "triang",          #Triangular
#    "truncnorm",       #Truncated Normal
#    "tukeylambda",     #Tukey-Lambda
#    "uniform",         #Uniform
#    "vonmises",        #Von-Mises (Circular)
#    "wrapcauchy",      #Wrapped Cauchy
    #"ksone",           #Kolmogorov-Smirnov one-sided (no stats)
    #"kstwobign"	#Kolmogorov-Smirnov two-sided test for Large N
]       
    



slow_ones = ["beta","chi","exponpow","guasshyper","johnsonsb","johnsonsu","nct"]


def process_file(parent_dir,filename,cdfs) :

	dictionary = {}
	for cdf in cdfs :
		dictionary[cdf] = {}
		dictionary[cdf][0] = -1
		dictionary[cdf][1] = 0
		dictionary[cdf][2] = -1

	print "processing %s" % filename
	data = np.loadtxt(parent_dir+ "/" + filename)
	sum_data = sum(data)
	data = [x for x in data]
	print "Length of orig data = ", len(data)
	#mean_data = sum(data)/float(len(data))
	#print mean_data
        #new_data = [x for x in data if x > mean_data]
	data = np.array(data)
        print data[1], " : ", data[2], " length = " , len(data)
	
	#parameters = eval("scipy.stats."+"genextreme"+".fit(data)");
	#D, p = scipy.stats.kstest(data, "t", args=(3.3674,25,8333.45));
	#print "D = ", D, " p = ", p
        #r = genextreme.rvs(parameters[0],loc = parameters[1],scale = parameters[2],size = 182111);
	#plt.hist(r,100)
	#plt.show()
	#A2,critical,sig = scipy.stats.anderson(data,dist='gumbel')
	#A2,critical,sig = scipy.stats.anderson_ksamp([data,r])
	#print "A2 = " , A2
	#print "critical = ", critical
	#print "Significance = ", sig
	

	new_cdfs=[]
	for cdf in cdfs:
		#fit our data set against every probability distribution
		print "curr cdf = ", cdf
		parameters = eval("scipy.stats."+cdf+".fit(data)");
                i = 0
		while i < len(parameters) :
			if np.isnan(parameters[i]) :
				break
			else :
				i = i + 1
		#Applying the Kolmogorov-Smirnof one sided test
		if i == len(parameters) :		
			D, p = scipy.stats.kstest(data, cdf, args=parameters);
                        print "D = ", D, " p = ", p
			# Discard all distributions that are very poor fits
			if np.isnan(p):
				pass
			else:
				#print parameters
				i = 0
				while i < len(parameters) :
					if np.isnan(parameters[i]) :
						break
					else :
						i = i + 1
	    
				if i == len(parameters) :
					negative_log_likelihood = eval("scipy.stats."+cdf+".nnlf("+str(parameters)+",data)")

						
					new_cdfs.append(cdf)
					dictionary[cdf][0] = D
					dictionary[cdf][1]+=negative_log_likelihood
					dictionary[cdf][2]= parameters
	cdfs=new_cdfs[:]
	return dictionary


parent_directory = os.listdir(sys.argv[1])
file_result_dict = {}
n_files = 0

for filename in parent_directory :
	dict = 	process_file(sys.argv[1],filename,cdfs)	
	file_result_dict[filename] = dict

	#print "\n\nBest fits for selected data set. Contains name,\nsum of negative log p-values for KS test (lower values indicate better fit),\nsum of negative log likelihoods (lower is better),\nand number of parameters (degrees of freedom) \n"
	#print "name".ljust(12) + "D value KS-test".ljust(21) + "neg log likelihood".ljust(21)+"# parameters"

	head, tail = os.path.split(filename)
	with open("/app/home/Fitting_results/Fitting_Results_"+tail,'w') as f :
		print "name".ljust(12) + "D value KS-test".ljust(21) + "neg log likelihood".ljust(21)+"# parameters"
		f.write("name".ljust(12) + "D value KS-test".ljust(21) + "neg log likelihood".ljust(21)+"# parameters")
		f.write("\n")
		for cdf in cdfs:
		    	print cdf.ljust(14) + str(dict[cdf][0]).ljust(20) + str(dict[cdf][1]).ljust(24), str(dict[cdf][2])
			f.write(cdf.ljust(14) + str(dict[cdf][0]).ljust(20) + str(dict[cdf][1]).ljust(24) + str(dict[cdf][2]))
			f.write("\n")
		f.close()
	n_files = n_files + 1


with open("/app/home/Fitting_results/Avg_Results.txt",'w') as f :
	for cdf in cdfs :
		avg_D_value = 0.0
		
		for filename in parent_directory :
	
			file_dict = file_result_dict[filename]
			avg_D_value = avg_D_value + file_dict[cdf][0]

		avg_D_value = avg_D_value/n_files			
		f.write(cdf.ljust(14) + str(avg_D_value))
		f.write("\n")

f.close()
				    

