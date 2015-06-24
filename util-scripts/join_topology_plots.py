from matplotlib import pyplot as plt
import os
import scipy
from scipy.interpolate import interp1d
import numpy as np
from numpy import loadtxt, exp, ceil


def return_percentile_value(sorted_data,percentile) :
	return np.percentile(sorted_data,percentile)




n_fails_with = [0,157999,180179,188015,177652]
n_acks_with = [96919,1006,6052,1261,3391]
n_emitted_tuples_with = [96919,81628,90930,103256,129209]

#n_fails_without = [0,0,221112,226181,221051]
#n_acks_without = [104776,164766,7357,6034,4269]
#n_emitted_tuples_without = [104776,164766,36436,36025,33332]

n_fails_without = [0,196794,208204,190375,206336]
n_acks_without = [97051,2530,2443,747,1575]
n_emitted_tuples_without = [97051,24884,31097,37243,44321]

length = len(n_fails_with)

retransmit_ratio_with = []
retransmit_ratio_without = []
ack_ratio_with = []
ack_ratio_without = []
rate = [10,20,30,40,50]
i = 0
while i < length :
	retransmit_ratio_with.append(float(n_fails_with[i])/float(n_emitted_tuples_with[i]))
	retransmit_ratio_without.append(float(n_fails_without[i])/float(n_emitted_tuples_without[i]))
	ack_ratio_with.append(float(n_acks_with[i])/float(n_emitted_tuples_with[i]))
	ack_ratio_without.append(float(n_acks_without[i])/float(n_emitted_tuples_without[i]))
	i = i + 1
f2 = interp1d(rate,retransmit_ratio_with, kind='cubic')
f3 = interp1d(rate,retransmit_ratio_without, kind='cubic')
plt.plot(rate,retransmit_ratio_with,'o',rate,f2(rate),'-',rate,retransmit_ratio_without,'o',rate,f3(rate),'-')
plt.xlabel("rate")
plt.ylabel("Retransmit ratio")
plt.legend(['data-with-adaptive-timeout', 'linear-fit-with-adaptive-timeout','data-without-adaptive-timeout', 'linear-fit-without-adaptive-timeout'], loc='best')
plt.show()






base_file_dir = "/home/vignesh/Desktop/END_to_END/Join_twitter_topology"
with_95_percentile_values = []
without_95_percentile_values = []

sample_size = [10,100,1000,10000,100000,0]
i = 0
curr_rate = 10

keyword = "Germanwings"
task_id = "24"
while i < len(sample_size) :


	base_without_file_name = "rate_" + str(curr_rate) + "_" + keyword + "_adaptive_timeout_disabled_sample_size_" + str(sample_size[i])

	base_with_file_name = "rate_" + str(curr_rate) + "_" + keyword + "_adaptive_timeout_enabled_sample_size_" + str(sample_size[i])


	curr_without_file_name = base_file_dir + "/" + "rate_" + str(curr_rate) + "_" + keyword + "_adaptive_timeout_disabled_sample_size_" + str(sample_size[i]) + "/" + task_id + "-spout1_total_latency_" + base_without_file_name + ".txt"

	curr_with_file_name = base_file_dir + "/" + "rate_" + str(curr_rate) + "_" + keyword + "_adaptive_timeout_enabled_sample_size_" + str(sample_size[i]) + "/" + task_id + "-spout1_total_latency_" + base_with_file_name + ".txt"

	
	with_data = loadtxt(curr_with_file_name, comments='#')
	with_data = np.sort(with_data)
	#with_95_percentile_values.append(return_percentile_value(with_data,99))	
	with_95_percentile_values.append(max(with_data))	

	without_data = loadtxt(curr_without_file_name, comments='#')
	without_data = np.sort(without_data)
	#without_95_percentile_values.append(return_percentile_value(without_data,99))
	without_95_percentile_values.append(max(without_data))
	
	i += 1

print "With 95 values = ", with_95_percentile_values
print "Without 95 values = ", without_95_percentile_values
sample_size = [1, 2, 3, 4, 5, 6]
f3 = interp1d(sample_size,with_95_percentile_values, kind='cubic')
f4 = interp1d(sample_size,without_95_percentile_values, kind='cubic')
plt.plot(sample_size,with_95_percentile_values,'o',sample_size,f3(sample_size),'-',sample_size,without_95_percentile_values,'o',sample_size,f4(sample_size),'-')
plt.xlabel("sample_size")
plt.ylabel("95 timeout percentile values (ms)")
plt.legend(['data-with-adaptive-timeout', 'linear-fit-with-adaptive-timeout','data-without-adaptive-timeout', 'linear-fit-without-adaptive-timeout'], loc='best')
plt.show()

with open("Join_topology_arrays_END_to_END.txt",'w') as f :
	f.write("END_to_END_retransmit_ratio = " + str(retransmit_ratio_with) + "\n")
	f.write("Normal_mode_retransmit_ratio = " + str(retransmit_ratio_without) + "\n")
	f.write("END_to_END_max_delays = " + str(with_95_percentile_values) + "\n")
	f.write("Normal_mode_max_delays = " + str(without_95_percentile_values) + "\n")
	f.write("Sample_size = " + str([10,100,1000,10000,100000,0]) + "\n")
f.close()



