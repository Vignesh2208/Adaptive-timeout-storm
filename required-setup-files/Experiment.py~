import subprocess
import time
import sys


#STORM_BIN_PATH = "/home/storm/storm"
#TOPOLOGY_PATH = "/home/storm/storm-starter-0.9.2-incubating-jar-with-dependencies.jar"

STORM_BIN_PATH = "/home/vignesh/Desktop/storm"
TOPOLOGY_PATH = "/home/vignesh/Desktop/Projects/CS-525-Project/apache-storm-0.9.2-incubating/examples/storm-starter/target/storm-starter-0.9.2-incubating-jar-with-dependencies.jar"

WORD_COUNT_TOPOLOGY_ID = "storm.starter.WordCountTopology"
ROLL_COUNT_TOPOLOGY_ID = "storm.starter.RollingTopTwitterWords"
SINGLE_JOIN_TOPOLOGY_ID = "storm.starter.JoinTwitterWordCount"

RUN_TIME = 600 # Run time of each topology in secs


arg_list = sys.argv

def print_usage() :

	print ""
	print ""

	print "################ Incorrect usage ################"

	print ""
	print ""

	print "python Experiment.py -t <topology_name> -r <initial_rate> -s <step_size> -n <number of iterations> -m <timeout-mode> -p <msg-drop-sample-size> -f <sample-size-multiplicative-factor> -h <help>"

	print ""
	print ""
	
	print "Each option is optional"
	
	print ""
	print ""

	print "Valid Topology names : "
	print "1. all "
	print "2. WordCount (default)"
	print "3. RollingTop"
	print "4. SingleJoin"

	print ""
	print ""

	print "Valid timeout modes : "
	print "1. NORMAL_<Fixed_Timeout_value> (default - NORMAL_30)"
	print "2. MM1 "
	print "3. HT"
	print "4. END_TO_END"

	print ""
	print ""

	print "Initial rate : > 0 (default 10)"
	print "Step size : >= 0 (default 5)"
	print "number of iterations >= 0 (default 5)"

	print ""
	print ""
	print "Sample size : 0 (no msg drops - default)" 
	print "or > 1 (msg drop probability = 1/sample_size)"

	print ""
	print ""

	print "Sample size multiplicative factor : > 0 (1 - default)"
	print "Sample size will be multiplied by this value with iteration"

	print ""	
	print ""


def check_arg_types(topology_name, timeout_mode, init_rate, step_size, N, sample_size, mf) :
	
	if topology_name != "WordCount" and  topology_name != "all" and topology_name != "RollingTop" and topology_name != "Singlejoin" :
		print "Unrecognized topology name ", topology_name
		print_usage()
		sys.exit(-1)

	if "NORMAL_" not in timeout_mode and timeout_mode != "MM1" and timeout_mode != "HT" and timeout_mode != "END_TO_END" :
		print "Unrecognized timeout mode ", timeout_mode
		print_usage()
		sys.exit(-1)

	if init_rate <= 0 :
		print "Init rate must be greater than 0"
		print_usage()
		sys.exit(-1)

	if step_size < 0 :
		print "Step size must be >= 0 "
		print_usage()
		sys.exit(-1)

	if N < 0 :
		print "Number of iterations must be >= 0 "
		print_usage()
		sys.exit(-1)

	if sample_size < 0 :
		print "Sample size must an integer >= 0 "
		print_usage()
		sys.exit(-1)

	if mf <= 0 :
		print "Sample size multiplicative factor must be >= 1"
		print_usage()
		sys.exit(-1)


def submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,TOPOLOGY_ID,topology_name,curr_rate,timeout_mode,sample_size,iteration_no) :

	print "Submitting new topology : ", topology_name
	print "Curr rate = ", curr_rate
	print "sample_size = ", sample_size
	
	subprocess.call([STORM_BIN_PATH,"jar",TOPOLOGY_PATH,TOPOLOGY_ID,topology_name,str(curr_rate) + "_" + str(iteration_no),str(timeout_mode),str(sample_size)])
 	
        print "Submitted topology with timeout mode ", timeout_mode, ". Current Rate : ", curr_rate, ". Waiting for 10 mins"
       	time.sleep(RUN_TIME)
        subprocess.call([STORM_BIN_PATH,"kill",topology_name])
	print "Killed topology"
        time.sleep(60)	



arg_list = sys.argv

#defaults
topology_name = "WordCount"
timeout_mode = "NORMAL_30"
init_rate = 10
step_size = 5
N = 5
sample_size = 0
mf = 1

if len(arg_list) > 1 :
	arg_list = arg_list[1:]
	arg_no = 0

	if len(arg_list) % 2 != 0 :
		print_usage()
		sys.exit(-1)

	try :
		while arg_no < len(arg_list) :
			if arg_list[arg_no] == "-t" :
				topology_name = arg_list[arg_no + 1]

			elif arg_list[arg_no] == "-r" :
				init_rate = int(arg_list[arg_no + 1])

			elif arg_list[arg_no] == "-s" :
				step_size = int(arg_list[arg_no + 1])

			elif arg_list[arg_no] == "-n" :
				N = int(arg_list[arg_no + 1])

			elif arg_list[arg_no] == "-m" :
				timeout_mode = arg_list[arg_no + 1]
				if "NORMAL_" in timeout_mode :
					if len(timeout_mode) < 7 :
						print "NORMAL timeout mode must be of the format NORMAL_<Fixed-Timeout-Value>"
						sys.exit(-1)

					timeout_val = int(timeout_mode[7:])
					if timeout_val <= 0 :
						print "Timeout val for NORMAL mode must be greater than 0 "
						sys.exit(-1)
					
			elif arg_list[arg_no] == "-p" :
				sample_size = int(arg_list[arg_no + 1])

			elif arg_list[arg_no] == "-f" :
				mf = int(arg_list[arg_no + 1])
			elif arg_list[arg_no] == "-h" :
				print_usage()
				sys.exit(-1)
			else :
				print_usage()
				sys.exit(-1)
			arg_no = arg_no + 2
	except Exception :
		print_usage()
		sys.exit(-1)

else :
	# All defaults
	pass




correct_timeout_mode = timeout_mode
check_arg_types(topology_name, correct_timeout_mode, init_rate, step_size, N, sample_size,mf)


curr_rate = init_rate
i = 0
while i < N :
	
	if topology_name == "all" :
		
		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,WORD_COUNT_TOPOLOGY_ID,"WordCount",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1

		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,ROLL_COUNT_TOPOLOGY_ID,"RollingTop",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1

		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,SINGLE_JOIN_TOPOLOGY_ID,"SingleJoin",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1

	elif topology_name == "WordCount" :	
		
		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,WORD_COUNT_TOPOLOGY_ID,"WordCount",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1

	elif topology_name == "RollingTop" :

		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,ROLL_COUNT_TOPOLOGY_ID,"RollingTop",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1

	elif topology_name == "SingleJoin" :
	
		submit_topology(STORM_BIN_PATH,TOPOLOGY_PATH,SINGLE_JOIN_TOPOLOGY_ID,"SingleJoin",curr_rate,correct_timeout_mode,sample_size,i+1)
		print "Iteration number : ", i + 1
	else :

		print "Unrecognized topology name. Exiting.."
		sys.exit(-1)
		print "Iteration number : ", i + 1

	i = i + 1
	curr_rate = curr_rate + step_size
	sample_size = sample_size * mf




