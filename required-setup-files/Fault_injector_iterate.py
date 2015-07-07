import subprocess
import time

init_rate = 10
step_size = 10


# With Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.WordCountTopology","Wordcount",str(curr_rate),str(1),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Word count Topology with adaptive timeout : failures"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","Wordcount"])
	print "Killed topology"
        time.sleep(60)

# With Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.RollingTopTwitterWords","RollingTop",str(curr_rate),str(1),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Rolling Top Words Topology with adaptive timeout : failures"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","RollingTop"])
	print "Killed topology"
        time.sleep(60)

# With Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.JoinTwitterWordCount","SingleJoin",str(curr_rate),str(1),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Single Join Topology with adaptive timeout : failures"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","SingleJoin"])
	print "Killed topology"
        time.sleep(60)




# With Adaptive timeout - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :
        
	
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.WordCountTopology","Wordcount",str(curr_rate[i]),str(1),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Word count Topology with adaptive timeout : rates"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","Wordcount"])
	print "Killed topology"
        time.sleep(60)

# With Adaptive timeout - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :
        
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.RollingTopTwitterWords","RollingTop",str(curr_rate[i]),str(1),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Rolling Top Words Topology with adaptive timeout : rates"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","RollingTop"])
	print "Killed topology"
        time.sleep(60)

# With Adaptive timeout  - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :

        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.JoinTwitterWordCount","SingleJoin",str(curr_rate[i]),str(1),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Single Join Topology with adaptive timeout : rates"
        print "Submitted topology with adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","SingleJoin"])
	print "Killed topology"
        time.sleep(60)







# EVERYTHING FROM BELOW THIS MUST BE RUN ONLY ONCE

# Without Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.WordCountTopology","Wordcount",str(curr_rate),str(0),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Word count Topology without adaptive timeout : failures"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","Wordcount"])
	print "Killed topology"
        time.sleep(60)

# Without Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.RollingTopTwitterWords","RollingTop",str(curr_rate),str(0),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Rolling Top Words Topology without adaptive timeout : failures"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","RollingTop"])
	print "Killed topology"
        time.sleep(60)

# Without Adaptive timeout
i = 0
sample_size = [10,100,1000,10000,100000,0]
while i < len(sample_size) :
        #curr_rate = init_rate + i*step_size
	curr_rate = init_rate
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.JoinTwitterWordCount","SingleJoin",str(curr_rate),str(0),str(sample_size[i])]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Single Join Topology without adaptive timeout : failures"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","SingleJoin"])
	print "Killed topology"
        time.sleep(60)



# Without Adaptive timeout - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :
        
	
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.WordCountTopology","Wordcount",str(curr_rate[i]),str(0),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Word count Topology without adaptive timeout : rates"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","Wordcount"])
	print "Killed topology"
        time.sleep(60)

# Without Adaptive timeout - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :
        
        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.RollingTopTwitterWords","RollingTop",str(curr_rate[i]),str(0),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Rolling Top Words Topology without adaptive timeout : rates"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","RollingTop"])
	print "Killed topology"
        time.sleep(60)

# Without Adaptive timeout  - Diff rates
i = 0
curr_rate = [20,30,40,50]
while i < len(curr_rate) :

        print "Submitting new topology..."

	
	subprocess.call(["/home/vignesh/storm","jar","/home/vignesh/storm-starter-0.9.2-incubating-jar-with-dependencies.jar","storm.starter.JoinTwitterWordCount","SingleJoin",str(curr_rate[i]),str(0),str(0)]) # The 3rd argument str(1) enables adaptive timeout. The fourth argument str(sample_size[i]) sets the message drop probability to 1/sample_size[i]. If sample_size[i] == 0, no messages are dropped. Fault injector is disabled.
        i = i + 1
        print "Iteration number ", i, "Single Join Topology without adaptive timeout : rates"
        print "Submitted topology without adaptive timeout. waiting for 5 mins"
        time.sleep(300)
        subprocess.call(["/home/vignesh/storm","kill","SingleJoin"])
	print "Killed topology"
        time.sleep(60)






