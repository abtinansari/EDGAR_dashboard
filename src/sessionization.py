###############################################################################################
# import the rquired dependencie:
import csv
from operator import itemgetter
from datetime import datetime
###############################################################################################
# obtain the inactivity_period:
lines = [line.rstrip('\n') for line in open('./input/inactivity_period.txt')]
inactivity_period=int(lines[0])
###############################################################################################
# read the first line of 'log.csv' file to initialize the queue:
# 'with' method is used to read only one line at a time for the scalability purposes 
with open('./input/log.csv', "r") as f:
  reader = csv.reader(f)
  next(reader)  # skip the header
  first_line = next(reader)  # gets the first line

# initilalize the list to store desired fields:
# targets=[ ip, time of 1st request,  time of last request, session duration, # of requests, inactivity]

targets=[]
fmt='%Y-%m-%d %H:%M:%S' # datetime format in 'log.csv'

current_time=first_line[1]+' '+first_line[2]
current_time=datetime.strptime(current_time, fmt)
ip=first_line[0]

targets.append(ip)
targets.append(current_time)
targets.append(current_time)
targets.append(0)
targets.append(1)
targets.append(0)
# 'list' is a dynamic array containing all the active sessions
list = [[]]
list.append(targets)
list.pop(0)


previous_time=current_time
###############################################################################################
# read and process 'log.csv' and write the results in 'sessionization.txt' line by line:
with open('./input/log.csv', "r") as f, open('./output/sessionization.txt', "w") as output:
    reader = csv.reader(f)
    writer = csv.writer( output )

    next(reader) # skip the header
    next(reader) # skip the 1st line ( previously processed)
# iterate over rows of 'log.csv':    
    for row in reader:

        targets=[]
        current_time=row[1]+' '+row[2]
        current_time=datetime.strptime(current_time, fmt)
        ip=row[0]
        s=[item[0] for item in list] # list of active sessions' ips 
        n=len(list)-1

        if ip not in s: # add the previously non-active ip to the list of active sessions
            targets.append(ip)
            targets.append(current_time)
            targets.append(current_time)
            targets.append(0)
            targets.append(1)
            targets.append(0)

            list.append(targets)
        else: # if the encountered request has an active session, referesh the session by bringing the session to the head of the list 
            i=s.index(ip)        
            list.append(list.pop(i))
            list[n][2]=current_time # update the last request
            list[n][4]+=1 # update the number of requests 
            list[n][5]=0

        if current_time > previous_time:
        
            previous_time=current_time
            for m in range(0,n+1):
                list[m][5]+=1 # update the inactivity for the sessions not encountered in the request
            # determine if any old sessions has ended:    
            if list[0][5] >  inactivity_period:
                count=1
                while (list[count][5] > inactivity_period) & (count<=n):
                    count+=1
                # extract the desired field of the ended sessions to be writen in the output:    
                for j in range(0,count):
                    list[j][3]=list[j][2]-list[j][1] # duration
                    list[j][3]=(list[j][3]).total_seconds()+1 # convertion to seconds
                    list[j][3]=int(list[j][3]) 
                    start=list[j][1].strftime(fmt) 
                    end=list[j][2].strftime(fmt) 
                    # format the line to be writen in the output:
                    line=list[j][0]+','+start+','+end+','+str(list[j][3])+','+str(list[j][4])
                    output.write( line )
                    output.write('\n')
                    list.pop(j) # remove the ended session from the active list   

# after reaching the endline of 'log.csv' sort the active list by the start time of the sessions:
    list=sorted(list, key=itemgetter(1))
    # terminate all the remaining sessions and write them into output: 
    for j in range(0,len(list)):
        list[j][3]=list[j][2]-list[j][1]
        list[j][3]=(list[j][3]).total_seconds()+1
        list[j][3]=int(list[j][3])
        start=list[j][1].strftime(fmt) 
        end=list[j][2].strftime(fmt) 
        line=list[j][0]+','+start+','+end+','+str(list[j][3])+','+str(list[j][4])
        output.write( line )               
        output.write('\n')



