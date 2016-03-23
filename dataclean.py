import glob
import pandas as pd
import numpy as np
import collections
pd.options.mode.chained_assignment = None  # default='warn'
#days = ['01-01','01-04','01-05','01-06','01-07','01-08','01-11','01-12','01-13','01-14','01-15','01-18','01-19','01-20','01-21','01-22','01-25','01-26','01-27','01-28','01-29']
#hours = ['16','17','18','19','20','21','22','23']
hours = ['16']
days = ['01-13']
for eachDay in days:
	for eachHour in hours:
		path =r'/home/jayasudha/spring/aml/Data/2016/01-20/splunk-data/16/' # use your path
		#path =r'/home/jayasudha/spring/aml/Data/2016/'+eachDay+'/splunk-data/'+eachHour+'/' # use your path
		#allFiles = glob.glob(path + "/apt*.csv")
		allFiles = glob.glob(path + "/apt-na10-16.csv")
		frame = pd.DataFrame()
		frameEach = pd.DataFrame()
		frame = pd.concat([pd.read_csv(f, index_col=0, header=None) for f in allFiles], keys=allFiles)
		# print "row count : "
		# print len(frame.index)
		for file in allFiles:
				
			# print file
			frameEach = pd.read_csv(file)
			#Check for DB Bounces
			frameEach = frameEach[frameEach.DBCPUTime != 0]
			frameEach = frameEach[frameEach.TotalDBTime != 0]
			frameEach = frameEach[frameEach.DBCPUTime != float('NaN')]
			frameEach = frameEach[frameEach.TotalDBTime != float('NaN')]
			print "after check for nan and 0's"
			print len(frameEach.index)
			#remove any NAN values
			#frameEach = frameEach.dropna(axis = 0)
			#to find majority vote
			orgidlist = pd.unique(frameEach.org_id.ravel())
			for org in orgidlist:
				framenew = pd.DataFrame()
				framenew = frameEach.loc[frameEach.org_id==org]
				countRacNode = framenew.racNode.value_counts()
				# print "countRacNode before"
				# print countRacNode
				if(len(countRacNode.index)>1):
					maxHit = countRacNode.idxmax()
					# print "maxHit"
					# print maxHit
					if(maxHit!=0):
						frameEach['temp']=(frameEach.org_id==org) & (frameEach.racNode!=maxHit) # all readings for which racNode!=maxhit
						frameEach.ix[frameEach.temp==True,'racNode'] = maxHit    #replace with maxhit
						del frameEach['temp']
					else: #find second highest
						
						maxHit = countRacNode.head(2).index[1]
						# print "second highest"
						# print maxHit
						frameEach['temp']= (frameEach.org_id==org) & (frameEach.racNode!=maxHit) 	
						frameEach.ix[frameEach.temp==True,'racNode'] = maxHit 
				else:
					maxHit = countRacNode.get(0,None)
					if(maxHit == 0):
						# print "0 is the only hit node"	
						frameEach = frameEach[frameEach.org_id!=org]
			print "after check for majority vote"
			print len(frameEach.index)	


			#DBCPUTime Adjustment
			frameEach[['DBCPUTime', 'TotalDBTime']] = frameEach[['DBCPUTime', 'TotalDBTime']].astype(float)

			#if >20%, remove record
			frameEach['temp']= (frameEach.DBCPUTime>frameEach.TotalDBTime)&((frameEach.DBCPUTime-frameEach.TotalDBTime)< 0.2*frameEach.TotalDBTime)
			frameEach['temp2']= (frameEach.DBCPUTime>frameEach.TotalDBTime)&((frameEach.DBCPUTime-frameEach.TotalDBTime)>= 0.2*frameEach.TotalDBTime)
			frameEach = frameEach[frameEach.temp2 == False] #remove records for which diff >20%
			frameEach.ix[frameEach.temp == True,'DBCPUTime'] = (frameEach.ix[frameEach.temp == True,'DBCPUTime'] +frameEach.ix[frameEach.temp == True,'TotalDBTime'])/2
			del frameEach['temp']	
			del frameEach['temp2']

			print "after check for DBCPUTime>TotalDBTime"
			print len(frameEach.index)

				#groupby
			frameEach['dup'] = frameEach.duplicated(['racNode','logRecordType','_time','org_id'],False)
			tempframe = frameEach
			tempframe = tempframe[tempframe.dup==True]
			frameEach = frameEach[frameEach.dup==False] #non duplicated data
			for name,group in tempframe.groupby(['racNode','logRecordType','_time','org_id']):
				group['TotalDBTime'] = group['TotalDBTime'].sum()
				group['DBCPUTime'] = group['DBCPUTime'].sum()
				group['product'] = group['apt']*group['requests']
				group['apt'] = group['product'].sum()
				del group['product']
				temp = group.iloc[0]
				#print temp
				frameEach = frameEach.append(temp)
				del frameEach['dup']

			print "after check for groupby"
			print len(frameEach.index)
	
#print frameEach
frameEach.to_csv('output.csv', sep=',')

##############################
# frame = pd.read_csv("/home/jayasudha/spring/aml/Data/code/sample.csv")
# frame['dup'] = frame.duplicated(['A','B'],False)
# tempframe = frame
# tempframe = tempframe[tempframe.dup==True]
# frame = frame[frame.dup==False] #non duplicated data
# for name,group in tempframe.groupby(['A','B']):
# 	print group
# 	group['C'] = group['C'].sum()
# 	print "group after"
# 	print group
# 	print "temp"
# 	temp = group.iloc[0]
# 	print temp
# 	frame = frame.append(temp)
# 	print frame
# 272950 rows x 8 columns
# 230447 rows x 9 columns









