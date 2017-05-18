# Change the path of the directory of test data!!
#Channel ID is stored as 'name' of the node.
#Conventions:
#If two nodes have same channel ID then they are connected with 'SAME_CHANNEL' relation
#If two nodes have common tag then they are connected with 'SIMILAR_TAG' relation with edge weightage of number of common tags if greater than 20
#If two nodes have common description then they are connected with 'SIMILAR_DESC' relation with edge weightage of number of common words if greater than 3000


                                                                        ## Libaries required - json, os, py2neo
from py2neo import Graph, Node, Relationship
import json
import os


                                                                        ## Change password - This is the password for my Neo4j Database 
graph = Graph(password="*****")


                                                                         #Description relation weightage caluculation
def descriptionCompare(description1,description2):
	word_description1 = description1.split()
	word_description2 = description2.split()
	count = len(set(word_description2)&set(word_description1))
	return count


                                                                         #Tag relation weightage caluculation
def tagsCompare(tags1,tags2):
	return len(set(tags1)&set(tags2))

print("Starting to read data from json file!!!")

arrayjson = []                                                           ## New array for storing json data
####
filelist = os.listdir("/home/btech2014/Desktop/neolab/test/")            ## Change Directory Here
####
for i in range(len(filelist)):
	####	
	filelist[i]="/home/btech2014/Desktop/neolab/test/"+filelist[i]   ## Change Directory Here
	####	
	print(filelist[i])
	page = open(filelist[i],"r")
	parsed = json.loads(page.read())
	arrayjson.append(parsed)


                                                                          ## For-loop for creating Nodes for each json file with Video_ID as name
for i in range(len(arrayjson)):
	arraystring = arrayjson[i]['videoInfo']['statistics']
	                                                                  # Node Creation
	a = Node("Youtube",name=arrayjson[i]['videoInfo']['id'],commentCount=arraystring['commentCount'],viewCount=arraystring['viewCount'],favoriteCount=arraystring['favoriteCount'],dislikeCount=arraystring['dislikeCount'],likeCount=int(arraystring['likeCount']))
	graph.create(a)
	print("Current run number(While Node Creation): "+str(i))
	# print(arrayjson[i]['videoInfo']['id'])                          ## uncomment to print the data


                                                                          ## For-loop for creating relation ships between the created nodes
for i in range(len(arrayjson)):
	element = arrayjson[i]
	for j in range(i-1,-1,-1):
		
		                                                          # For establishing 'SAME_CHANNEL' relation 
		if arrayjson[j]['videoInfo']['snippet']['channelId'] == element['videoInfo']['snippet']['channelId']:
			a = graph.find_one("Youtube",property_key='name', property_value=element['videoInfo']['id'])
			b = graph.find_one("Youtube",property_key='name', property_value=arrayjson[j]['videoInfo']['id'])
			channelRelation = Relationship(a,"SAME_CHANNEL",b)
			graph.create(channelRelation)
		
		
		                                                           # For establishing 'SIMILAR_DESC' relation		
		Count=descriptionCompare(arrayjson[i]['videoInfo']['snippet']['description'],arrayjson[j]['videoInfo']['snippet']['description'])
		if Count > 3000:
			a = graph.find_one("Youtube",property_key='name', property_value=element['videoInfo']['id'])
			b = graph.find_one("Youtube",property_key='name', property_value=arrayjson[j]['videoInfo']['id'])
			DescriptionRelation = Relationship(a,"SIMILAR_DESC",b,weightage=Count)
			graph.create(DescriptionRelation)
		

		                                                           # For establishing 'SIMILAR_TAG' relation
		if 'tags' in arrayjson[i]['videoInfo']['snippet'] and 'tags' in arrayjson[j]['videoInfo']['snippet']:
			tagCount = tagsCompare(arrayjson[i]['videoInfo']['snippet']['tags'], arrayjson[j]['videoInfo']['snippet']['tags'])
			if tagCount > 20:
				a = graph.find_one("Youtube",property_key='name', property_value=element['videoInfo']['id'])
				b = graph.find_one("Youtube",property_key='name', property_value=arrayjson[j]['videoInfo']['id'])
				TagRelation = Relationship(a,"SIMILAR_TAG",b,weightage=tagCount)
				graph.create(TagRelation)
 
	print("Current run number(While inserting relationships): "+str(i))

print("Data from json files successfully imported to Neo4j Database")
                                                                            ## END
