import json
import time
c=['http_status_code','count']
final={}
l=[(200, 128091567), (403, 27241634), (302, 19204143), (0, 4614661), (304, 2928374), (404, 560016), (301, 118873), (503, 64513), (500, 24569), (499, 8723), (411, 1996), (408, 1013), (400, 259), (502, 5), (206, 2)]

dic={}
for i in range(len(l)): 
	for j in range(len(l[i])):
		# print("c is ,",c[j])
		dic[c[j]]=l[i][j]
	dic['date']=time.time()
	dic['qtype']=1
	print(dic)


	