import sys
import re
import requests
import numpy as np


class Model:
    name = ""
    quantity = 0
    percentage = 0

    def __init__(self, name, quantity, percentage):
        self.name = name
        self.quantity = quantity
        self.percentage = percentage

       

def listOfEqualItemsOfList(list):
    listOut = []
    pastItemName = 'jhdaijdajnd_#S9090j'
    for item in list:
        if item != pastItemName:
            listOut.append([])
        listOut[len(listOut)-1].append(item)
        pastItemName = item
    return listOut

#The ranking function consist of two parts:
#   * The grouping section that condense a list of items in one element with the important info to show
#   * The sorting of the grouped elements 
#The grouping depends on how the N total elements in the scipt distribute in elements of same Ip o same ClientHost
#if the elements distribute equaly in the matrixToRank it will have a O(N) time complexity. 
#In the sorting section, the complexity by definition is O(NlnN). 
#If we add this two functionalities we find that the most significant item 
# is the sorting because grows faster than the grouping section

def rankingFunction(matrixToRank):
    rankedList =  []
    for i in range(len(matrixToRank)):
        lenghtList = len(matrixToRank[i])
        for j in range(lenghtList):
            if j == lenghtList - 1:
                rankedList.append(
                    Model
                    (
                        name = matrixToRank[i][0], 
                        quantity = j, 
                        percentage = round(j*100/len(data_dict),2)
                    )
                )
        rankedList.sort(key=lambda x: x.quantity, reverse = True)
    return rankedList


    
        

if __name__ == "__main__":
    name  =  sys.argv[1]        

    with open(name) as f:
        data = str(f.readlines())
    
        pattern = r'(\d+-\w+-\d+)\s(\d+:\d+:\d+\.\d+)\squeries:\sinfo:\sclient\s@(.*?)\s(\d+\.\d+\.\d+\.\d+)#(\d+)\s\((.*?)\):\squery:\s(.*?)\sIN\s(.*?)\s\+(.*?)\s\((.*?)\)'

        data_dict = np.array([])
        
        matches = re.findall(pattern, data)
        id = 0
        for match in matches:
            data_dict = np.append({ 
                "id": id, 
                "timestamp": f'{match[0]}T{match[1]}Z', 
                "client_ip": match[3], 
                "op_code": "QUERY", 
                "response_code": "NOERROR", 
                "question": { 
                    "type": match[7], 
                    "name": match[6], 
                    "class": "IN" 
                    }, 
                "flags": { 
                    "authoritative": False, 
                    "recursion_available": True, 
                    "truncated_response": False, 
                    "checking_disabled": False, 
                    "recursion_desired": True, 
                    "authentic_data": False 
                    }   
                },data_dict
                )
            id = id + 1

        clientHosts = []
        ips = []

        for entry in data_dict:
            clientHosts.append(entry["question"]["name"])
            ips.append(entry['client_ip'])

        clientHosts.sort()
        ips.sort()

        sortedClientHostsMatrix = listOfEqualItemsOfList(clientHosts)
        sortedIpMatrix = listOfEqualItemsOfList(ips)

        rankingClientHost = rankingFunction(sortedClientHostsMatrix)

        rankingIp = rankingFunction(sortedIpMatrix)

        print('Total records', len(data_dict))
        print('Clients IP Rank')
        print('--------------------------------')            
       
    
        for i in range(6):
            print(f'{rankingClientHost[i].name} {rankingClientHost[i].quantity} {rankingClientHost[i].percentage}%')
        print('--------------------------------')
        print('')
        print('Host Rank')
        print('--------------------------------')
        for i in range(6):
            print(f'{rankingIp[i].name} {rankingIp[i].quantity} {rankingIp[i].percentage}%')
        print('--------------------------------')



        data_dict_batches = []
        for i in range(id):
            if i % 500 == 0:
                data_dict_batches.append([])
            data_dict_batches[int(float(i)/500)].append(data_dict[i]) 

        for batch in data_dict_batches:
            url = 'https://api.lumu.io/collectors/5ab55d08-ae72-4017-a41c-d9d735360288/dns/packets?key=d39a0f19-7278-4a64-a255-b7646d1ace80'
            myobj = batch
            print(requests.post(url, json = myobj))



            #

    
    
    


    






    