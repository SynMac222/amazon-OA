



import string
def poplularNfeatures(numFeatures,topFeatures,possibleFeatures,numFeatureRequests,featureRequests):
    re_collection=[]
    freq_map=dict()
    re=[]
    possibleFeatures = set(possibleFeatures)
    for request in featureRequests:
        a = str.maketrans("","",string.punctuation)
        request = str.translate(request,a)
        re_collection += request.split(' ')
        
    print(re_collection)
    
    for word in re_collection:
        if word.lower() in possibleFeatures:
            if word.lower() in freq_map:
                freq_map[word.lower()] +=1
            else:
                freq_map[word.lower()] = 1
    print(freq_map)
    b = sorted(freq_map.items(),key=lambda x:x[1],reverse=True)

    re = [ b[i][0] for i in range(topFeatures)]
    print(re)
    return re

poplularNfeatures(6,2, possibleFeatures = ["storage", "battery", "hover", "a exa", "waterproof", "solar"],numFeatureRequests = 7,featureRequests = ["I wish my Kindle had even more storage!", "I wish the battery life on my Kindle lasted 2 years.", "I read in the bath and would enjoy a waterproof Kindle", "Waterproof and increased battery are my top two requests.", "I want to take my Kindle into the shower. Waterproof please waterproof !", "It would be neat if my Kindle would hover on my desk when not in use.", "How cool would it be if my Kindle charged in the sun via solar power?"] )
poplularNfeatures(10,4, possibleFeatures = ["the", "clay", "is", "sunny", "the", "the", "the", "sunny", "is", "is"],numFeatureRequests = 10,featureRequests =["the", "clay", "is", "sunny", "the", "the", "the", "sunny", "is", "is"] )

