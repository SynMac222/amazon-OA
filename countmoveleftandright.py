


nums=[0,1,0,1]



def countMoveleft(nums,target):
    re = 0
    last = 0
    for i in range(len(nums)):
        
        if nums[i] == target: 
            re +=i-last
            last+=1
    return re



print (min(countMoveleft(nums,0),countMoveleft(nums,1)))