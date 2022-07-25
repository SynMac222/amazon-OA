



def musicPair(nums,target):
    d = dict()
    count =0
    for i in range(len(nums)):
        nums[i] = nums[i]%target
    
    for j in range(len(nums)):
        if nums[j] in d:
            d[nums[j]].append(j)
        else:
            d[nums[j]]=[j]
    print(d)
    
    for k in range(len(nums)):
        complement = 0 if target -nums[k] == target else target -nums[k]
 
        if complement in d: 
            if k in d[complement]:
                count += len(d[complement]) -1
            else:
                count += len(d[complement])
        print(complement,count)
    

    return count/2
print (musicPair([100,180,40,120,10],220))





