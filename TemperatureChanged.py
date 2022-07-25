


'''
Find the maximum aggregate temperature changed evaluated among all the days.
ex - [-1,2,3] - 5

explanation -
[-1],[-1,2,3] - max(-1,4) = 4
[-1,2],[2,3] - max(1,5) = 5
[-1,2,3][3] - max(4,3) = 4
'''

# import math 


arr = [-1,-1,-1]

presum =0
sufsum=0
re = -float('inf')

for i in range(len(arr)):
    sufsum+=arr[i]


for i in range(len(arr)):
    re= max(max(sufsum,presum+arr[i]),re)
    sufsum -= arr[i]
    presum += arr[i]

print(re)


'''
Find the power of each possible contiguous group of servers.
'''

arr = [2,1,3]
re = 0
temp_sum=0
for i in range(len(arr)):
    min_value = arr[i]
    re += min_value * arr[i]
    temp_sum = arr[i]
    print(re)
    for j in range(i+1,len(arr)):
        min_value= min(min_value,arr[j])
        temp_sum+=arr[j]
        re += min_value*temp_sum
        print(re)
print(re)

        
