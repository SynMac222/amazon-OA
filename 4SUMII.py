def shoppingOptions(pairOfJeans, pairOfShoes, pairOfSkirts, pairOfTops, dollars):
    sum_dict = dict()
    count = 0
    for i in range(len(pairOfJeans)):
        for j in range(len(pairOfShoes)):
            sum_val = pairOfJeans[i] + pairOfShoes[j]
            if sum_val in sum_dict:
                sum_dict[sum_val] +=1
            else:
                sum_dict[sum_val] =1
    
    for c in range(len(pairOfSkirts)):
        for d in range(len(pairOfTops)):
            cur_val = dollars - pairOfSkirts[c] - pairOfTops[d]
            possible_sum = [key for key in sum_dict if key <= cur_val]
            value = [sum_dict.get(key) for key in possible_sum]
            count += sum(value)
    
    return count


print(shoppingOptions([2], [3, 4], [2, 5], [4, 6], 12)) # Ans 2
print(shoppingOptions([2], [2, 2], [2], [2], 9)) # Ans 2
print(shoppingOptions([4, 7], [6, 6], [1, 3, 5], [5, 7, 12], 20)) # Ans 12