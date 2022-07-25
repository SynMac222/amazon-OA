

def WinPrize(codeList,shoppingCart):
    if codeList == []:
        return 1
    if shoppingCart == []:
        return 0
   
    idx_codeList = 0
    idx_shoppingCart = 0
    while(idx_codeList< len(codeList) and idx_shoppingCart+len(codeList[idx_codeList])<= len(shoppingCart)):
        match = True
        for k in range(len(codeList[idx_codeList])):
            print(codeList[idx_codeList][k],shoppingCart[idx_shoppingCart+k])
            if (not codeList[idx_codeList][k]=='anything' and not(codeList[idx_codeList][k]==shoppingCart[idx_shoppingCart+k])):
               match = False
               break
        print(match,idx_codeList,idx_shoppingCart)
        if(match):
            idx_shoppingCart += len(codeList[idx_codeList])
            idx_codeList+=1 
        else:
            idx_shoppingCart +=1
    return 1 if idx_codeList == len(codeList)  else 0

codeList = [['apple', 'apple'], ['banana', 'anything', 'banana']] 
shoppingCart = ['apple', 'apple', 'apple', 'banana', 'orange', 'banana']

print(WinPrize(codeList,shoppingCart))




