
import sys
def strength_password(self,password= "thisisbeautiful"):
    vowels={'a','e','i','o','u'}

    vowel =0
    consonant =0
    re=0

    for i in password:
        if i in vowels:
            vowel+=1
        else:
            consonant+=1
        if vowel>=1 and consonant>=1:
            vowel = 0
            consonant = 0
            re +=1
    
    return re

if __name__ =='__main__':
    print (strength_password(sys.argv))

   