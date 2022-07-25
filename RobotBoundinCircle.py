class Solution:
    def isRobotBounded(self, instructions: str) -> bool:
        #  deltax，delta y：0 is north, 1 is right,2 south
        direction = [[0,1],[1,0],[0,-1],[-1,0]]
        dx = 0
        dy = 0 
        idx=0
        for s in instructions:
            if s == 'L':
                idx = (idx+3)%4
            elif s == 'R':
                idx = (idx+1)%4
            else:
                dx += direction[idx][0]
                dy += direction[idx][1]
            
        return (dx==0 and dy==0) or idx!=0
                