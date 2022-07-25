
class Solution:
    def isRobotBounded(self, instructions: str) -> bool:

        offset_dict_L = {}
        offset_dict_L[(0,1)] = (-1,0)
        offset_dict_L[(0,-1)] = (1,0)
        offset_dict_L[(1,0)] = (0,1)
        offset_dict_L[(-1,0)] = (0,-1)

        offset_dict_R = {}
        offset_dict_R[(0,1)] = (1,0)
        offset_dict_R[(0,-1)] = (-1,0)
        offset_dict_R[(1,0)] = (0,-1)
        offset_dict_R[(-1,0)] = (0,1)

        cx = 0
        cy = 0
        