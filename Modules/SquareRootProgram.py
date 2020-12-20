import math
import numpy as np

scores = [88,92,79,93,85]
print(np.mean(scores))

square_root = [math.sqrt(scores) for score in scores]
print(square_root)