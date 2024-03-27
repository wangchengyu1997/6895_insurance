import pandas as pd
import matplotlib.pyplot as plt

scores = [0.5365335107572364, 0.5187918992600271, 0.5331245359206828, 0.5424816985626971, 0.5094073672907622, 0.5124314776028791, 0.5091150221749088]
index = [4, 5, 6, 7, 8, 9, 10]

s = pd.Series(scores, index=index)
_ = s.plot()

plt.title('Topic Coherence: Determine Optimal Number of Topics')

# Set the label for the x-axis
plt.xlabel('Number of Topics')

# Set the label for the y-axis
plt.ylabel('Coherence Score')
plt.show()
