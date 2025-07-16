import matplotlib.pyplot as plt

# Data for the graph
x_values = [1, 2, 3, 4, 5]
y_values = [2, 4, 1, 5, 3]

# Create the plot
plt.plot(x_values, y_values, marker='o', linestyle='-', color='blue')

# Add labels and title
plt.xlabel("X-axis Label")
plt.ylabel("Y-axis Label")
plt.title("Simple Line Graph")

# Display the graph
plt.grid(True) # Add a grid for better readability
plt.show()