#Question 4.1
import math

class FMAlgorithm:
    def __init__(self, m):
        self.m = m
        self.max_trailing_zeros = -1
        
    def hash_function_linear(self, x, a, b):
        return ((a * x + b) % self.m)
    
    def hash_function_quadratic(self, x, a, b, c):
        return ((a * x * x + b * x + c) % self.m)
    
    def trailing_zeros(self, x):
        if x == 0:
            return 1
        return int(math.log2(x & -x)) + 1
    
    def process_stream(self, data_stream, hash_type='linear', hash_params=None):
        if hash_params is None:
            hash_params = {}
        if hash_type == 'linear':
            hash_func = self.hash_function_linear
        elif hash_type == 'quadratic':
            hash_func = self.hash_function_quadratic
        else:
            raise ValueError("Invalid hash type. Choose 'linear' or 'quadratic'.")
        
        for user_id in data_stream:
            if hash_type == 'linear':
                hashed_value = hash_func(user_id, hash_params.get('a', 1), hash_params.get('b', 0))
            elif hash_type == 'quadratic':
                hashed_value = hash_func(user_id, hash_params.get('a', 1), hash_params.get('b', 0), hash_params.get('c', 0))
            
            trailing_zeros = self.trailing_zeros(hashed_value)
            self.max_trailing_zeros = max(self.max_trailing_zeros, trailing_zeros)
    
    def estimate_cardinality(self):
        return 2 ** self.max_trailing_zeros

# Example usage:
data_stream = [3, 1, 4, 1, 5, 9, 2, 6, 5]
m = 32  # Example value for bit array size
fm = FMAlgorithm(m)

# Process the stream using linear hash function
fm.process_stream(data_stream, hash_type='linear', hash_params={'a': 3, 'b': 7})

# Estimate cardinality
cardinality_estimate = fm.estimate_cardinality()
print("Estimated cardinality using linear hash function:", cardinality_estimate)

# Process the stream using quadratic hash function
fm.process_stream(data_stream, hash_type='quadratic', hash_params={'a': 2, 'b': 3, 'c': 5})

# Estimate cardinality
cardinality_estimate = fm.estimate_cardinality()
print("Estimated cardinality using quadratic hash function:", cardinality_estimate)


#Question 4.2

import math

# Constants for hash functions
m = 64  # Bit array size
data_stream = [3, 1, 4, 1, 5, 9, 2, 6, 5]

# Define different hash functions
def hash_function_1(x):
    return (5 * x + 7) % m

def hash_function_2(x):
    return (3 * x) % m

def hash_function_3(x):
    return ((2 * x**2) + (3 * x) + 5) % m  # Quadratic expression

# Function to calculate trailing zero length (t)
def trailing_zero_length(x):
    if x == 0:
        return 1  # Special case: If hash result is 0, t is 1
    t = 0
    while x & 1 == 0:
        t += 1
        x >>= 1
    return t

# Function to estimate cardinality using FM algorithm
def cardinality_estimate(hash_function):
    max_t = -1
    for user_id in data_stream:
        hash_result = hash_function(user_id)
        t = trailing_zero_length(hash_result)
        max_t = max(max_t, t)
    return 2**max_t

# Perform experiments with different hash functions
hash_functions = [hash_function_1, hash_function_2, hash_function_3]

for i, hash_func in enumerate(hash_functions):
    estimate = cardinality_estimate(hash_func)
    print(f"Cardinality Estimate with Hash Function {i+1}: {estimate}")
