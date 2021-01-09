'''
    The prime factors of 13195 are 5, 7, 13 and 29.

    What is the largest prime factor of the number 600,851,475,143?
'''
import math

# In this I take advantage of a mathematical concept that factors can be determined only using O(sqrt(n)) complexity.
### For example, the factors of 10 are (1, 10) + (2,50) + (4, 25) + (5, 20) + (10, 10).
### To get these pairs, I only need iterate until the square root (10), thus greatly reducing the number of iterations.

def get_largest_prime_factor(number):
    divisors = range(1, int(math.sqrt(number)) + 1)
    factor_list = list()
    for divisor in divisors:
        if number % divisor == 0:
            factor = int(number/divisor)
            factor_list.append(factor)
            factor_list.append(divisor)
    factor_list.sort(reverse=True)
    # Iterate from the back, if I find a match, then can exit as the prime has been found.
    for factor in factor_list:
        if is_prime(factor):
            return factor
    raise ValueError("No prime factors exist!")


def is_prime(number):
    if number < 2:
        raise ValueError("Primes start at 2, please provide a value >= 2")
    for x in range(2, int(math.sqrt(number)) + 1):
        if number % x == 0:
            return False
    return True

if __name__ == '__main__':
    print(get_largest_prime_factor(600851475143))

