'''
    If we list all the natural numbers below 10 that are multiples of 3 or 5, we get 3, 5, 6 and 9. The sum of these multiples is 23.
    Find the sum of all the multiples of 3 or 5 below 1000.
'''

# List comprehension method
def solve_with_lc():
    numbers = range(1000)
    answer = sum([x for x in numbers if x % 3 == 0 or x % 5 == 0])
    print(answer)

# Simpler method
def solve_simply():
    numbers = range(1000)
    answer = 0
    for number in numbers:
        if number % 3 == 0 or number % 5 == 0:
            answer = answer + number
    print(answer)

if __name__ == '__main__':
    solve_with_lc()
    solve_simply()