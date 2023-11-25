import timeit

python = timeit.timeit('program2.fib(100000)',
                       setup='import program2', number=100)
cython = timeit.timeit('program1.fib(100000)',
                       setup='import program1', number=100)

print("Python Time: ", python)
print("Cython Time: ", cython)
print(f"{python / cython}x times faster")
