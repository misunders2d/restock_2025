from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random


def func1(a, b, operation="add"):
    if operation != "add":
        return "Wrong operation selected"
    return a + b


def func2(input_str):
    print(input_str)


def func3():
    time.sleep(1)
    return random.randint(0, 100)


# slow version
with ThreadPoolExecutor() as executor:
    future1 = executor.submit(func1, 5, 8, operation="divide")
    future2 = executor.submit(func2, "Hello, World")
    future3 = executor.submit(func3)


result1 = future1.result()
result2 = future2.result()
result3 = future3.result()


# correct fast version
with ThreadPoolExecutor() as executor2:
    futures = {}
    futures[executor2.submit(func1, 4, 3, operation="divide")] = "func1"
    futures[executor2.submit(func2, "Hello, World2")] = "func2"
    futures[executor2.submit(func3)] = "fucn3"
    results = {}
    for future in as_completed(futures):
        name = futures[future]
        results[name] = future.result()
